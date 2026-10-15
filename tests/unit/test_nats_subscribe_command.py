import asyncio
import base64
import sys
import textwrap
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest
from nats.aio.msg import Msg
from nats.errors import NoServersError

import nats_subscribe_command as cmd
import natsutil


class ErrorExit(Exception):
    pass


class FakeCommand:
    """Stands in for the UCC-generated splunklib GeneratingCommand."""

    def __init__(self, **options):
        self.errors = []
        for name, value in options.items():
            setattr(self, name, value)

    def error_exit(self, error, message=None):
        self.errors.append(message)
        raise ErrorExit(message)


def make_msg(subject="telemetry.VIN1.data", data=b"{}", headers=None):
    return Msg(_client=None, subject=subject, data=data, headers=headers)


class FakeSubscription:
    async def unsubscribe(self):
        pass


class FakeNats:
    """Minimal nats-py client that publishes `messages` once subscribed."""

    def __init__(self, messages, interval=0.0):
        self.messages = messages
        self.interval = interval
        self.connected_url = urlparse("nats://nats.example:4222")
        self.closed = False

    async def subscribe(self, subject, cb):
        async def publish():
            for msg in self.messages:
                await asyncio.sleep(self.interval)
                await cb(msg)

        self.publisher = asyncio.ensure_future(publish())
        return FakeSubscription()

    async def close(self):
        self.publisher.cancel()
        self.closed = True


def patch_connect(monkeypatch, client):
    natsutil.patch_connect(monkeypatch, cmd, client)


def collect(subject="telemetry.>", count=100, duration=10, decoder=None):
    return asyncio.run(
        cmd._collect_messages(
            subject, {"servers": "nats://nats.example:4222"}, count, duration, None, decoder
        )
    )


def test_event_matches_input_sourcetype_and_host_form():
    event = cmd.message_to_event(make_msg(data=b'{"a": 1}'), "nats.example:4222")

    assert event["sourcetype"] == "nats:message"
    assert event["host"] == "nats.example:4222"
    assert event["source"] == "telemetry.VIN1.data"
    assert event["subject"] == "telemetry.VIN1.data"
    assert event["_raw"] == '{"a": 1}'


def test_headers_become_fields():
    event = cmd.message_to_event(
        make_msg(headers={"traceparent": "00-abc-def-01", "Nats-Msg-Id": "7"}),
        "h:4222",
    )

    assert event["headers.traceparent"] == "00-abc-def-01"
    assert event["headers.Nats-Msg-Id"] == "7"


def test_binary_payload_is_base64():
    payload = b"\x08\x96\x01\xff"
    event = cmd.message_to_event(make_msg(data=payload), "h:4222")

    assert event["_raw"] == base64.b64encode(payload).decode("ascii")


def test_host_is_connected_server_netloc():
    nc = SimpleNamespace(connected_url=urlparse("nats://nats.example:4222"))

    assert cmd.connected_host(nc, ["nats://other:4222"]) == "nats.example:4222"


def test_host_falls_back_to_first_server_netloc():
    nc = SimpleNamespace(connected_url=None)

    assert cmd.connected_host(nc, ["nats://other:4222"]) == "other:4222"


def test_count_stops_collection_early(monkeypatch):
    client = FakeNats([make_msg(data=str(i).encode()) for i in range(10)])
    patch_connect(monkeypatch, client)

    events = collect(count=3, duration=30)

    assert [e["_raw"] for e in events] == ["0", "1", "2"]
    assert all(e["host"] == "nats.example:4222" for e in events)
    assert client.closed


def test_duration_bounds_collection(monkeypatch):
    # One message every 0.4 s; a 1 s window sees two or three of them.
    client = FakeNats([make_msg() for _ in range(20)], interval=0.4)
    patch_connect(monkeypatch, client)

    events = collect(count=0, duration=1)

    assert 1 <= len(events) <= 3


def test_no_messages_returns_after_duration(monkeypatch):
    patch_connect(monkeypatch, FakeNats([]))

    assert collect(count=100, duration=1) == []


@pytest.mark.parametrize(
    "options, expected",
    [
        ({}, (cmd.DEFAULT_COUNT, cmd.DEFAULT_DURATION)),
        ({"count": "0", "duration": "5"}, (0, 5)),
        ({"count": 25, "duration": 60}, (25, 60)),
    ],
)
def test_count_and_duration_options(options, expected):
    command = FakeCommand(**options)

    assert (
        cmd._int_option(command, "count", cmd.DEFAULT_COUNT, minimum=0),
        cmd._int_option(command, "duration", cmd.DEFAULT_DURATION, minimum=1),
    ) == expected


def test_invalid_duration_is_rejected():
    with pytest.raises(ValueError, match="duration must be at least 1"):
        cmd._int_option(FakeCommand(duration="0"), "duration", 10, minimum=1)


def test_generate_passes_options(monkeypatch):
    seen = {}

    async def fake_collect(subject, account_config, count, duration, logger, decoder):
        seen.update(subject=subject, count=count, duration=duration, decoder=decoder)
        return [{"_raw": "x"}]

    monkeypatch.setattr(cmd, "_get_account_config", lambda c, a: {"servers": "s"})
    monkeypatch.setattr(cmd, "_collect_messages", fake_collect)

    command = FakeCommand(subject="a.>", account="acct", count="5", duration="2")

    assert list(cmd.generate(command)) == [{"_raw": "x"}]
    assert seen == {"subject": "a.>", "count": 5, "duration": 2, "decoder": None}


def test_nats_error_reported_once_without_generic_location(monkeypatch):
    async def fail(*args):
        raise NoServersError()

    monkeypatch.setattr(cmd, "_get_account_config", lambda c, a: {"servers": "s"})
    monkeypatch.setattr(cmd, "_collect_messages", fail)
    command = FakeCommand(subject="a.>", account="acct")

    with pytest.raises(ErrorExit):
        list(cmd.generate(command))

    assert command.errors == ["nats: no servers available for connection"]


def test_missing_account_reported_once(monkeypatch):
    def missing(command_instance, account):
        raise ValueError(f"Failed to get account configuration '{account}'")

    monkeypatch.setattr(cmd, "_get_account_config", missing)
    command = FakeCommand(subject="a.>", account="nope")

    with pytest.raises(ErrorExit):
        list(cmd.generate(command))

    assert command.errors == ["Failed to get account configuration 'nope'"]


def test_unexpected_error_keeps_its_own_location(monkeypatch):
    def broken(command_instance, account):
        raise TypeError("bug")

    monkeypatch.setattr(cmd, "_get_account_config", broken)
    command = FakeCommand(subject="a.>", account="acct")

    # A bug propagates unwrapped, so splunklib reports the line that raised it.
    with pytest.raises(TypeError) as info:
        list(cmd.generate(command))

    assert info.traceback[-1].name == "broken"
    assert command.errors == []


UPPER_DECODER = """
    import base64, json, sys
    for line in sys.stdin:
        payload = base64.b64decode(json.loads(line)["payload"])
        print(json.dumps({"text": payload.decode().upper()}) if payload != b"bad" else "", flush=True)
"""


def test_decoder_decodes_collected_messages(monkeypatch, tmp_path):
    decoder_path = tmp_path / "upper"
    decoder_path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(UPPER_DECODER))
    decoder_path.chmod(0o755)
    client = FakeNats([make_msg(data=b"abc"), make_msg(data=b"bad")])
    patch_connect(monkeypatch, client)
    decoder = cmd.Decoder("upper", decoder_dir=str(tmp_path))

    decoded, failed = collect(count=2, duration=30, decoder=decoder)

    assert (decoded["_raw"], decoded["sourcetype"]) == ('{"text": "ABC"}', "nats:json")
    assert "decode_error" not in decoded
    assert (failed["_raw"], failed["sourcetype"]) == ("YmFk", "nats:message")
    assert failed["decode_error"] == "Decoder returned an empty line"
    assert decoder._process is None  # stopped once the search is done


@pytest.mark.parametrize(
    "options, error",
    [
        ({"decoder": "../upper"}, "Invalid decoder '../upper'"),
        ({"decoder": "missing"}, "Decoder not found"),
        ({"decoder": "upper", "decoder_args": "'unbalanced"}, "Invalid decoder arguments"),
    ],
)
def test_bad_decoder_reported_once_before_subscribing(monkeypatch, tmp_path, options, error):
    monkeypatch.setattr(cmd, "_get_account_config", lambda c, a: {"servers": "s"})
    monkeypatch.setattr(cmd, "_collect_messages", None)  # never reached
    import nats_decoder

    monkeypatch.setattr(nats_decoder, "DECODER_DIR", str(tmp_path))
    command = FakeCommand(subject="a.>", account="acct", **options)

    with pytest.raises(ErrorExit):
        list(cmd.generate(command))

    assert len(command.errors) == 1 and error in command.errors[0]
