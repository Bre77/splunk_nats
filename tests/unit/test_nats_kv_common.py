import asyncio
import datetime
import json
from types import SimpleNamespace

from nats.js.errors import BucketNotFoundError, ServiceUnavailableError
from nats.js.kv import KeyValue

from nats_kv_common import (
    entry_from_msg,
    entry_operation,
    entry_raw,
    entry_time,
    kv_error_message,
    read_history,
)

CREATED = datetime.datetime(
    2026, 10, 11, 7, 11, 10, 551637, tzinfo=datetime.timezone.utc
)


def make_entry(value=b"", operation=None, created=CREATED):
    return KeyValue.Entry(
        bucket="cfg",
        key="vehicle.config",
        value=value,
        revision=7,
        delta=0,
        created=created,
        operation=operation,
    )


def test_put_value_is_the_raw_text():
    assert entry_raw(make_entry(b'{"a": 1}')) == '{"a": 1}'
    assert entry_operation(make_entry(b"x")) == "PUT"


def test_binary_put_value_is_base64():
    assert entry_raw(make_entry(b"\xff\x00")) == "/wA="


def test_delete_and_purge_are_json_records():
    for operation in ("DEL", "PURGE"):
        entry = make_entry(operation=operation)
        assert entry_operation(entry) == operation
        assert json.loads(entry_raw(entry)) == {
            "key": "vehicle.config",
            "operation": operation,
            "revision": 7,
        }


def test_empty_put_is_a_json_record_so_splunk_does_not_drop_it():
    assert json.loads(entry_raw(make_entry(b"")))["operation"] == "PUT"


def test_entry_time_is_epoch_seconds_of_created():
    # Regression: the input passed the datetime itself, which splunklib wrote as
    # "2026-10-11 07:11:10.551637+00:00" and Splunk ignored.
    assert entry_time(make_entry(b"x")) == CREATED.timestamp()
    assert isinstance(entry_time(make_entry(b"x")), float)


def test_entry_time_falls_back_to_now(monkeypatch):
    monkeypatch.setattr("nats_kv_common.time.time", lambda: 1234.5)
    assert entry_time(make_entry(b"x", created=None)) == 1234.5


def test_service_unavailable_without_domain_suggests_domain():
    message = kv_error_message(
        ServiceUnavailableError(), "cfg", None, "domain=<domain>"
    )
    assert "set domain=<domain>" in message
    assert "code=None" not in message


def test_service_unavailable_with_domain_names_the_domain():
    message = kv_error_message(
        ServiceUnavailableError(), "cfg", "WRONG", "domain=<domain>"
    )
    assert "domain 'WRONG'" in message


def test_bucket_not_found_names_bucket_and_suggests_domain():
    message = kv_error_message(BucketNotFoundError(), "cfg", None, "the Domain field")
    assert "'cfg' was not found" in message
    assert "set the Domain field" in message
    message = kv_error_message(
        BucketNotFoundError(), "cfg", "GLOBAL", "the Domain field"
    )
    assert "domain 'GLOBAL'" in message


def test_other_errors_pass_through():
    assert kv_error_message(ValueError("boom"), "cfg", None, "x") == "boom"


class FakeMsg:
    def __init__(self, key, revision, pending, headers=None):
        self.subject = f"$KV.cfg.{key}"
        self.data = key.encode()
        self.headers = headers
        self.metadata = SimpleNamespace(
            sequence=SimpleNamespace(stream=revision),
            num_pending=pending,
            timestamp=CREATED,
        )


class LateDeliverySub:
    """
    The server has already sent every message (delivered=3, num_pending=0)
    but none has reached the client yet when consumer_info answers.
    """

    def __init__(self):
        self.consumer_info_answered = False
        self.unsubscribed = False

    async def consumer_info(self):
        self.consumer_info_answered = True
        return SimpleNamespace(delivered=SimpleNamespace(consumer_seq=3), num_pending=0)

    async def next_msg(self, timeout):
        assert self.consumer_info_answered
        if not hasattr(self, "msgs"):
            self.msgs = [
                FakeMsg("a", 1, 2),
                FakeMsg("b", 2, 1),
                FakeMsg("a", 3, 0, {"KV-Operation": "DEL"}),
            ]
        return self.msgs.pop(0)

    async def unsubscribe(self):
        self.unsubscribed = True


def test_read_history_waits_for_entries_already_sent_by_the_server():
    # Regression: natskv used kv.history(), whose watcher ends the history as
    # soon as the consumer has nothing pending, so about 1 in 5 runs against a
    # remote bucket returned no rows at all.
    sub = LateDeliverySub()

    class FakeJetStream:
        async def subscribe(self, subject, **kwargs):
            assert subject == "$KV.cfg.>"
            assert kwargs["stream"] == "KV_cfg"
            return sub

    entries = asyncio.run(read_history(FakeJetStream(), "cfg", ">"))
    assert [(e.key, e.revision, entry_operation(e)) for e in entries] == [
        ("a", 1, "PUT"),
        ("b", 2, "PUT"),
        ("a", 3, "DEL"),
    ]
    assert sub.unsubscribed


def test_entry_from_msg_maps_server_marker_reasons():
    for reason, operation in (
        ("MaxAge", "PURGE"),
        ("Purge", "PURGE"),
        ("Remove", "DEL"),
    ):
        msg = FakeMsg("a", 1, 0, {"Nats-Marker-Reason": reason})
        assert entry_from_msg("cfg", msg).operation == operation
    assert (
        entry_from_msg("cfg", FakeMsg("a", 1, 0, {"Nats-Marker-Reason": "?"})) is None
    )
