import asyncio
import base64
import json
import sys
import textwrap
import time

import pytest

import nats_decoder
from nats_decoder import (
    BrokenDecoder,
    DecodeError,
    DecodeErrorLog,
    Decoder,
    DecoderConfigError,
    raw_payload,
    render,
)

PAYLOAD = b"\x0a\x0d\x08\x29\xff\x00"
PAYLOAD_B64 = base64.b64encode(PAYLOAD).decode()

# A decoder that follows the documented protocol, with payloads that misbehave
DECODER = """
    import base64, json, os, sys, time
    for line in sys.stdin:
        request = json.loads(line)
        payload = base64.b64decode(request["payload"])
        if payload == b"bad":
            print("bad frame", file=sys.stderr, flush=True)
            print(flush=True)
        elif payload == b"crash":
            print("crashing", file=sys.stderr, flush=True)
            sys.exit(1)
        elif payload == b"hang":
            time.sleep(30)
        elif payload == b"latin1":
            sys.stdout.buffer.write(b"\\xff\\xfe\\n")
            sys.stdout.flush()
        elif payload == b"env":
            print(json.dumps(sorted(k for k in os.environ if k in (
                "LD_LIBRARY_PATH", "PYTHONHTTPSVERIFY", "OPENSSL_CONF", "SPLUNK_HOME"))), flush=True)
        else:
            print(json.dumps({
                "pid": os.getpid(),
                "subject": request["subject"],
                "headers": request["headers"],
                "size": len(payload),
                "args": sys.argv[1:],
                "cwd": os.getcwd(),
            }), flush=True)
"""


def _write_decoder(directory, name="decoder", body=DECODER, mode=0o755):
    """Write an executable Python decoder script into the decoder directory."""
    path = directory / name
    path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body))
    path.chmod(mode)
    return path


def _decoder(directory, **kwargs):
    return Decoder("decoder", decoder_dir=str(directory), **kwargs)


def _render_all(decoder, *payloads, subject="telemetry.VIN.data", headers=None):
    async def run():
        try:
            return [await render(decoder, payload, subject, headers) for payload in payloads]
        finally:
            await decoder.close()

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# Without a decoder: unchanged behaviour
# ---------------------------------------------------------------------------


def test_raw_payload_keeps_utf8_and_base64_encodes_binary():
    assert raw_payload(b'{"a": 1}') == '{"a": 1}'
    assert raw_payload(PAYLOAD) == PAYLOAD_B64
    assert raw_payload(b"") == ""


def test_render_without_decoder_is_unchanged():
    assert asyncio.run(render(None, PAYLOAD, "a")) == (PAYLOAD_B64, False, None)
    assert asyncio.run(render(None, b"hello", "a")) == ("hello", False, None)


# ---------------------------------------------------------------------------
# Decoder settings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name", ["../evil", "/bin/sh", "a/b", ".hidden", "", "a b", "..", "x\\y"]
)
def test_decoder_must_be_a_file_name(tmp_path, name):
    with pytest.raises(DecoderConfigError, match="use the file name of an executable"):
        Decoder(name, decoder_dir=str(tmp_path))


def test_decoder_arguments_are_split_like_a_shell(tmp_path):
    assert Decoder("d", "--type 'a b' -v", str(tmp_path)).args == ["--type", "a b", "-v"]
    with pytest.raises(DecoderConfigError, match="Invalid decoder arguments"):
        Decoder("d", "--type 'unbalanced", str(tmp_path))


def test_decoder_lives_in_the_local_decoders_directory(monkeypatch, tmp_path):
    assert nats_decoder.DECODER_DIR.endswith("local/decoders")
    monkeypatch.setattr(nats_decoder, "DECODER_DIR", str(tmp_path))
    assert Decoder("tesla").path == str(tmp_path / "tesla")


def test_decoder_must_exist_and_be_executable(tmp_path):
    decoder = _decoder(tmp_path)
    with pytest.raises(DecodeError, match="Decoder not found: .*decoder"):
        decoder.check()
    assert "Decoder not found" in _render_all(decoder, PAYLOAD)[0].error

    _write_decoder(tmp_path, mode=0o644)
    with pytest.raises(DecodeError, match="Decoder is not executable"):
        decoder.check()


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


def test_decoder_process_decodes_message_after_message(tmp_path):
    _write_decoder(tmp_path)
    decoder = Decoder("decoder", "--type 'a b'", str(tmp_path))

    first, second = _render_all(decoder, PAYLOAD, b"xy", headers={"traceparent": "00-1"})

    assert first.decoded and first.error is None
    first_event, second_event = json.loads(first.raw), json.loads(second.raw)
    assert first_event == {
        "pid": first_event["pid"],
        "subject": "telemetry.VIN.data",
        "headers": {"traceparent": "00-1"},
        "size": len(PAYLOAD),
        "args": ["--type", "a b"],
        "cwd": str(tmp_path),
    }
    assert second_event["pid"] == first_event["pid"]
    assert second_event["size"] == 2
    assert json.loads(_render_all(_decoder(tmp_path), b"")[0].raw)["headers"] == {}


def test_decoder_runs_without_splunk_runtime_variables(tmp_path, monkeypatch):
    monkeypatch.setenv("LD_LIBRARY_PATH", "/opt/splunk/lib")
    monkeypatch.setenv("PYTHONHTTPSVERIFY", "0")
    monkeypatch.setenv("OPENSSL_CONF", "/opt/splunk/openssl.cnf")
    monkeypatch.setenv("SPLUNK_HOME", "/opt/splunk")
    _write_decoder(tmp_path)

    rendered = _render_all(_decoder(tmp_path), b"env")[0]

    assert json.loads(rendered.raw) == ["SPLUNK_HOME"]


def test_empty_line_fails_one_message_and_keeps_the_process(tmp_path):
    _write_decoder(tmp_path)

    before, failed, after = _render_all(_decoder(tmp_path), b"a", b"bad", b"b")

    assert failed.decoded is False
    assert failed.raw == base64.b64encode(b"bad").decode()
    assert failed.error.startswith("Decoder returned an empty line")
    assert json.loads(before.raw)["pid"] == json.loads(after.raw)["pid"]


def test_invalid_utf8_answer_keeps_base64_payload(tmp_path):
    _write_decoder(tmp_path)
    rendered = _render_all(_decoder(tmp_path), b"latin1")[0]
    assert rendered == (
        base64.b64encode(b"latin1").decode(),
        False,
        "Decoder wrote a line that is not valid UTF-8",
    )


def test_decoder_is_restarted_after_a_crash(tmp_path):
    _write_decoder(tmp_path)
    decoder = _decoder(tmp_path, restart_delay=0.5)

    async def scenario():
        try:
            before = await render(decoder, b"a", "s")
            crashed = await render(decoder, b"crash", "s")
            throttled = await render(decoder, b"b", "s")
            await asyncio.sleep(0.6)
            after = await render(decoder, b"c", "s")
            return before, crashed, throttled, after
        finally:
            await decoder.close()

    before, crashed, throttled, after = asyncio.run(scenario())

    assert crashed == (
        base64.b64encode(b"crash").decode(),
        False,
        "Decoder exited with status 1: crashing",
    )
    assert throttled.decoded is False
    assert "restarted at most every 0.5s: crashing" in throttled.error
    assert after.decoded
    assert json.loads(after.raw)["pid"] != json.loads(before.raw)["pid"]


def test_decoder_that_hangs_is_replaced(tmp_path):
    _write_decoder(tmp_path)
    decoder = _decoder(tmp_path, timeout=0.5, restart_delay=0)

    started = time.monotonic()
    hung, after = _render_all(decoder, b"hang", b"a")

    assert time.monotonic() - started < 10
    assert hung == (
        base64.b64encode(b"hang").decode(),
        False,
        "Decoder did not answer within 0.5s and was stopped",
    )
    assert after.decoded


def test_close_ends_the_decoder_through_its_stdin(tmp_path):
    _write_decoder(tmp_path)
    decoder = _decoder(tmp_path)

    async def scenario():
        rendered = await render(decoder, b"a", "s")
        process = decoder._process
        await decoder.close()
        return rendered, process

    rendered, process = asyncio.run(scenario())

    assert rendered.decoded
    assert process.returncode == 0


# ---------------------------------------------------------------------------
# Errors and the input
# ---------------------------------------------------------------------------


def test_broken_decoder_keeps_every_payload():
    rendered = asyncio.run(render(BrokenDecoder("d", "Invalid decoder 'a/b'"), PAYLOAD, "a"))
    assert rendered == (PAYLOAD_B64, False, "Invalid decoder 'a/b'")


def test_decode_error_log_is_rate_limited():
    lines = []
    now = [100.0]
    log = DecodeErrorLog(
        lambda msg, *args: lines.append(msg % args), "tesla", 60, lambda: now[0]
    )

    log.record("a.1", "boom 1")
    for i in range(2, 6):
        now[0] += 10
        log.record(f"a.{i}", f"boom {i}")
    now[0] += 20
    log.record("a.6", "boom 6")

    assert lines == [
        "Decoder 'tesla' failed to decode 1 message(s); the payload is kept as base64. "
        "Last failure on subject 'a.1': boom 1",
        "Decoder 'tesla' failed to decode 5 message(s); the payload is kept as base64. "
        "Last failure on subject 'a.6': boom 6",
    ]


def test_subscribe_input_creates_its_decoder(tmp_path, monkeypatch, caplog):
    import nats_subscribe_helper

    monkeypatch.setattr(nats_decoder, "DECODER_DIR", str(tmp_path))

    assert nats_subscribe_helper._create_decoder({"decoder": ""}) is None

    broken = nats_subscribe_helper._create_decoder({"decoder": "../x"})
    assert isinstance(broken, BrokenDecoder)
    assert "Invalid decoder '../x'" in caplog.text

    # A decoder that is not in place yet is still used: it is checked at each start
    later = nats_subscribe_helper._create_decoder({"decoder": "decoder", "decoder_args": "-v"})
    assert later.args == ["-v"]
    assert "Decoder not found" in caplog.text
    _write_decoder(tmp_path)
    assert _render_all(later, b"a")[0].decoded
