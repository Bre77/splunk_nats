"""
F8 regression tests: inputs and search commands log through solnlib to their own
file at the Logging tab's level, one line per record and nothing on stderr, and
write the events_ingested and exc_l records the UCC Monitoring Dashboard reads.
"""

import asyncio
import logging
import os
import subprocess
import sys
import textwrap

import nats
import nats_connection
import nats_logging
import natsutil
import pytest
from nats.js.errors import BucketNotFoundError

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "package", "bin")


def records(path):
    with open(path) as f:
        return f.read().splitlines()


def fake_level(monkeypatch, value):
    monkeypatch.setattr(
        nats_logging.conf_manager, "get_log_level", lambda **kwargs: value
    )


def test_input_logs_everything_to_its_own_file_one_line_per_record(addon_log_dir, monkeypatch):
    fake_level(monkeypatch, "INFO")
    nats_logging.setup_input("nats_subscribe://demo", "session-key")

    logging.getLogger("nats_subscribe_helper").warning("first\nsecond")
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        # Third-party loggers (nats-py, asyncio) propagate to the same file
        logging.getLogger("nats.aio.client").exception("callback failed")
    logging.getLogger("nats_connection").debug("not at INFO")

    lines = records(addon_log_dir / "nats_subscribe_demo.log")
    assert len(lines) == 2
    assert "log_level=WARNING" in lines[0] and lines[0].endswith("| first\\nsecond")
    assert "log_level=ERROR" in lines[1]
    assert "callback failed\\nTraceback (most recent call last):\\n" in lines[1]
    assert lines[1].endswith("RuntimeError: boom")


@pytest.mark.parametrize(
    "setting, level",
    [
        ("DEBUG", logging.DEBUG),
        ("INFO", logging.INFO),
        ("WARN", logging.WARNING),
        ("ERROR", logging.ERROR),
        ("CRITICAL", logging.CRITICAL),
        ("bogus", logging.INFO),
    ],
)
def test_level_comes_from_the_logging_setting(monkeypatch, setting, level):
    fake_level(monkeypatch, setting)
    nats_logging.setup_command("natskv", "session-key")
    assert logging.getLogger().level == level


def test_command_failure_is_one_labelled_line_in_the_command_log(addon_log_dir, monkeypatch):
    import nats_subscribe_command as cmd

    def missing(command_instance, account):
        raise nats_connection.AccountConfigError("Account configuration 'nope' not found")

    class Command:
        subject = "a.>"
        account = "nope"
        service = None
        logger = logging.getLogger("NatssubscribeCommand")

        def error_exit(self, error, message=None):
            # splunklib repeats the error, unlabelled; it stays in search.log
            self.logger.error("Abnormal exit: %s", error)
            raise SystemExit(1)

    monkeypatch.setattr(cmd, "_get_account_config", missing)
    with pytest.raises(SystemExit):
        list(cmd.generate(Command()))

    errors = [
        line
        for line in records(addon_log_dir / "nats_natssubscribe.log")
        if "log_level=ERROR" in line
    ]
    assert len(errors) == 1
    assert errors[0].endswith(
        "| exc_l=\"Configuration Error\" natssubscribe failed: "
        "Account configuration 'nope' not found"
    )


def test_ingestion_counts_are_logged_for_the_dashboard_whatever_the_level(addon_log_dir, monkeypatch):
    fake_level(monkeypatch, "ERROR")
    nats_logging.setup_input("nats_subscribe://demo", "session-key")
    ingestion = nats_logging.Ingestion("nats_subscribe://demo", "default", "acct")

    ingestion.add("nats:message")
    ingestion.add("nats:message")
    ingestion.add("nats:json", 3)
    ingestion.flush()
    ingestion.flush()  # nothing new to count

    lines = records(addon_log_dir / "nats_subscribe_demo.log")
    assert [line.split("| ", 1)[1] for line in lines] == [
        "action=events_ingested modular_input_name=nats_subscribe://demo "
        "sourcetype_ingested=nats:message n_events=2 event_input=demo "
        "event_index=main event_account=acct",
        "action=events_ingested modular_input_name=nats_subscribe://demo "
        "sourcetype_ingested=nats:json n_events=3 event_input=demo "
        "event_index=main event_account=acct",
    ]
    assert all("log_level=INFO" in line for line in lines)


def test_running_input_picks_up_a_logging_level_change(addon_log_dir, monkeypatch):
    fake_level(monkeypatch, "INFO")
    nats_logging.setup_input("nats_kv://cfg", "session-key")
    setting = ["INFO"]
    ingestion = nats_logging.Ingestion("nats_kv://cfg", "nats", "acct")
    housekeeping = nats_logging.InputHousekeeping(
        ingestion,
        "session-key",
        read=lambda session_key: nats_logging.parse_level(setting[0]),
        interval=0.05,
    )

    async def scenario():
        stop = asyncio.Event()
        task = asyncio.ensure_future(housekeeping.run(stop))
        await asyncio.sleep(0.2)
        assert logging.getLogger().level == logging.INFO
        setting[0] = "DEBUG"
        await asyncio.sleep(0.2)
        assert logging.getLogger().level == logging.DEBUG
        ingestion.add("nats:kv", 4)
        stop.set()
        await asyncio.wait_for(task, 5)

    asyncio.run(scenario())

    lines = records(addon_log_dir / "nats_kv_cfg.log")
    assert sum("Logging level changed to DEBUG" in line for line in lines) == 1
    # Counts still held when the input stops are written
    assert "sourcetype_ingested=nats:kv n_events=4" in lines[-1]


def test_nothing_reaches_stderr(tmp_path):
    # Splunk logs every stderr line of an input to splunkd.log as an ERROR, so a
    # traceback became one ERROR per line. Everything goes to the input's log.
    log_dir = tmp_path / "splunk-log"
    log_dir.mkdir()
    script = textwrap.dedent(
        """
        import asyncio, logging, sys, warnings
        sys.path.insert(0, sys.argv[1])
        import nats_connection, nats_logging
        from solnlib import log

        log.Logs.set_context(directory=sys.argv[2])  # $SPLUNK_HOME/var/log/splunk
        nats_logging.setup_input("nats_subscribe://demo", None)

        async def session(connection):
            pass

        async def main():
            stop = asyncio.Event()
            asyncio.get_running_loop().call_later(1.5, stop.set)
            await nats_connection.run_forever(
                "nats_subscribe://demo",
                lambda: {"servers": "nats://127.0.0.1:%d"},
                session,
                stop=stop,
                backoff=nats_connection.Backoff(base=0.1, cap=0.2),
            )

        asyncio.run(main())

        async def fail():
            raise RuntimeError("never retrieved")

        loop = asyncio.new_event_loop()
        loop.run_until_complete(asyncio.sleep(0, loop.create_task(fail())))
        loop.close()
        import gc; gc.collect()
        warnings.warn("deprecated thing", DeprecationWarning)
        logging.getLogger("nats.aio.client").error("nats-py error")
        """
        % natsutil.free_port()
    )
    result = subprocess.run(
        [sys.executable, "-W", "always", "-c", script, BIN_DIR, str(log_dir)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""

    lines = records(log_dir / "nats_subscribe_demo.log")
    text = "\n".join(lines)
    connect_errors = [line for line in lines if "cannot connect to NATS" in line]
    assert len(connect_errors) == 1
    assert 'log_level=ERROR' in connect_errors[0]
    assert 'exc_l="Connection Error" nats_subscribe://demo: cannot connect' in connect_errors[0]
    assert "Task exception was never retrieved" in text
    assert "deprecated thing" in text
    assert "nats-py error" in text
    assert all(line[:4].isdigit() for line in lines)


def _cause(error, cause):
    try:
        raise error from cause
    except Exception as e:
        return e


@pytest.mark.parametrize(
    "error, label",
    [
        (nats_connection.AccountConfigError("Account configuration 'x' not found"), "Configuration Error"),
        (ValueError("count must be an integer"), "Configuration Error"),
        (
            _cause(
                nats_connection.NatsConnectError("Authorization violation from nats://h:4222"),
                nats.errors.Error("nats: 'Authorization Violation'"),
            ),
            "Authentication Error",
        ),
        (nats_connection.NatsConnectError("Permissions violation: Subscription to \"x\""), "Permission Error"),
        (
            _cause(
                nats_connection.NatsConnectError("Connection refused by nats://h:4222"),
                ConnectionRefusedError(),
            ),
            "Connection Error",
        ),
        (nats_connection.NatsConnectError("Connection to NATS closed"), "Connection Error"),
        (
            _cause(nats_connection.NatsConnectError("Failed to monitor KV bucket"), BucketNotFoundError()),
            "NATS Error",
        ),
        (asyncio.TimeoutError(), "Connection Error"),
        (TypeError("bug"), "Unexpected Error"),
    ],
)
def test_error_labels_group_failures_on_the_dashboard(error, label):
    assert nats_connection.error_label(error) == label


def test_unexpected_failure_keeps_its_traceback_on_one_line(addon_log_dir):
    nats_logging.setup_input("nats_kv://cfg", None)
    try:
        {}["missing"]
    except KeyError as e:
        nats_connection.log_failure(logging.getLogger("x"), e, "%s: failed", "nats_kv://cfg")

    (line,) = records(addon_log_dir / "nats_kv_cfg.log")
    assert "file=test_logging.py:test_unexpected_failure" in line
    assert '| exc_l="Unexpected Error" nats_kv://cfg: failed: missing\\nTraceback' in line
    assert line.endswith("KeyError: 'missing'")


def test_subscribe_input_writes_input_source_and_counts_events(start_nats_server):
    import nats_subscribe_helper

    server = start_nats_server()

    class Writer:
        def __init__(self):
            self.events = []

        def write_event(self, event):
            self.events.append(event)

    async def scenario():
        writer = Writer()
        ingestion = nats_logging.Ingestion("nats_subscribe://demo", "nats", "acct")
        connection = await nats_connection.connect({"servers": server.url})
        publisher = await nats.connect(server.url)
        try:
            await nats_subscribe_helper._subscribe_to_nats(
                connection=connection,
                subject="demo.>",
                sourcetype="nats:message",
                event_writer=writer,
                input_name="nats_subscribe://demo",
                ingestion=ingestion,
            )
            for subject in ("demo.a", "demo.b"):
                await publisher.publish(subject, b"{}")
            await publisher.flush()
            for _ in range(100):
                if len(writer.events) == 2:
                    break
                await asyncio.sleep(0.02)
        finally:
            await publisher.close()
            await connection.close()
        return writer.events, ingestion

    events, ingestion = asyncio.run(scenario())
    # props.conf/transforms.conf move the subject to nats_subject at index time
    assert [e.source for e in events] == [
        "nats_subscribe://demo demo.a",
        "nats_subscribe://demo demo.b",
    ]
    assert ingestion._counts == {"nats:message": 2}


def test_unreadable_logging_setting_falls_back_to_info(addon_log_dir, monkeypatch):
    def unreachable(**kwargs):
        raise ConnectionRefusedError("splunkd is not answering")

    monkeypatch.setattr(nats_logging.conf_manager, "get_log_level", unreachable)
    nats_logging.setup_input("nats_subscribe://demo", "session-key")

    assert logging.getLogger().level == logging.INFO
    (line,) = records(addon_log_dir / "nats_subscribe_demo.log")
    assert line.endswith("Failed to read the Logging level, using INFO: splunkd is not answering")
