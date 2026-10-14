"""
F2 / F6 regression tests for the input supervisor (`nats_connection.run_forever`):
inputs reconnect forever with backoff and jitter, never sit idle on a closed
connection, retry an initial connect failure, pick up a corrected Account without
a restart, and log each disconnect / reconnect / failure reason once.
"""

import asyncio
import logging
import os
import signal

import nats
import nats_connection
import natsutil
import pytest

PASSWORD_AUTH = 'authorization {{ users: [ {{user: "splunk", password: "{password}", permissions: {{subscribe: {{allow: ["telemetry.>", "_INBOX.>"]}}}}}} ] }}'
FAST = dict(base=0.05, cap=0.3)


class Input:
    """Runs run_forever() as a subscribe input and records what it receives."""

    def __init__(self, account, subject="telemetry.>", pull=False):
        self.account = account
        self.subject = subject
        # pull=True reads with next_msg() like the KV input, so the session itself
        # fails when the connection drops.
        self.pull = pull
        self.received = []
        self.loads = 0
        self.stop = asyncio.Event()
        self.log = logging.getLogger("test-input")

    def load_account(self):
        self.loads += 1
        return dict(self.account)

    async def session(self, connection):
        async def handler(msg):
            self.received.append(msg.data)

        if not self.pull:
            await connection.nc.subscribe(self.subject, cb=handler)
            await connection.confirm()
            return
        sub = await connection.nc.subscribe(self.subject)
        await connection.confirm()
        while True:
            msg = await sub.next_msg(timeout=None)
            self.received.append(msg.data)

    def start(self):
        self.task = asyncio.ensure_future(
            nats_connection.run_forever(
                "nats_subscribe://test",
                self.load_account,
                self.session,
                log=self.log,
                stop=self.stop,
                backoff=nats_connection.Backoff(**FAST),
            )
        )

    async def shutdown(self):
        self.stop.set()
        await asyncio.wait_for(self.task, 10)


async def publish_until_received(url, inp, payload, timeout=10.0, **connect):
    """Publish until the input has received `payload` (it may be reconnecting)."""
    publisher = await nats.connect(url, **connect)
    try:
        deadline = asyncio.get_running_loop().time() + timeout
        while payload not in inp.received:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"input never received {payload!r}")
            await publisher.publish("telemetry.vin.data", payload)
            await publisher.flush()
            await asyncio.sleep(0.05)
    finally:
        await publisher.close()


def messages(caplog, needle):
    return [r for r in caplog.records if r.name == "test-input" and needle in r.getMessage()]


def run(coro, timeout=60):
    return asyncio.run(asyncio.wait_for(coro, timeout))


@pytest.mark.parametrize("pull", [False, True], ids=["callback", "next_msg"])
def test_long_outage_reconnects_and_flow_resumes(start_nats_server, caplog, pull):
    # Before the fix nats-py gave up after 60 reconnect attempts (about 2 minutes),
    # closed the connection, and the input sat idle forever.
    caplog.set_level(logging.DEBUG)
    server = start_nats_server()

    async def scenario():
        proxy = natsutil.TcpProxy(server.port)
        await proxy.start()
        inp = Input(
            {"servers": f"nats://127.0.0.1:{proxy.port}", "connect_timeout": "1"},
            pull=pull,
        )
        inp.start()
        await publish_until_received(server.url, inp, b"before")

        await proxy.stop()
        # Far more failed attempts than nats-py's default reconnect budget would allow
        # within the outage, each re-reading the Account.
        loads_before = inp.loads
        await asyncio.sleep(3)
        assert inp.loads - loads_before >= 8

        await proxy.start()
        await publish_until_received(server.url, inp, b"after")
        await inp.shutdown()
        await proxy.stop()

    run(scenario())
    disconnects = messages(caplog, "disconnected from")
    assert len(disconnects) == 1
    assert "Connection lost" in disconnects[0].getMessage()
    assert len(messages(caplog, "reconnected to")) == 1
    # Each failure reason is logged once at ERROR, repeats only at DEBUG.
    errors = [r for r in messages(caplog, "cannot connect to NATS") if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "Connection refused" in errors[0].getMessage()
    assert messages(caplog, "stopped")


def test_initial_connect_failure_is_retried_until_server_appears(start_nats_server, caplog):
    # Before the fix an input that could not connect at start-up logged one error and exited.
    caplog.set_level(logging.INFO)
    port = natsutil.free_port()

    async def scenario():
        inp = Input({"servers": f"nats://127.0.0.1:{port}"})
        inp.start()
        await asyncio.sleep(1)
        assert not inp.task.done()
        server = start_nats_server(port=port)
        await publish_until_received(server.url, inp, b"hello")
        await inp.shutdown()

    run(scenario())
    assert len(messages(caplog, "cannot connect to NATS")) == 1
    assert len(messages(caplog, ": connected to")) == 1


def test_bad_credentials_then_fixed_account_recovers_without_restart(start_nats_server, caplog):
    # Before the fix the input exited on the authorization failure and a corrected
    # Account was only picked up after a Splunk restart.
    caplog.set_level(logging.INFO)
    server = start_nats_server(PASSWORD_AUTH.format(password="right"))

    async def scenario():
        inp = Input({"servers": server.url, "username": "splunk", "password": "wrong"})
        inp.start()
        await asyncio.sleep(1)
        assert not inp.received
        inp.account["password"] = "right"
        await publish_until_received(server.url, inp, b"hello", user="splunk", password="right")
        await inp.shutdown()

    run(scenario())
    errors = messages(caplog, "cannot connect to NATS")
    assert len(errors) == 1
    assert "Authorization violation" in errors[0].getMessage()


def test_rotated_password_then_updated_account_recovers(start_nats_server, caplog):
    caplog.set_level(logging.INFO)
    server = start_nats_server(PASSWORD_AUTH.format(password="old"))

    async def scenario():
        inp = Input({"servers": server.url, "username": "splunk", "password": "old"})
        inp.start()
        await publish_until_received(server.url, inp, b"one", user="splunk", password="old")

        server.restart(PASSWORD_AUTH.format(password="new"))
        await asyncio.sleep(1.5)
        inp.account["password"] = "new"
        await publish_until_received(server.url, inp, b"two", user="splunk", password="new")
        await inp.shutdown()

    run(scenario())
    assert len(messages(caplog, "disconnected from")) == 1
    auth = messages(caplog, "Authorization violation")
    assert len(auth) == 1
    assert len(messages(caplog, "reconnected to")) == 1


def test_permissions_violation_is_reported_and_retried(start_nats_server, caplog):
    caplog.set_level(logging.INFO)
    server = start_nats_server(PASSWORD_AUTH.format(password="pw"))

    async def scenario():
        inp = Input(
            {"servers": server.url, "username": "splunk", "password": "pw"}, subject="secret.>"
        )
        inp.start()
        await asyncio.sleep(1.5)
        await inp.shutdown()
        return inp

    inp = run(scenario())
    disconnects = messages(caplog, "disconnected from")
    assert len(disconnects) >= 1
    assert "Permissions violation" in disconnects[0].getMessage()
    # It keeps retrying (the server-side permissions may be fixed) instead of sitting idle.
    assert inp.loads >= 2


def test_backoff_is_capped_exponential_with_jitter():
    lows = nats_connection.Backoff(base=1, cap=60, rand=lambda: 0.0)
    highs = nats_connection.Backoff(base=1, cap=60, rand=lambda: 1.0)
    assert [lows.next() for _ in range(8)] == [0.5, 1, 2, 4, 8, 16, 30, 30]
    assert [highs.next() for _ in range(8)] == [1, 2, 4, 8, 16, 32, 60, 60]
    highs.reset()
    assert highs.next() == 1
    jittered = nats_connection.Backoff(base=1, cap=60)
    for _ in range(20):
        delay = jittered.next()
    assert 30 <= delay <= 60


def test_sigterm_stops_the_input_runs_cleanup_and_exits(start_nats_server):
    # Splunk stops an input with SIGTERM and closes its event pipe, so the input
    # closes the connection, runs its cleanup (e.g. stops a decoder) and exits
    # rather than returning to splunklib, which would write to the closed pipe.
    server = start_nats_server()
    cleaned_up = []

    async def session(connection):
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.sleep(30)

    async def cleanup():
        cleaned_up.append(True)

    with pytest.raises(SystemExit) as excinfo:
        nats_connection.run_input(
            "nats_kv://test", lambda: {"servers": server.url}, session, cleanup=cleanup
        )
    assert excinfo.value.code == 0
    assert cleaned_up == [True]


def test_session_reporting_input_stopped_ends_the_input(start_nats_server, caplog):
    # A closed event pipe means Splunk stopped reading: stop, never reconnect.
    caplog.set_level(logging.INFO)
    server = start_nats_server()
    sessions = []

    async def session(connection):
        sessions.append(connection)
        raise nats_connection.InputStopped("Splunk stopped reading events: Broken pipe")

    async def scenario():
        await nats_connection.run_forever(
            "nats_kv://test",
            lambda: {"servers": server.url},
            session,
            log=logging.getLogger("test-input"),
            backoff=nats_connection.Backoff(**FAST),
        )

    run(scenario(), timeout=10)
    assert len(sessions) == 1
    assert messages(caplog, "stopping: Splunk stopped reading events")
