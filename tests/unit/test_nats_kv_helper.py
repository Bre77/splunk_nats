import asyncio
import json
import types

import nats
import pytest
from nats.js.api import KeyValueConfig

import nats_connection
import nats_kv_helper
import nats_logging
from nats_kv_helper import Checkpoint, _monitor_kv_bucket

BUCKET = "cfg"
DOMAIN = "TEST"


class FakeStore:
    """Stands in for solnlib's KVStoreCheckpointer (JSON round trip included)."""

    def __init__(self, data=None):
        self.data = dict(data or {})

    def get(self, key):
        return self.data.get(key)

    def update(self, key, state):
        self.data[key] = json.loads(json.dumps(state))


class FakeWriter:
    def __init__(self):
        self.events = []

    def write_event(self, event):
        self.events.append(event)


async def eventually(condition, timeout=10):
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


async def open_bucket(url, history=5):
    nc = await nats.connect(url)
    js = nc.jetstream(domain=DOMAIN)
    kv = await js.create_key_value(KeyValueConfig(bucket=BUCKET, history=history))
    return nc, js, kv


class Monitor:
    """Runs _monitor_kv_bucket in the background, like one Splunk input process."""

    def __init__(
        self, url, store, subject=">", domain=DOMAIN, bucket=BUCKET, checkpoint=None
    ):
        self.writer = FakeWriter()
        self.ingestion = nats_logging.Ingestion("nats_kv://kvcfg", "nats", "acct")
        if checkpoint is None:
            checkpoint = Checkpoint(store, "kvcfg", bucket, domain)
            checkpoint.load()
        self.checkpoint = checkpoint
        self.task = asyncio.ensure_future(
            self._session(url, bucket, subject, domain)
        )

    async def _session(self, url, bucket, subject, domain):
        connection = await nats_connection.connect({"servers": url})
        try:
            await _monitor_kv_bucket(
                connection=connection,
                bucket=bucket,
                subject=subject,
                sourcetype="nats:kv",
                event_writer=self.writer,
                checkpoint=self.checkpoint,
                domain=domain,
                input_name="nats_kv://kvcfg",
                ingestion=self.ingestion,
            )
        finally:
            await connection.close()

    @property
    def raws(self):
        return [e.data for e in self.writer.events]

    async def stop(self):
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass


def run(coro):
    asyncio.run(coro)


def test_first_run_indexes_bucket_with_entry_times_and_checkpoints_after_snapshot(
    nats_server,
):
    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b'{"v": 1}')
        await kv.put("b", b"\xff\xfe")
        store = FakeStore()
        monitor = Monitor(nats_server, store)
        # Checkpoint is written once the initial snapshot is read, while still running
        await eventually(lambda: store.get("kvcfg") is not None)
        assert store.get("kvcfg") == {"bucket": BUCKET, "domain": DOMAIN, "revision": 2}
        await monitor.stop()

        history = {e.key: e for k in ("a", "b") for e in await kv.history(k)}
        events = monitor.writer.events
        # The source is the input name, then the subject (moved to nats_subject
        # at index time).
        assert [e.source for e in events] == [
            "nats_kv://kvcfg $KV.cfg.a",
            "nats_kv://kvcfg $KV.cfg.b",
        ]
        assert monitor.raws == ['{"v": 1}', "//4="]
        # Written events are counted for the Monitoring Dashboard
        assert monitor.ingestion._counts == {"nats:kv": 2}
        for event in events:
            key = event.source.rsplit(".", 1)[1]
            # Regression: _time must be the entry created time as epoch seconds
            assert event.time == history[key].created.timestamp()
            assert event.sourceType == "nats:kv"
        await nc.close()

    run(scenario())


def test_restart_resumes_after_checkpoint_without_duplicates(nats_server):
    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        for i in range(5):
            await kv.put(f"k{i}", f"v{i}".encode())
        store = FakeStore()
        first = Monitor(nats_server, store)
        await eventually(lambda: len(first.raws) == 5)
        await first.stop()

        # Changes while the input is down are picked up after the restart
        await kv.put("k1", b"v1-updated")
        await kv.put("k5", b"v5")
        second = Monitor(nats_server, store)
        assert second.checkpoint.revision == 5
        await eventually(lambda: len(second.raws) == 2)
        await asyncio.sleep(0.5)
        assert second.raws == ["v1-updated", "v5"]
        await second.stop()
        assert store.get("kvcfg")["revision"] == 7

        # A restart with nothing new writes nothing
        third = Monitor(nats_server, store)
        await asyncio.sleep(1)
        assert third.raws == []
        await third.stop()
        await nc.close()

    run(scenario())


def test_deletes_and_purges_are_indexed_with_their_operation(nats_server):
    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b"1")
        await kv.delete("a")
        await kv.put("b", b"2")
        await kv.purge("b")
        monitor = Monitor(nats_server, FakeStore())
        await eventually(lambda: len(monitor.raws) >= 3)
        await asyncio.sleep(0.5)
        await monitor.stop()
        # Purge rolls up b's history, so its put is gone from the stream
        assert monitor.raws[0] == "1"
        assert [json.loads(r) for r in monitor.raws[1:]] == [
            {"key": "a", "operation": "DEL", "revision": 2},
            {"key": "b", "operation": "PURGE", "revision": 4},
        ]
        assert [e.source.rsplit(" ", 1)[1] for e in monitor.writer.events] == [
            "$KV.cfg.a",
            "$KV.cfg.a",
            "$KV.cfg.b",
        ]
        await nc.close()

    run(scenario())


def test_events_held_at_shutdown_are_neither_written_nor_checkpointed(
    nats_server, monkeypatch
):
    monkeypatch.setattr(nats_kv_helper, "CHECKPOINT_INTERVAL", 3600)

    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b"1")
        store = FakeStore()
        first = Monitor(nats_server, store)
        # The initial snapshot is checkpointed and written straight away
        await eventually(lambda: first.raws == ["1"])
        assert store.get("kvcfg")["revision"] == 1
        await kv.put("b", b"2")
        await kv.put("c", b"3")
        await eventually(lambda: first.checkpoint.revision == 3)
        await first.stop()
        # b and c were held, so they are neither written nor checkpointed...
        assert first.raws == ["1"]
        assert store.get("kvcfg")["revision"] == 1

        # ...and the next run picks them up
        second = Monitor(nats_server, store)
        await eventually(lambda: second.raws == ["2", "3"])
        await second.stop()
        await nc.close()

    run(scenario())


class SwitchableStore(FakeStore):
    """A KV Store that can be made unavailable, like splunkd while it stops."""

    available = True

    def update(self, key, state):
        if not self.available:
            raise ConnectionRefusedError("splunkd is not serving the KV Store")
        super().update(key, state)


def test_events_are_held_while_the_checkpoint_cannot_be_saved(nats_server, monkeypatch):
    # Regression: during a Splunk restart splunkd stops serving the KV Store
    # well before it stops inputs. Events written in that window could not be
    # checkpointed and were indexed again after the restart.
    monkeypatch.setattr(nats_kv_helper, "CHECKPOINT_INTERVAL", 0.2)
    monkeypatch.setattr(nats_kv_helper, "SAVE_RETRY_INTERVAL", 0.3)

    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b"1")
        store = SwitchableStore()
        monitor = Monitor(nats_server, store)
        await eventually(lambda: monitor.raws == ["1"])
        store.available = False
        await kv.put("b", b"2")
        await kv.put("c", b"3")
        await eventually(lambda: monitor.checkpoint.save_failed)
        held = monitor.checkpoint.revision
        # Reading pauses after the failed save: d is not taken off the consumer
        await kv.put("d", b"4")
        await asyncio.sleep(1)
        assert monitor.raws == ["1"]
        assert store.get("kvcfg")["revision"] == 1
        assert monitor.checkpoint.revision == held
        store.available = True
        await eventually(lambda: monitor.raws == ["1", "2", "3", "4"])
        assert store.get("kvcfg")["revision"] == 4
        await monitor.stop()
        await nc.close()

    run(scenario())


class ClosedPipeWriter(FakeWriter):
    """Splunk closes the event pipe when it stops an input."""

    closed = False

    def write_event(self, event):
        if self.closed:
            raise BrokenPipeError(32, "Broken pipe")
        super().write_event(event)


def test_checkpoint_moves_back_when_the_events_cannot_be_written(
    nats_server, monkeypatch
):
    monkeypatch.setattr(nats_kv_helper, "CHECKPOINT_INTERVAL", 0.2)

    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b"1")
        store = FakeStore()
        monitor = Monitor(nats_server, store)
        monitor.writer.__class__ = ClosedPipeWriter
        await eventually(lambda: monitor.raws == ["1"])
        monitor.writer.closed = True
        await kv.put("b", b"2")
        with pytest.raises(nats_connection.InputStopped):
            await asyncio.wait_for(monitor.task, 10)
        assert store.get("kvcfg")["revision"] == 1
        await nc.close()

    run(scenario())


def test_failed_first_write_rolls_back_to_reading_everything(nats_server, monkeypatch):
    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b"1")
        store = FakeStore()
        monitor = Monitor(nats_server, store)
        monitor.writer.__class__ = ClosedPipeWriter
        monitor.writer.closed = True
        with pytest.raises(nats_connection.InputStopped):
            await asyncio.wait_for(monitor.task, 10)
        assert store.get("kvcfg")["revision"] == 0

        again = Monitor(nats_server, store)
        await eventually(lambda: again.raws == ["1"])
        await again.stop()
        await nc.close()

    run(scenario())


def test_checkpoint_is_saved_periodically_while_running(nats_server, monkeypatch):
    monkeypatch.setattr(nats_kv_helper, "CHECKPOINT_INTERVAL", 0.2)

    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b"1")
        store = FakeStore()
        monitor = Monitor(nats_server, store)
        await eventually(lambda: store.get("kvcfg") is not None)
        await kv.put("b", b"2")
        await eventually(lambda: store.get("kvcfg")["revision"] == 2)
        await monitor.stop()
        await nc.close()

    run(scenario())


def test_key_filter_limits_the_keys(nats_server):
    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("cars.one", b"1")
        await kv.put("trucks.one", b"2")
        await kv.put("cars.two", b"3")
        monitor = Monitor(nats_server, FakeStore(), subject="cars.*")
        await eventually(lambda: len(monitor.raws) == 2)
        await asyncio.sleep(0.5)
        await monitor.stop()
        assert monitor.raws == ["1", "3"]
        await nc.close()

    run(scenario())


def test_checkpoint_of_another_bucket_is_ignored(nats_server):
    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b"1")
        store = FakeStore(
            {"kvcfg": {"bucket": "other", "domain": DOMAIN, "revision": 1}}
        )
        monitor = Monitor(nats_server, store)
        assert monitor.checkpoint.revision is None
        await eventually(lambda: len(monitor.raws) == 1)
        await monitor.stop()
        await nc.close()

    run(scenario())


def test_checkpoint_past_the_end_of_a_recreated_bucket_rereads_it(nats_server):
    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b"1")
        await kv.put("b", b"2")
        store = FakeStore(
            {"kvcfg": {"bucket": BUCKET, "domain": DOMAIN, "revision": 50}}
        )
        monitor = Monitor(nats_server, store)
        await eventually(lambda: len(monitor.raws) == 2)
        await monitor.stop()
        assert store.get("kvcfg")["revision"] == 2
        await nc.close()

    run(scenario())


@pytest.mark.parametrize(
    "bucket, domain, expected",
    [
        (
            "missing",
            DOMAIN,
            "KV bucket 'missing' was not found in JetStream domain 'TEST'",
        ),
        (BUCKET, "WRONG", "JetStream did not respond in domain 'WRONG'"),
    ],
)
def test_errors_are_reported_in_user_terms(nats_server, bucket, domain, expected):
    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        monitor = Monitor(nats_server, FakeStore(), bucket=bucket, domain=domain)
        try:
            with pytest.raises(nats_connection.NatsConnectError) as excinfo:
                await asyncio.wait_for(monitor.task, 30)
        finally:
            await nc.close()
        return str(excinfo.value)

    message = asyncio.run(scenario())
    assert expected in message
    assert "code=None" not in message


def test_events_held_when_a_session_ends_are_read_by_the_next_session(
    nats_server, monkeypatch
):
    # The input reconnects with the same Checkpoint object, so a new session must
    # start after the last saved revision, not after events the old one only held.
    monkeypatch.setattr(nats_kv_helper, "CHECKPOINT_INTERVAL", 3600)

    async def scenario():
        nc, js, kv = await open_bucket(nats_server)
        await kv.put("a", b"1")
        store = FakeStore()
        first = Monitor(nats_server, store)
        await eventually(lambda: first.raws == ["1"])
        await kv.put("b", b"2")
        await kv.put("c", b"3")
        await eventually(lambda: first.checkpoint.revision == 3)
        await first.stop()

        second = Monitor(nats_server, store, checkpoint=first.checkpoint)
        await eventually(lambda: second.raws == ["2", "3"])
        await second.stop()
        await nc.close()

    run(scenario())


def test_bucket_name_validation_message_does_not_allow_periods():
    definition = types.SimpleNamespace(
        parameters={"bucket": "eu.config", "subject": ">", "account": "a"},
        metadata={},
    )
    with pytest.raises(Exception) as excinfo:
        nats_kv_helper.validate_input(definition)
    message = str(excinfo.value)
    assert "letters, digits, hyphens and underscores" in message
    assert "period" not in message


def test_open_checkpoint_waits_for_the_kv_store_instead_of_starting_over(monkeypatch):
    # Regression: on Splunk 10.6 the input started while the KV Store was still
    # initializing (HTTP 503), fell back to "no checkpoint" and indexed the
    # whole bucket again after every restart.
    statuses = iter(
        [
            RuntimeError("HTTP 503 KV Store is initializing"),
            "starting",
            "ready",
            "ready",
        ]
    )

    def kvstore_status(session_key):
        status = next(statuses)
        if isinstance(status, Exception):
            raise status
        return status

    class FlakyStore(FakeStore):
        reads = 0

        def get(self, key):
            FlakyStore.reads += 1
            if FlakyStore.reads == 1:
                raise RuntimeError("HTTP 503 KV Store is initializing")
            return super().get(key)

    sleeps = []
    monkeypatch.setattr(nats_kv_helper, "_kvstore_status", kvstore_status)
    monkeypatch.setattr(
        nats_kv_helper.checkpointer,
        "KVStoreCheckpointer",
        lambda *args: FlakyStore(
            {"kvcfg": {"bucket": BUCKET, "domain": "", "revision": 42}}
        ),
    )
    monkeypatch.setattr(nats_kv_helper.time, "sleep", sleeps.append)

    checkpoint = nats_kv_helper._open_checkpoint(
        "session", "nats_kv://kvcfg", BUCKET, None
    )
    assert checkpoint.revision == 42
    assert len(sleeps) == 3
