import asyncio
import json

import nats
import pytest
from nats.js.api import KeyValueConfig
from nats.js.errors import BucketNotFoundError

import nats_kv_command as cmd


class ErrorExit(Exception):
    pass


class FakeCommand:
    def __init__(self, **options):
        self.errors = []
        for name, value in options.items():
            setattr(self, name, value)

    def error_exit(self, error, message=None):
        self.errors.append(message)
        raise ErrorExit(message)


def test_nats_error_reported_once_without_generic_location(monkeypatch):
    async def fail(*args):
        raise BucketNotFoundError()

    monkeypatch.setattr(cmd, "_get_account_config", lambda c, a: {"servers": "s"})
    monkeypatch.setattr(cmd, "_get_kv_history", fail)
    command = FakeCommand(account="acct", bucket="nope", key=">")

    with pytest.raises(ErrorExit):
        list(cmd.generate(command))

    assert command.errors == [
        "KV bucket 'nope' was not found. Check the bucket name. If the bucket is "
        "in a JetStream domain, set domain=<domain>."
    ]


def fill_bucket(url):
    async def setup():
        nc = await nats.connect(url)
        kv = await nc.jetstream().create_key_value(
            KeyValueConfig(bucket="cfg", history=5)
        )
        await kv.put("a", b'{"v": 1}')
        await kv.delete("a")
        await kv.put("b", b"2")
        await kv.purge("b")
        await kv.put("c.x", b"3")
        await nc.close()

    asyncio.run(setup())


def run_command(url, monkeypatch, **options):
    monkeypatch.setattr(cmd, "_get_account_config", lambda c, a: {"servers": url})
    defaults = {"account": "acct", "bucket": "cfg", "key": ">", "domain": None}
    defaults.update(options)
    command = FakeCommand(**defaults)
    try:
        return list(cmd.generate(command)), command.errors
    except ErrorExit:
        return [], command.errors


def test_history_includes_deletes_and_purges_with_operation(nats_server, monkeypatch):
    fill_bucket(nats_server)
    rows, errors = run_command(nats_server, monkeypatch)
    assert errors == []
    assert [(r["source"], r["operation"]) for r in rows] == [
        ("a", "PUT"),
        ("a", "DEL"),
        ("b", "PURGE"),
        ("c.x", "PUT"),
    ]
    assert rows[0]["_raw"] == '{"v": 1}'
    assert json.loads(rows[1]["_raw"]) == {
        "key": "a",
        "operation": "DEL",
        "revision": 2,
    }
    assert all(isinstance(r["_time"], float) for r in rows)
    assert {r["sourcetype"] for r in rows} == {"nats:kv"}
    assert {r["host"] for r in rows} == {nats_server[len("nats://") :]}


def test_key_filter(nats_server, monkeypatch):
    fill_bucket(nats_server)
    rows, errors = run_command(nats_server, monkeypatch, key="c.*")
    assert [r["source"] for r in rows] == ["c.x"]


def test_empty_bucket_returns_no_rows(nats_server, monkeypatch):
    async def setup():
        nc = await nats.connect(nats_server)
        await nc.jetstream().create_key_value(KeyValueConfig(bucket="cfg"))
        await nc.close()

    asyncio.run(setup())
    assert run_command(nats_server, monkeypatch) == ([], [])


@pytest.mark.parametrize(
    "options, expected",
    [
        ({"bucket": "missing"}, "KV bucket 'missing' was not found."),
        ({"domain": "WRONG"}, "JetStream did not respond in domain 'WRONG'"),
    ],
)
def test_errors_are_reported_in_user_terms(nats_server, monkeypatch, options, expected):
    fill_bucket(nats_server)
    rows, errors = run_command(nats_server, monkeypatch, **options)
    assert len(errors) == 1 and expected in errors[0]
    assert "code=None" not in errors[0]
