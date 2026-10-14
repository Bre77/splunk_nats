import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest
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


class FakeKV:
    async def history(self, key):
        return [
            SimpleNamespace(
                key="cfg.a",
                value=b'{"x": 1}',
                revision=3,
                created=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        ]


class FakeJetStream:
    async def key_value(self, bucket):
        return FakeKV()


class FakeNats:
    connected_url = urlparse("nats://nats.example:4222")

    def jetstream(self, domain=None):
        return FakeJetStream()

    async def close(self):
        pass


def test_results_use_kv_sourcetype_and_host_form(monkeypatch):
    async def connect(**kwargs):
        return FakeNats()

    monkeypatch.setattr(cmd.nats, "connect", connect)

    events = asyncio.run(
        cmd._get_kv_history("bucket", ">", {"servers": "nats://nats.example:4222"})
    )

    assert events[0]["sourcetype"] == "nats:kv"
    assert events[0]["host"] == "nats.example:4222"


def test_nats_error_reported_once_without_generic_location(monkeypatch):
    async def fail(*args):
        raise BucketNotFoundError()

    monkeypatch.setattr(cmd, "_get_account_config", lambda c, a: {"servers": "s"})
    monkeypatch.setattr(cmd, "_get_kv_history", fail)
    command = FakeCommand(account="acct", bucket="nope", key=">")

    with pytest.raises(ErrorExit):
        list(cmd.generate(command))

    assert command.errors == [str(BucketNotFoundError())]
