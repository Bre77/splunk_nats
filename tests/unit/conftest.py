import logging
import os
import shutil
import socket
import subprocess
import sys
import time

import pytest

# The add-on modules live in package/bin; their dependencies (nats-py, solnlib,
# splunk-sdk) come from tests/requirements.txt instead of the vendored lib/.
# Keep __pycache__ out of package/bin, which ucc-gen copies into the add-on.
sys.dont_write_bytecode = True
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "package", "bin")
)
sys.path.insert(0, os.path.dirname(__file__))

import natsutil  # noqa: E402
from solnlib import log  # noqa: E402


@pytest.fixture(autouse=True)
def addon_log_dir(tmp_path, monkeypatch):
    """
    Inputs and commands log to files in $SPLUNK_HOME/var/log/splunk and route the
    root logger there; keep the files in tmp_path and undo the root logger setup.
    """
    log_dir = tmp_path / "log"
    log_dir.mkdir()
    monkeypatch.setattr(log.Logs, "_default_directory", str(log_dir))
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield log_dir
    for handler in root.handlers[:]:
        if handler not in handlers:
            root.removeHandler(handler)
            handler.close()
    root.setLevel(level)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def nats_server(tmp_path):
    """A throwaway nats-server with JetStream in domain TEST; yields its URL."""
    binary = shutil.which("nats-server")
    if not binary:
        pytest.skip("nats-server binary not found on PATH")
    port = _free_port()
    config = tmp_path / "server.conf"
    config.write_text(
        f'listen: "127.0.0.1:{port}"\n'
        f'jetstream {{ store_dir: "{tmp_path / "js"}", domain: "TEST" }}\n'
    )
    proc = subprocess.Popen(
        [binary, "-c", str(config)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)
        yield f"nats://127.0.0.1:{port}"
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture
def start_nats_server(tmp_path):
    """Start nats-server processes with a given config (and port); stopped at teardown."""
    if not natsutil.NATS_SERVER:
        message = "nats-server binary not found (set NATS_SERVER_BIN)"
        if os.environ.get("CI"):
            pytest.fail(message)
        pytest.skip(message)
    servers = []

    def start(config: str = "", port: int = 0) -> natsutil.NatsServer:
        server = natsutil.NatsServer(tmp_path, config, port).start()
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.stop()
