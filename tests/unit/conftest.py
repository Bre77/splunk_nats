import os
import shutil
import socket
import subprocess
import sys
import time

import pytest

# The add-on modules live in package/bin; their dependencies (nats-py, solnlib,
# splunk-sdk) come from tests/requirements.txt instead of the vendored lib/.
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "package", "bin")
)


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
