"""Test helpers: throwaway nats-server processes, a killable TCP proxy, certificates and NKeys/JWTs."""

import asyncio
import base64
import hashlib
import json
import os
import shutil
import socket
import subprocess
import time

import nats_nkeys
from ecdsa import SigningKey
from ecdsa.curves import Ed25519

NATS_SERVER = os.environ.get("NATS_SERVER_BIN") or shutil.which("nats-server")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} did not open")


class NatsServer:
    """A nats-server process with the given configuration body, on a free port."""

    def __init__(self, tmp_path, config: str = "", port: int = 0):
        self.port = port or free_port()
        self.dir = tmp_path
        self.config_path = os.path.join(str(tmp_path), f"nats-{self.port}.conf")
        self.config = config
        self.proc = None

    @property
    def url(self) -> str:
        return f"nats://127.0.0.1:{self.port}"

    def start(self) -> "NatsServer":
        with open(self.config_path, "w") as f:
            f.write(f"listen: 127.0.0.1:{self.port}\n{self.config}\n")
        self.proc = subprocess.Popen(
            [NATS_SERVER, "-c", self.config_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        wait_for_port(self.port)
        return self

    def stop(self) -> None:
        if self.proc is not None:
            self.proc.terminate()
            self.proc.wait(10)
            self.proc = None

    def restart(self, config: str) -> None:
        self.stop()
        self.config = config
        self.start()


class TcpProxy:
    """A TCP proxy that can be taken down (dropping every connection) and brought back."""

    def __init__(self, target_port: int):
        self.target_port = target_port
        self.port = free_port()
        self._server = None
        self._writers = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", self.port)

    async def stop(self) -> None:
        self._server.close()
        # Drop live connections first: on Python 3.12+ wait_closed() waits for them.
        for writer in list(self._writers):
            writer.close()
        self._writers.clear()
        await self._server.wait_closed()

    async def _handle(self, reader, writer):
        try:
            up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self.target_port)
        except OSError:
            writer.close()
            return
        self._writers.update((writer, up_writer))

        async def pipe(src, dst):
            try:
                while True:
                    data = await src.read(65536)
                    if not data:
                        break
                    dst.write(data)
                    await dst.drain()
            except Exception:
                pass
            finally:
                dst.close()

        await asyncio.gather(pipe(reader, up_writer), pipe(up_reader, writer))


def make_certs(directory) -> dict:
    """A throwaway CA, a server certificate for 127.0.0.1/localhost and a client certificate."""
    d = str(directory)

    def run(*args):
        subprocess.run(["openssl", *args], check=True, capture_output=True, cwd=d)

    with open(os.path.join(d, "ca.ext"), "w") as f:
        f.write(
            "basicConstraints=critical,CA:TRUE\n"
            "keyUsage=critical,keyCertSign,cRLSign\n"
            "subjectKeyIdentifier=hash\n"
        )
    with open(os.path.join(d, "server.ext"), "w") as f:
        f.write(
            "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\n"
            "authorityKeyIdentifier=keyid\n"
        )
    with open(os.path.join(d, "client.ext"), "w") as f:
        f.write(
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=clientAuth\n"
            "authorityKeyIdentifier=keyid\n"
        )
    run("req", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.csr",
        "-subj", "/CN=nats-test-ca")
    run("x509", "-req", "-in", "ca.csr", "-key", "ca.key", "-days", "2",
        "-extfile", "ca.ext", "-out", "ca.pem")
    for name in ("server", "client"):
        run("req", "-newkey", "rsa:2048", "-nodes", "-keyout", f"{name}.key",
            "-out", f"{name}.csr", "-subj", f"/CN=nats-test-{name}")
        run("x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
            "-CAcreateserial", "-days", "2", "-extfile", f"{name}.ext", "-out", f"{name}.pem")
    return {name: os.path.join(d, name) for name in
            ("ca.pem", "server.pem", "server.key", "client.pem", "client.key")}


def read(path: str) -> str:
    with open(path) as f:
        return f.read()


def encode_seed(prefix: int, raw_seed: bytes) -> str:
    b1 = nats_nkeys.PREFIX_BYTE_SEED | (prefix >> 5)
    b2 = (prefix & 31) << 3
    payload = bytes([b1, b2]) + raw_seed
    crc = nats_nkeys._crc16(payload).to_bytes(2, "little")
    return base64.b32encode(payload + crc).rstrip(b"=").decode()


def new_nkey(prefix: int) -> "tuple[str, str]":
    """Returns (seed, public key)."""
    seed = encode_seed(prefix, os.urandom(32))
    return seed, nats_nkeys.from_seed(seed).public_key.decode()


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def make_jwt(issuer_seed: str, subject: str, nats_claims: dict, name: str) -> str:
    issuer = nats_nkeys.from_seed(issuer_seed)
    claims = {
        "iat": int(time.time()),
        "iss": issuer.public_key.decode(),
        "name": name,
        "sub": subject,
        "nats": nats_claims,
    }
    claims["jti"] = base64.b32encode(
        hashlib.sha256(json.dumps(claims, sort_keys=True).encode()).digest()
    ).rstrip(b"=").decode()
    header = _b64url(json.dumps({"typ": "JWT", "alg": "ed25519-nkey"}).encode())
    body = _b64url(json.dumps(claims).encode())
    signing_input = f"{header}.{body}".encode()
    raw_seed = nats_nkeys.decode_seed(issuer_seed)[1]
    signature = SigningKey.from_string(raw_seed, curve=Ed25519).sign(signing_input)
    return f"{header}.{body}.{_b64url(signature)}"


def operator_setup() -> "tuple[str, str]":
    """Returns (nats-server config for operator mode, user .creds content)."""
    op_seed, op_pub = new_nkey(nats_nkeys.PREFIX_BYTE_OPERATOR)
    acc_seed, acc_pub = new_nkey(nats_nkeys.PREFIX_BYTE_ACCOUNT)
    sys_seed, sys_pub = new_nkey(nats_nkeys.PREFIX_BYTE_ACCOUNT)
    user_seed, user_pub = new_nkey(nats_nkeys.PREFIX_BYTE_USER)
    unlimited = {"subs": -1, "data": -1, "payload": -1, "imports": -1,
                 "exports": -1, "wildcards": True, "conn": -1, "leaf": -1}
    op_jwt = make_jwt(op_seed, op_pub, {"type": "operator", "version": 2,
                                        "system_account": sys_pub}, "op")
    acc_jwt = make_jwt(op_seed, acc_pub, {"type": "account", "version": 2,
                                          "limits": unlimited}, "acc")
    sys_jwt = make_jwt(op_seed, sys_pub, {"type": "account", "version": 2,
                                          "limits": unlimited}, "sys")
    user_jwt = make_jwt(acc_seed, user_pub, {"type": "user", "version": 2, "pub": {},
                                             "sub": {}, "subs": -1, "data": -1,
                                             "payload": -1}, "user")
    config = (
        f"operator: {op_jwt}\n"
        f"system_account: {sys_pub}\n"
        "resolver: MEMORY\n"
        f"resolver_preload: {{\n  {acc_pub}: {acc_jwt}\n  {sys_pub}: {sys_jwt}\n}}\n"
    )
    creds = (
        "-----BEGIN NATS USER JWT-----\n"
        f"{user_jwt}\n"
        "------END NATS USER JWT------\n\n"
        "************************* IMPORTANT *************************\n"
        "NKEY Seed printed below can be used to sign and prove identity.\n\n"
        "-----BEGIN USER NKEY SEED-----\n"
        f"{user_seed}\n"
        "------END USER NKEY SEED------\n"
    )
    return config, creds


class FakeConnection:
    """Stands in for nats_connection.Connection around a fake nats-py client."""

    def __init__(self, nc, servers=("nats://nats.example:4222",)):
        self.nc = nc
        self.servers = list(servers)
        self.closed = asyncio.Event()
        self.fatal = asyncio.Event()

    async def confirm(self):
        pass

    def raise_if_failed(self):
        pass

    def close_reason(self):
        return "the server closed the connection"

    async def close(self):
        await self.nc.close()
        self.closed.set()


def patch_connect(monkeypatch, module, client):
    """Make `module`'s nats_connection.connect() return `client` wrapped as a Connection."""

    async def connect(account, name=None):
        return FakeConnection(client)

    monkeypatch.setattr(module.nats_connection, "connect", connect)
