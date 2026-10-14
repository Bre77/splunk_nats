# Copyright 2025 Brett Adams
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Shared NATS connection setup for the add-on.

Every entry point (the `nats_subscribe` and `nats_kv` inputs, the `natssubscribe`
and `natskv` search commands, and the Account REST handler) connects through this
module so authentication, TLS, timeouts and error reporting behave the same way.

- `connect()` makes one bounded, fail-fast connection attempt and raises
  `NatsConnectError` carrying the real cause (authorization violation, connection
  refused, TLS error, ...) instead of nats-py's generic "no servers available".
- `run_forever()` keeps an input's session running on a live connection: it
  reconnects forever with capped exponential backoff and jitter, reloads the
  Account before every attempt (so a corrected Account is picked up without a
  Splunk restart), and logs each disconnect, reconnect and close once.
"""

import asyncio
import logging
import os
import random
import re
import shutil
import signal
import socket
import ssl
import sys
import tempfile
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

import nats_nkeys
from nats.aio.client import Client, RawCredentials
from nats import errors as nats_errors

nats_nkeys.install()

logger = logging.getLogger(__name__)

DEFAULT_SERVERS = "nats://localhost:4222"
DEFAULT_CONNECT_TIMEOUT = 10
MAX_CONNECT_TIMEOUT = 300

# Input reconnect policy: capped exponential backoff with full jitter.
RECONNECT_BACKOFF_BASE = 1.0
RECONNECT_BACKOFF_CAP = 20.0
# A connection must stay up this long before the backoff resets, so a server that
# accepts and immediately drops connections cannot cause a tight reconnect loop.
RECONNECT_STABLE_AFTER = 30.0

# Keepalive for long-running inputs: a half-open connection is detected after
# about PING_INTERVAL * (MAX_OUTSTANDING_PINGS + 1) seconds.
PING_INTERVAL = 20
MAX_OUTSTANDING_PINGS = 2

CLOSE_TIMEOUT = 5
# Closing after a failed connection attempt only releases the socket.
FAILED_CLOSE_TIMEOUT = 1

ENCRYPTED_FIELDS = ("password", "token", "nkey_seed", "credentials", "tls_key")
_TRUE_VALUES = ("1", "true", "t", "yes", "y", "on")


class NatsConnectError(Exception):
    """A NATS connection or session failure, with a message stating the real cause."""


class InputStopped(Exception):
    """Raised by a session when Splunk no longer reads the input's events, so the
    input must stop rather than reconnect."""


def _is_true(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in _TRUE_VALUES


def server_list(account: Dict[str, Any]) -> List[str]:
    servers = account.get("servers") or DEFAULT_SERVERS
    return [server.strip() for server in servers.split(",") if server.strip()]


def connect_timeout(account: Dict[str, Any]) -> float:
    value = account.get("connect_timeout")
    if value in (None, ""):
        return float(DEFAULT_CONNECT_TIMEOUT)
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        raise NatsConnectError(
            f"Connect timeout must be a number of seconds, got '{value}'"
        )
    if not 0 < timeout <= MAX_CONNECT_TIMEOUT:
        raise NatsConnectError(
            f"Connect timeout must be between 1 and {MAX_CONNECT_TIMEOUT} seconds"
        )
    return timeout


_PEM_BLOCK = re.compile(r"-----BEGIN ([A-Z0-9 ]+)-----(.*?)-----END \1-----", re.DOTALL)
_CREDS_JWT = re.compile(r"-{3,}BEGIN NATS USER JWT-{3,}\s*(\S+)\s*-{3,}END NATS USER JWT-{3,}")
_CREDS_SEED = re.compile(r"-{3,}BEGIN USER NKEY SEED-{3,}\s*(\S+)\s*-{3,}END USER NKEY SEED-{3,}")


def normalize_pem(text: str, what: str) -> str:
    """
    Re-flow PEM blocks into canonical form, so certificates and keys survive
    whitespace changes from copy and paste or from storage in .conf files.
    """
    blocks = []
    for label, body in _PEM_BLOCK.findall(text or ""):
        data = "".join(body.split())
        lines = [data[i : i + 64] for i in range(0, len(data), 64)]
        blocks.append("\n".join([f"-----BEGIN {label}-----", *lines, f"-----END {label}-----"]))
    if not blocks:
        raise NatsConnectError(f"{what} must be PEM text (-----BEGIN ...-----)")
    return "\n".join(blocks) + "\n"


def normalize_credentials(text: str) -> str:
    """Rebuild a NATS .creds file from its user JWT and NKey seed, whatever the whitespace."""
    jwt = _CREDS_JWT.search(text)
    seed = _CREDS_SEED.search(text)
    if not jwt or not seed:
        raise NatsConnectError(
            "Credentials must be the full content of a NATS .creds file "
            "(user JWT and NKey seed)"
        )
    try:
        nats_nkeys.decode_seed(seed.group(1))
    except nats_nkeys.ErrInvalidSeed:
        raise NatsConnectError("Credentials file does not contain a valid NKey seed")
    return (
        "-----BEGIN NATS USER JWT-----\n"
        f"{jwt.group(1)}\n"
        "------END NATS USER JWT------\n\n"
        "-----BEGIN USER NKEY SEED-----\n"
        f"{seed.group(1)}\n"
        "------END USER NKEY SEED------\n"
    )


def build_tls_context(account: Dict[str, Any]) -> Optional[ssl.SSLContext]:
    """
    Build the TLS context for an Account, or None when the Account sets no TLS options
    (a `tls://` server URL then uses nats-py's default verifying context).
    """
    ca = (account.get("tls_ca") or "").strip()
    cert = (account.get("tls_cert") or "").strip()
    key = (account.get("tls_key") or "").strip()
    verify = _is_true(account.get("tls_verify"), default=True)
    handshake_first = _is_true(account.get("tls_handshake_first"))

    if not (ca or cert or key or handshake_first or not verify):
        return None

    if bool(cert) != bool(key):
        raise NatsConnectError(
            "TLS client certificate and client key must be provided together"
        )

    if ca:
        ca = normalize_pem(ca, "TLS CA certificates")
    if cert:
        cert = normalize_pem(cert, "TLS client certificate")
        key = normalize_pem(key, "TLS client key")

    try:
        context = ssl.create_default_context(cadata=ca or None)
    except ssl.SSLError as e:
        raise NatsConnectError(f"Invalid TLS CA bundle: {e}")

    if not verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    if cert:
        # The ssl module only loads certificate chains from files, so stage the
        # PEM text in a private temporary directory just long enough to load it.
        directory = tempfile.mkdtemp(prefix="nats-tls-")
        try:
            cert_path = os.path.join(directory, "client.crt")
            key_path = os.path.join(directory, "client.key")
            for path, content in ((cert_path, cert), (key_path, key)):
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "w") as f:
                    f.write(content)
            context.load_cert_chain(cert_path, key_path)
        except ssl.SSLError as e:
            raise NatsConnectError(f"Invalid TLS client certificate or key: {e}")
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    return context


def build_connect_options(account: Dict[str, Any]) -> Dict[str, Any]:
    """Translate an Account into nats-py connect() options (servers, auth, TLS, timeout)."""
    options: Dict[str, Any] = {
        "servers": server_list(account),
        "connect_timeout": connect_timeout(account),
    }

    username = account.get("username")
    password = account.get("password")
    if username and password:
        options["user"] = username
        options["password"] = password

    token = (account.get("token") or "").strip()
    if token:
        options["token"] = token

    credentials = (account.get("credentials") or "").strip()
    nkey_seed = (account.get("nkey_seed") or "").strip()
    if credentials and nkey_seed:
        raise NatsConnectError(
            "Set either a credentials file or an NKey seed, not both"
        )
    if credentials:
        options["user_credentials"] = RawCredentials(normalize_credentials(credentials))
    if nkey_seed:
        try:
            nats_nkeys.decode_seed(nkey_seed)
        except nats_nkeys.ErrInvalidSeed:
            raise NatsConnectError("NKey seed is not a valid NATS NKey seed (SU...)")
        options["nkeys_seed_str"] = nkey_seed

    tls_context = build_tls_context(account)
    if tls_context is not None:
        options["tls"] = tls_context
    options["tls_handshake_first"] = _is_true(account.get("tls_handshake_first"))

    return options


def describe_error(
    error: BaseException,
    servers: Optional[List[str]] = None,
    tls: bool = False,
    setup: bool = True,
) -> str:
    """
    Turn a nats-py / socket / TLS exception into a message that states the real cause.

    Args:
        error: The exception
        servers: The servers that were tried
        tls: Whether the connection was set up with TLS
        setup: Whether the error happened while connecting, rather than on an
            established connection
    """
    where = ", ".join(servers) if servers else "the NATS server"

    if isinstance(error, NatsConnectError):
        return str(error)
    if isinstance(error, nats_errors.AuthorizationError) or (
        "authorization violation" in str(error).lower()
    ):
        return (
            f"Authorization violation from {where}: the server rejected the "
            "Account's credentials"
        )
    if isinstance(error, ssl.SSLCertVerificationError):
        return f"TLS certificate verification failed for {where}: {error.verify_message or error}"
    if isinstance(error, ssl.SSLError):
        reason = error.reason or error.strerror or str(error)
        if "CERTIFICATE_REQUIRED" in reason:
            return (
                f"TLS error connecting to {where}: the server requires a client "
                "certificate (mutual TLS); set the TLS client certificate and key"
            )
        if any(alert in reason for alert in ("UNKNOWN_CA", "BAD_CERTIFICATE", "CERTIFICATE_UNKNOWN")):
            return f"TLS error connecting to {where}: the server rejected the client certificate ({reason})"
        return f"TLS error connecting to {where}: {reason}"
    if isinstance(error, (asyncio.TimeoutError, nats_errors.TimeoutError)):
        return f"Timed out connecting to {where}"
    if isinstance(error, ConnectionRefusedError):
        return f"Connection refused by {where}"
    if isinstance(error, socket.gaierror):
        return f"Cannot resolve the host name of {where}: {error.strerror or error}"
    if isinstance(error, (BrokenPipeError, ConnectionResetError, nats_errors.UnexpectedEOF)):
        if not setup:
            return f"Connection lost: {where} closed the connection"
        if tls:
            # With TLS 1.3 a server that rejects the client certificate (or gets none)
            # closes the connection after the handshake, before the client reads why.
            return (
                f"{where} closed the TLS connection during setup: the server most likely "
                "requires a client certificate (mutual TLS) and did not get one it trusts"
            )
        return (
            f"Connection closed by {where} during setup "
            "(does the server require TLS, or is this not a NATS client port?)"
        )
    if isinstance(error, OSError):
        detail = os.strerror(error.errno) if error.errno else str(error)
        if not setup:
            return f"Connection to {where} lost: {detail}"
        return f"Cannot connect to {where}: {detail}"
    if isinstance(error, nats_errors.NoServersError):
        return f"No NATS server could be reached at {where}"

    message = str(error) or type(error).__name__
    if message.startswith("nats: "):
        message = message[len("nats: "):]
    message = message.strip("'")
    if "permissions violation" in message.lower():
        return f"Permissions violation: {message.split(':', 1)[-1].strip() or message}"
    return message


def is_permissions_violation(error: BaseException) -> bool:
    return "permissions violation" in str(error).lower()


class Connection:
    """A connected nats-py client plus the asynchronous events reported for it."""

    def __init__(self, servers: List[str]):
        self.servers = servers
        self.nc = Client()
        self.closed = asyncio.Event()
        self.fatal = asyncio.Event()
        self.errors: List[BaseException] = []
        self.fatal_error: Optional[BaseException] = None
        self.tls = False

    @property
    def url(self) -> str:
        if self.nc.connected_url is not None:
            return self.nc.connected_url.geturl()
        return ", ".join(self.servers)

    @property
    def last_error(self) -> Optional[BaseException]:
        return self.errors[-1] if self.errors else None

    async def _error_cb(self, error: BaseException) -> None:
        self.errors.append(error)
        del self.errors[:-20]
        # The server reports subscription permission violations asynchronously and
        # keeps the connection open, so surface them as a fatal session error.
        if is_permissions_violation(error):
            self.fatal_error = error
            self.fatal.set()

    async def _closed_cb(self) -> None:
        self.closed.set()

    def close_reason(self) -> str:
        """Why the connection closed, from the last error the client saw."""
        error = self.nc.last_error or self.last_error
        if error is None:
            return "the server closed the connection"
        return describe_error(error, [self.url], tls=self.tls, setup=False)

    async def confirm(self, timeout: float = DEFAULT_CONNECT_TIMEOUT) -> None:
        """
        Complete round trips with the server and raise NatsConnectError if it reported
        a permissions violation for what was just sent. The server can deliver the
        violation one round trip after the PONG for the same request, hence two; a
        later one still surfaces through `fatal`.
        """
        for _ in range(2):
            await self.nc.flush(timeout=timeout)
            self.raise_if_failed()

    def raise_if_failed(self) -> None:
        """Raise NatsConnectError if the server reported a permissions violation."""
        if self.fatal_error is not None:
            raise NatsConnectError(describe_error(self.fatal_error, self.servers))

    async def close(self, timeout: float = CLOSE_TIMEOUT) -> None:
        try:
            # Bounded: nats-py can wait forever for a transport whose TLS
            # handshake failed to finish closing.
            await asyncio.wait_for(self.nc.close(), timeout=timeout)
        except Exception:
            pass
        self.closed.set()


async def connect(
    account: Dict[str, Any],
    name: Optional[str] = None,
    ping_interval: int = PING_INTERVAL,
) -> Connection:
    """
    Connect to NATS with one bounded attempt per configured server.

    Raises:
        NatsConnectError: With the real cause if no server accepted the connection
            within the Account's connect timeout.
    """
    options = build_connect_options(account)
    timeout = options["connect_timeout"]
    connection = Connection(options["servers"])
    connection.tls = "tls" in options or any(
        server.lower().startswith("tls://") for server in options["servers"]
    )
    try:
        # nats-py never reconnects here: reconnection is owned by run_forever(),
        # which reloads the Account first. max_reconnect_attempts bounds how many
        # times nats-py cycles through the server list before giving up.
        await asyncio.wait_for(
            connection.nc.connect(
                **options,
                name=name,
                allow_reconnect=False,
                max_reconnect_attempts=1,
                reconnect_time_wait=0,
                ping_interval=ping_interval,
                max_outstanding_pings=MAX_OUTSTANDING_PINGS,
                error_cb=connection._error_cb,
                closed_cb=connection._closed_cb,
            ),
            timeout=timeout * len(options["servers"]) + 1,
        )
    except asyncio.CancelledError:
        await connection.close(FAILED_CLOSE_TIMEOUT)
        raise
    except Exception as e:
        await connection.close(FAILED_CLOSE_TIMEOUT)
        cause: BaseException = e
        if isinstance(e, (nats_errors.NoServersError, asyncio.TimeoutError)):
            cause = connection.last_error or e
        raise NatsConnectError(
            describe_error(cause, options["servers"], tls=connection.tls)
        ) from e
    return connection


async def check_connection(account: Dict[str, Any]) -> None:
    """Connect, complete a round trip with the server, and disconnect."""
    connection = await connect(account, name="splunk-nats connection test")
    try:
        await connection.confirm(timeout=connect_timeout(account))
    except NatsConnectError:
        raise
    except Exception as e:
        raise NatsConnectError(describe_error(connection.last_error or e, connection.servers))
    finally:
        await connection.close()


class Backoff:
    """Capped exponential backoff with full jitter."""

    def __init__(
        self,
        base: float = RECONNECT_BACKOFF_BASE,
        cap: float = RECONNECT_BACKOFF_CAP,
        rand: Callable[[], float] = random.random,
    ):
        self.base = base
        self.cap = cap
        self.attempt = 0
        self._rand = rand

    def next(self) -> float:
        ceiling = min(self.cap, self.base * (2 ** self.attempt))
        self.attempt = min(self.attempt + 1, 32)
        # Never retry instantly, but spread retries out across [ceiling/2, ceiling].
        return ceiling / 2 + self._rand() * ceiling / 2

    def reset(self) -> None:
        self.attempt = 0


Session = Callable[[Connection], Awaitable[None]]


async def run_forever(
    input_name: str,
    load_account: Callable[[], Dict[str, Any]],
    session: Session,
    log: logging.Logger = logger,
    stop: Optional[asyncio.Event] = None,
    backoff: Optional[Backoff] = None,
) -> None:
    """
    Keep `session` running on a live NATS connection until `stop` is set.

    `load_account` is called before every connection attempt, so changes to the
    Account (for example corrected credentials) take effect on the next retry.
    `session` sets up subscriptions or watchers; it may return once set up or keep
    running. The connection is replaced whenever it closes, `session` raises, or the
    server reports a permissions violation.
    """
    stop = stop or asyncio.Event()
    backoff = backoff or Backoff()
    failing_since: Optional[float] = None
    last_reason: Optional[str] = None
    ever_connected = False

    while not stop.is_set():
        try:
            account = load_account()
            connection = await connect(account, name=f"splunk-nats {input_name}")
        except NatsConnectError as e:
            reason = str(e)
        except Exception as e:
            reason = describe_error(e)
        else:
            if ever_connected:
                outage = time.monotonic() - failing_since if failing_since else 0.0
                log.warning(
                    "%s: reconnected to %s after %.0fs", input_name, connection.url, outage
                )
            else:
                log.info("%s: connected to %s", input_name, connection.url)
            ever_connected = True
            failing_since = None
            last_reason = None

            connected_at = time.monotonic()
            reason = await _run_session(connection, session, stop)
            if stop.is_set():
                if reason != "stopped":
                    log.info("%s: stopping: %s", input_name, reason)
                break
            if time.monotonic() - connected_at >= RECONNECT_STABLE_AFTER:
                backoff.reset()
            failing_since = time.monotonic()
            delay = backoff.next()
            log.warning(
                "%s: disconnected from %s: %s; reconnecting in %.1fs",
                input_name,
                connection.url,
                reason,
                delay,
            )
            last_reason = reason
            await _sleep_unless_stopped(stop, delay)
            continue

        if failing_since is None:
            failing_since = time.monotonic()
        delay = backoff.next()
        if reason != last_reason:
            log.error(
                "%s: cannot connect to NATS: %s; retrying in %.1fs and will keep retrying",
                input_name,
                reason,
                delay,
            )
        else:
            log.debug("%s: still cannot connect: %s; retrying in %.1fs", input_name, reason, delay)
        last_reason = reason
        await _sleep_unless_stopped(stop, delay)

    log.info("%s: stopped", input_name)


async def _run_session(connection: Connection, session: Session, stop: asyncio.Event) -> str:
    """Run one session until the connection closes; return why it ended."""
    session_task = asyncio.ensure_future(session(connection))
    waiters = [
        session_task,
        asyncio.ensure_future(connection.closed.wait()),
        asyncio.ensure_future(connection.fatal.wait()),
        asyncio.ensure_future(stop.wait()),
    ]
    reason = "stopped"
    try:
        pending = set(waiters)
        while True:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if session_task in done and not session_task.cancelled():
                error = session_task.exception()
                if isinstance(error, InputStopped):
                    stop.set()
                    reason = str(error)
                    break
                if error is not None:
                    # A session reading from a connection that just dropped fails
                    # because of it; report the drop rather than its side effect.
                    if connection.nc.is_closed or connection.closed.is_set():
                        reason = connection.close_reason()
                    else:
                        reason = describe_error(error, connection.servers)
                    break
            if connection.fatal.is_set():
                reason = describe_error(connection.fatal_error, connection.servers)
                break
            if connection.closed.is_set():
                reason = connection.close_reason()
                break
            if stop.is_set():
                break
            # Only the session finished setting up: keep the connection running.
    finally:
        for waiter in waiters:
            if not waiter.done():
                waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        await connection.close()
    return reason


async def _sleep_unless_stopped(stop: asyncio.Event, delay: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
    except asyncio.TimeoutError:
        pass


def run_input(
    input_name: str,
    load_account: Callable[[], Dict[str, Any]],
    session: Session,
    log: logging.Logger = logger,
    cleanup: Optional[Callable[[], Awaitable[None]]] = None,
) -> None:
    """
    Run an input until Splunk stops it with SIGTERM (input disabled, or Splunk
    stopping), then close the connection, await `cleanup` and exit.
    """

    async def main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, RuntimeError, ValueError):
                # Signal handlers are not available on Windows event loops
                pass
        try:
            await run_forever(input_name, load_account, session, log=log, stop=stop)
        finally:
            if cleanup is not None:
                await cleanup()

    asyncio.run(main())
    # Splunk closes the event pipe when it stops an input, so exit now instead of
    # letting splunklib write the closing </stream> tag into it.
    sys.exit(0)


def load_account(session_key: str, account_name: str) -> Dict[str, Any]:
    """
    Read an Account (with decrypted secrets) from the add-on's configuration.

    Raises:
        NatsConnectError: If the Account does not exist or cannot be read.
    """
    from solnlib import conf_manager

    try:
        cfm = conf_manager.ConfManager(
            session_key,
            "nats",
            realm="__REST_CREDENTIAL__#nats#configs/conf-nats_account",
        )
        account_config = cfm.get_conf("nats_account").get(account_name)
    except Exception as e:
        raise NatsConnectError(
            f"Failed to get account configuration '{account_name}': {str(e)}"
        )
    if not account_config:
        raise NatsConnectError(f"Account configuration '{account_name}' not found")
    return dict(account_config)
