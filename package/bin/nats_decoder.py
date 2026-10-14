#!/usr/bin/env python

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
NATS Payload Decoders

Turns binary NATS message payloads into event text (normally JSON) with an
executable the admin places in $SPLUNK_HOME/etc/apps/nats/local/decoders/, which
Splunk keeps across add-on upgrades. The add-on needs no decoding libraries: the
executable brings its own.

Each input, and each natssubscribe search, keeps one decoder process running and
exchanges one line per message with it:

- The add-on writes a JSON object with "subject", "headers" (an object) and
  "payload" (base64) to the decoder's stdin, followed by a newline.
- The decoder writes the event (normally JSON) to stdout on one line, or an empty
  line when it cannot decode the payload.

The decoder is restarted when it exits or does not answer within the timeout, and
it should exit when its stdin closes. A payload that fails to decode is never
dropped: it is kept as base64 and the error is returned to the caller to record.
"""

import asyncio
import base64
import json
import os
import re
import shlex
import time
from typing import Callable, Dict, Mapping, NamedTuple, Optional

DECODER_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "local", "decoders"
)
DEFAULT_SOURCETYPE = "nats:json"
# Seconds the decoder may take to answer one message
TIMEOUT = 10.0
# Minimum seconds between decoder starts, so a decoder that keeps crashing is not
# restarted for every message
RESTART_DELAY = 1.0
# Longest line the decoder may write
MAX_LINE = 16 * 1024 * 1024
# Splunk sets these for its own Python, OpenSSL and libraries. A decoder brings its
# own runtime, which they can break, so they are removed from its environment.
SPLUNK_RUNTIME_VARIABLES = (
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "LDAPCONF",
    "OPENSSL_CONF",
    "OPENSSL_MODULES",
    "SSL_CERT_FILE",
    "PYTHONHOME",
    "PYTHONPATH",
    "PYTHONHTTPSVERIFY",
)

_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")


class DecoderConfigError(Exception):
    """The decoder setting is invalid, so no decoder can run."""


class DecodeError(Exception):
    """The decoder did not turn a payload into an event."""


class Rendered(NamedTuple):
    """A message payload rendered for Splunk's _raw."""

    raw: str
    decoded: bool
    error: Optional[str]  # why decoding failed, None on success or without a decoder


def raw_payload(payload: bytes) -> str:
    """Undecoded payload for _raw: UTF-8 text as-is, anything else as base64."""
    if not payload:
        return ""
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return base64.b64encode(payload).decode("ascii")


async def render(
    decoder: Optional["Decoder"],
    payload: bytes,
    subject: str,
    headers: Optional[Mapping[str, str]] = None,
) -> Rendered:
    """
    Render a payload for _raw, decoding it when a decoder is set. A failed decode
    falls back to the base64 payload and reports the error.
    """
    if decoder is None:
        return Rendered(raw_payload(payload), False, None)
    try:
        event = await decoder.decode(payload, subject, dict(headers or {}))
        return Rendered(event, True, None)
    except Exception as e:
        error = str(e) if isinstance(e, DecodeError) else f"{type(e).__name__}: {e}"
        return Rendered(base64.b64encode(payload).decode("ascii"), False, error)


class DecodeErrorLog:
    """
    Records decode failures without flooding the log: the first failure is logged
    at once, later ones at most once per interval with the number of failures since.
    """

    def __init__(
        self,
        log: Callable[..., None],
        decoder_name: str,
        interval: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._log = log
        self._decoder_name = decoder_name
        self._interval = interval
        self._clock = clock
        self._last: Optional[float] = None
        self._count = 0

    def record(self, subject: str, error: str) -> None:
        self._count += 1
        now = self._clock()
        if self._last is not None and now - self._last < self._interval:
            return
        self._log(
            "Decoder '%s' failed to decode %d message(s); the payload is kept as "
            "base64. Last failure on subject '%s': %s",
            self._decoder_name,
            self._count,
            subject,
            error,
        )
        self._last = now
        self._count = 0


def decoder_environment() -> Dict[str, str]:
    """The environment for a decoder: Splunk's, without its runtime variables."""
    return {k: v for k, v in os.environ.items() if k not in SPLUNK_RUNTIME_VARIABLES}


async def _kill(process: asyncio.subprocess.Process) -> None:
    try:
        process.kill()
    except ProcessLookupError:
        pass
    await process.wait()


class Decoder:
    """A decoder executable kept running to decode one message after another."""

    def __init__(
        self,
        name: str,
        args: str = "",
        decoder_dir: Optional[str] = None,
        timeout: float = TIMEOUT,
        restart_delay: float = RESTART_DELAY,
    ) -> None:
        decoder_dir = decoder_dir or DECODER_DIR
        name = (name or "").strip()
        # Only a file name, so nothing outside the decoder directory can be run
        if not _NAME.match(name):
            raise DecoderConfigError(
                f"Invalid decoder '{name}': use the file name of an executable in "
                f"{decoder_dir}"
            )
        try:
            self.args = shlex.split(args or "")
        except ValueError as e:
            raise DecoderConfigError(f"Invalid decoder arguments '{args}': {e}")
        self.name = name
        self.path = os.path.join(decoder_dir, name)
        self.timeout = timeout
        self._restart_delay = restart_delay
        self._process: Optional[asyncio.subprocess.Process] = None
        self._stderr_reader: Optional["asyncio.Future[None]"] = None
        self._stderr = ""
        self._not_before = 0.0
        # Created on first use, inside the event loop that runs the decoder
        self._lock: Optional[asyncio.Lock] = None

    def check(self) -> None:
        """Raise DecodeError unless the decoder exists and is executable."""
        if not os.path.isfile(self.path):
            raise DecodeError(f"Decoder not found: {self.path}")
        if not os.access(self.path, os.X_OK):
            raise DecodeError(f"Decoder is not executable: {self.path}")

    async def decode(self, payload: bytes, subject: str, headers: Mapping[str, str]) -> str:
        """Send one message to the decoder and return its event."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            process = await self._running()
            assert process.stdin is not None and process.stdout is not None
            request = json.dumps(
                {
                    "subject": subject,
                    "headers": headers,
                    "payload": base64.b64encode(payload).decode("ascii"),
                },
                separators=(",", ":"),
            )
            try:
                process.stdin.write(request.encode("utf-8") + b"\n")
                await asyncio.wait_for(process.stdin.drain(), self.timeout)
                line = await asyncio.wait_for(process.stdout.readline(), self.timeout)
            except asyncio.TimeoutError:
                await self._stop(kill=True)
                raise DecodeError(
                    f"Decoder did not answer within {self.timeout:g}s and was stopped"
                )
            except ConnectionError:
                line = b""  # its stdin is closed: the decoder exited
            except ValueError:
                await self._stop(kill=True)
                raise DecodeError(
                    f"Decoder wrote a line longer than {MAX_LINE} bytes and was stopped"
                )
            if not line.endswith(b"\n"):
                status = await self._stop()
                raise DecodeError(f"Decoder exited with status {status}{self._detail()}")
            try:
                event = line.decode("utf-8").strip()
            except UnicodeDecodeError:
                raise DecodeError("Decoder wrote a line that is not valid UTF-8")
            if not event:
                await asyncio.sleep(0)  # let the stderr reader catch up
                raise DecodeError(f"Decoder returned an empty line{self._detail()}")
            return event

    async def close(self) -> None:
        """Stop the decoder process."""
        await self._stop()

    def _detail(self) -> str:
        return f": {self._stderr}" if self._stderr else ""

    async def _running(self) -> asyncio.subprocess.Process:
        if self._process is not None and self._process.returncode is None:
            return self._process
        await self._stop()
        now = time.monotonic()
        if now < self._not_before:
            raise DecodeError(
                f"Decoder stopped and is restarted at most every {self._restart_delay:g}s"
                + self._detail()
            )
        self._not_before = now + self._restart_delay
        self.check()
        self._stderr = ""
        try:
            self._process = await asyncio.create_subprocess_exec(
                self.path,
                *self.args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=decoder_environment(),
                cwd=os.path.dirname(self.path),
                limit=MAX_LINE,
            )
        except OSError as e:
            raise DecodeError(f"Cannot start decoder {self.path}: {e}")
        self._stderr_reader = asyncio.ensure_future(self._read_stderr(self._process))
        return self._process

    async def _read_stderr(self, process: asyncio.subprocess.Process) -> None:
        """Keep the decoder's latest stderr line to explain its failures."""
        assert process.stderr is not None
        try:
            async for line in process.stderr:
                text = line.decode("utf-8", "replace").strip()
                if text:
                    self._stderr = text[:500]
        except ValueError:
            pass  # a line beyond the stream limit: stop following stderr

    async def _stop(self, kill: bool = False) -> Optional[int]:
        """Stop the decoder process by closing its stdin, or kill it."""
        process, self._process = self._process, None
        if process is None:
            return None
        if process.returncode is None:
            if kill:
                await _kill(process)
            else:
                assert process.stdin is not None
                process.stdin.close()
                try:
                    await asyncio.wait_for(process.wait(), 5)
                except asyncio.TimeoutError:
                    await _kill(process)
        if self._stderr_reader is not None:
            try:
                await asyncio.wait_for(self._stderr_reader, 1)
            except asyncio.TimeoutError:
                pass
            self._stderr_reader = None
        return process.returncode


class BrokenDecoder(Decoder):
    """Stands in for an invalid decoder setting, so every message keeps its payload."""

    def __init__(self, name: str, error: str) -> None:
        self.name = name
        self.error = error

    async def decode(self, payload: bytes, subject: str, headers: Mapping[str, str]) -> str:
        raise DecodeError(self.error)

    async def close(self) -> None:
        pass
