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

import asyncio
import os
import sys
import time
from collections.abc import Generator
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

# These imports are not needed for UCC custom search commands
# UCC generates the command wrapper automatically
import base64

import nats_connection
import nats_logging
from nats.aio.msg import Msg
from nats.errors import Error as NatsError
from nats_decoder import (
    DEFAULT_SOURCETYPE as DECODED_SOURCETYPE,
    DecodeError,
    Decoder,
    DecoderConfigError,
    render,
)

# Same sourcetype as the nats_subscribe input default, so input events and
# command results share one props.conf stanza.
SOURCETYPE = "nats:message"

# Defaults for the count= and duration= options.
DEFAULT_COUNT = 100
DEFAULT_DURATION = 10

# Errors that are about the user's input or environment, not a bug in this
# command. They are reported as a single clean message; anything else is left
# to propagate so splunklib names the file and line where it really happened.
USER_ERRORS = (
    ValueError,
    OSError,
    asyncio.TimeoutError,
    NatsError,
    nats_connection.NatsConnectError,
)


def generate(command_instance: Any) -> Generator[Dict[str, Any], None, None]:
    """
    Generate function for NATS subscribe command.
    This function will be called by the UCC-generated wrapper.

    Listens on the subject until `count` messages arrived (0 = no limit) or
    `duration` seconds passed, whichever comes first.

    Args:
        command_instance: The command instance with options set

    Yields:
        Dict: Event dictionaries for Splunk
    """
    logger = nats_logging.setup_command(
        "natssubscribe", _session_key(command_instance), _splunklib_logger(command_instance)
    )
    try:
        subject: Optional[str] = getattr(command_instance, "subject", None)
        account: Optional[str] = getattr(command_instance, "account", None)
        count = _int_option(command_instance, "count", DEFAULT_COUNT, minimum=0)
        duration = _int_option(
            command_instance, "duration", DEFAULT_DURATION, minimum=1
        )
        decoder = _decoder_option(command_instance)

        if not subject:
            raise ValueError("Subject parameter is required")

        if not account:
            raise ValueError("Account parameter is required")

        logger.info(
            "NATS subscribe command starting: subject='%s', account='%s', "
            "count=%s, duration=%s",
            subject,
            account,
            count,
            duration,
        )

        account_config = _get_account_config(command_instance, account)

        events = asyncio.run(
            _collect_messages(subject, account_config, count, duration, logger, decoder)
        )
    except USER_ERRORS as e:
        nats_connection.log_failure(logger, e, "natssubscribe failed")
        # Writes one ERROR message and exits; no generic re-raise location.
        command_instance.error_exit(e, str(e))
        return
    except Exception as e:
        nats_connection.log_failure(logger, e, "natssubscribe failed")
        raise

    yield from events


def _int_option(command_instance: Any, name: str, default: int, minimum: int) -> int:
    """Read an integer option, applying the default and a lower bound."""
    value = getattr(command_instance, name, None)
    if value is None or value == "":
        return default
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be an integer, got '{value}'")
    if number < minimum:
        raise ValueError(f"{name} must be at least {minimum}, got {number}")
    return number


def _decoder_option(command_instance: Any) -> Optional[Decoder]:
    """Create the decoder= option's decoder, checking it before any message arrives."""
    name = getattr(command_instance, "decoder", None)
    if not name:
        return None
    try:
        decoder = Decoder(name, getattr(command_instance, "decoder_args", None) or "")
        decoder.check()
    except (DecoderConfigError, DecodeError) as e:
        raise ValueError(str(e))
    return decoder


def message_to_event(msg: Msg, host: str) -> Dict[str, Any]:
    """
    Convert a NATS message to a Splunk result.

    The fields match what the nats_subscribe input indexes (sourcetype, host,
    and the subject as nats_subject), and every NATS header is added as a
    `headers.<name>` field.
    """
    if msg.data:
        try:
            data_str = msg.data.decode("utf-8")
        except UnicodeDecodeError:
            data_str = base64.b64encode(msg.data).decode("ascii")
    else:
        data_str = ""

    event: Dict[str, Any] = {
        "_time": time.time(),
        "_raw": data_str,
        "source": msg.subject,
        "sourcetype": SOURCETYPE,
        "host": host,
        "subject": msg.subject,
        "nats_subject": msg.subject,
    }
    for name, value in (msg.headers or {}).items():
        event[f"headers.{name}"] = value
    return event


def connected_host(nc: Any, servers: List[str]) -> str:
    """
    The host field value: host:port of the server we are connected to, the
    same form the nats_subscribe input uses.
    """
    if nc.connected_url:
        return nc.connected_url.netloc
    return urlparse(servers[0]).netloc or servers[0]


async def _collect_messages(
    subject: str,
    account_config: Dict[str, Any],
    count: int,
    duration: int,
    logger: Optional[Any] = None,
    decoder: Optional[Decoder] = None,
) -> List[Dict[str, Any]]:
    """
    Async coroutine to connect to NATS and collect messages from a subject

    Args:
        subject: NATS subject to subscribe to
        account_config: Account configuration dictionary
        count: Stop after this many messages (0 = no limit)
        duration: Stop after this many seconds
        logger: Optional logger instance
        decoder: Optional payload decoder, run once collecting has ended

    Returns:
        List of event dictionaries for Splunk
    """
    connection = None
    events: List[Dict[str, Any]] = []
    messages: List[Msg] = []
    enough = asyncio.Event()

    try:
        # One bounded connection attempt that fails with the real cause
        connection = await nats_connection.connect(
            account_config, name="splunk-nats natssubscribe"
        )
        nc = connection.nc
        host = connected_host(nc, connection.servers)

        async def message_handler(msg: Msg) -> None:
            if enough.is_set():
                return
            events.append(message_to_event(msg, host))
            messages.append(msg)
            if count and len(events) >= count:
                enough.set()

        # Wait for the server to confirm the subscription, so a permissions
        # violation on the subject fails the search immediately.
        sub = await nc.subscribe(subject, cb=message_handler)
        await connection.confirm()

        # Stop early on enough messages, a late permissions violation or a
        # dropped connection.
        waiters = [
            asyncio.ensure_future(event.wait())
            for event in (enough, connection.fatal, connection.closed)
        ]
        await asyncio.wait(
            waiters, timeout=duration, return_when=asyncio.FIRST_COMPLETED
        )
        for waiter in waiters:
            waiter.cancel()
        connection.raise_if_failed()
        if connection.closed.is_set() and not enough.is_set():
            raise nats_connection.NatsConnectError(
                f"Connection to NATS closed: {connection.close_reason()}"
            )
        await sub.unsubscribe()

        # Decode once collecting has ended, so a slow decoder does not hold up
        # receiving. A payload that fails to decode keeps its base64 _raw.
        if decoder is not None:
            for event, msg in zip(events, messages):
                rendered = await render(decoder, msg.data, msg.subject, msg.headers)
                event["_raw"] = rendered.raw
                if rendered.decoded:
                    event["sourcetype"] = DECODED_SOURCETYPE
                else:
                    event["decode_error"] = rendered.error

    finally:
        if connection:
            await connection.close()
        if decoder is not None:
            await decoder.close()

    return events


def _get_account_config(command_instance, account_name):
    """
    Retrieve account configuration (with decrypted secrets) for the command.

    Args:
        command_instance: The command instance
        account_name: Name of the account configuration

    Returns:
        Dictionary containing the account configuration

    Raises:
        NatsConnectError: If the account does not exist or cannot be read
    """
    session_key = _session_key(command_instance)
    if not session_key:
        raise RuntimeError("Unable to get session key for configuration access")

    return nats_connection.load_account(session_key, str(account_name))


def _splunklib_logger(command_instance) -> Optional[str]:
    """The name of the command's own splunklib logger, if it has one."""
    command_logger = getattr(command_instance, "logger", None)
    return getattr(command_logger, "name", None)


def _session_key(command_instance) -> Optional[str]:
    """The search's session key, for reading the add-on's configuration."""
    session_key = getattr(command_instance, "session_key", None)
    if not session_key:
        service = getattr(command_instance, "service", None)
        if service:
            session_key = service.token
    return session_key
