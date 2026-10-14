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

from nats.aio.msg import Msg
from nats.errors import Error as NatsError
from solnlib import conf_manager

import nats

# Same sourcetype as the nats_subscribe input default, so input events and
# command results share one props.conf stanza.
SOURCETYPE = "nats:message"

# Defaults for the count= and duration= options.
DEFAULT_COUNT = 100
DEFAULT_DURATION = 10

# Errors that are about the user's input or environment, not a bug in this
# command. They are reported as a single clean message; anything else is left
# to propagate so splunklib names the file and line where it really happened.
USER_ERRORS = (ValueError, OSError, asyncio.TimeoutError, NatsError)


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
    logger = getattr(command_instance, "logger", None)
    try:
        subject: Optional[str] = getattr(command_instance, "subject", None)
        account: Optional[str] = getattr(command_instance, "account", None)
        count = _int_option(command_instance, "count", DEFAULT_COUNT, minimum=0)
        duration = _int_option(
            command_instance, "duration", DEFAULT_DURATION, minimum=1
        )

        if not subject:
            raise ValueError("Subject parameter is required")

        if not account:
            raise ValueError("Account parameter is required")

        if logger:
            logger.info(
                "NATS subscribe command starting: subject='%s', account='%s', "
                "count=%s, duration=%s",
                subject,
                account,
                count,
                duration,
            )

        account_config = _get_account_config(command_instance, account)
        if not account_config:
            raise ValueError(f"Account configuration '{account}' not found or invalid")

        events = asyncio.run(
            _collect_messages(subject, account_config, count, duration, logger)
        )
    except USER_ERRORS as e:
        if logger:
            logger.error("natssubscribe failed: %s", e)
        # Writes one ERROR message and exits; no generic re-raise location.
        command_instance.error_exit(e, str(e))
        return

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


def message_to_event(msg: Msg, host: str) -> Dict[str, Any]:
    """
    Convert a NATS message to a Splunk result.

    The fields match what the nats_subscribe input indexes (sourcetype, host,
    source), and every NATS header is added as a `headers.<name>` field.
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
) -> List[Dict[str, Any]]:
    """
    Async coroutine to connect to NATS and collect messages from a subject

    Args:
        subject: NATS subject to subscribe to
        account_config: Account configuration dictionary
        count: Stop after this many messages (0 = no limit)
        duration: Stop after this many seconds
        logger: Optional logger instance

    Returns:
        List of event dictionaries for Splunk
    """
    nc = None
    events: List[Dict[str, Any]] = []
    enough = asyncio.Event()

    try:
        connect_options: Dict[str, Any] = {}

        servers = account_config.get("servers", "nats://localhost:4222")
        server_list: List[str] = [server.strip() for server in servers.split(",")]

        if account_config.get("username") and account_config.get("password"):
            connect_options["user"] = account_config["username"]
            connect_options["password"] = account_config["password"]

        nc = await nats.connect(servers=server_list, **connect_options)
        host = connected_host(nc, server_list)

        async def message_handler(msg: Msg) -> None:
            if enough.is_set():
                return
            events.append(message_to_event(msg, host))
            if count and len(events) >= count:
                enough.set()

        sub = await nc.subscribe(subject, cb=message_handler)
        try:
            await asyncio.wait_for(enough.wait(), timeout=duration)
        except asyncio.TimeoutError:
            pass
        await sub.unsubscribe()

    finally:
        if nc:
            try:
                await nc.close()
            except Exception as e:
                if logger:
                    logger.warning("Failed to close NATS connection: %s", e)

    return events


def _get_account_config(
    command_instance: Any, account_name: str
) -> Optional[Dict[str, Any]]:
    """
    Retrieve account configuration from Splunk configuration using UCC patterns.

    Args:
        command_instance: The command instance
        account_name: Name of the account configuration

    Returns:
        Dictionary containing account configuration or None if not found
    """
    try:
        # Get session key from command instance
        session_key = getattr(command_instance, "session_key", None)
        if not session_key:
            # Try to get from service
            service = getattr(command_instance, "service", None)
            if service:
                session_key = service.token

        if not session_key:
            raise RuntimeError("Unable to get session key for configuration access")

        # Use UCC configuration manager to get account details
        cfm = conf_manager.ConfManager(
            session_key,
            "nats",
            realm="__REST_CREDENTIAL__#nats#configs/conf-nats_account",
        )

        account_conf_file = cfm.get_conf("nats_account")
        account_config = account_conf_file.get(account_name)

        if not account_config:
            return None

        return dict(account_config)

    except Exception as e:
        raise ValueError(
            f"Failed to get account configuration '{account_name}': {str(e)}"
        )
