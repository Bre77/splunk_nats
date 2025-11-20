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
from typing import Any

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

# These imports are not needed for UCC custom search commands
# UCC generates the command wrapper automatically
import base64

from nats.aio.msg import Msg
from solnlib import conf_manager

import nats


def generate(command_instance: Any) -> Generator[dict[str, Any], None, None]:
    """
    Generate function for NATS subscribe command.
    This function will be called by the UCC-generated wrapper.

    Args:
        command_instance: The command instance with options set

    Yields:
        Dict: Event dictionaries for Splunk
    """
    try:
        # Get command arguments
        subject: str | None = getattr(command_instance, "subject", None)
        account: str | None = getattr(command_instance, "account", None)

        # Validate required parameters
        if not subject:
            raise ValueError("Subject parameter is required")

        if not account:
            raise ValueError("Account parameter is required")

        # Get logger if available
        logger = getattr(command_instance, "logger", None)
        if logger:
            logger.info(
                "NATS subscribe command starting: subject='%s', account='%s'",
                subject,
                account,
            )

        # Get account configuration
        account_config = _get_account_config(command_instance, account)
        if not account_config:
            raise ValueError(f"Account configuration '{account}' not found or invalid")

        # Get server for host field - use first server from servers list
        servers = account_config.get("servers", "nats://localhost:4222")
        server_host = servers.split(",")[0].strip()

        # Run the async function to collect messages
        try:
            events = asyncio.run(
                _collect_messages(subject, account_config, server_host, logger)
            )

            # Yield each event
            for event in events:
                yield event

        except Exception as e:
            if logger:
                logger.error("Error in NATS subscription: %s", str(e))
            raise RuntimeError(f"Subscription error: {str(e)}")

    except Exception as e:
        raise RuntimeError(str(e))


async def _collect_messages(
    subject: str,
    account_config: dict[str, Any],
    server_host: str,
    logger: Any | None = None,
) -> list[dict[str, Any]]:
    """
    Async coroutine to connect to NATS and collect messages from topic

    Args:
        subject: NATS subject to subscribe to
        account_config: Account configuration dictionary
        server_host: Server host for event host field
        logger: Optional logger instance

    Returns:
        List of event dictionaries for Splunk
    """
    nc = None
    events: list[dict[str, Any]] = []

    try:
        # Connection options
        connect_options: dict[str, Any] = {}

        # Get server URLs (comma-separated)
        servers = account_config.get("servers", "nats://localhost:4222")
        server_list: list[str] = [server.strip() for server in servers.split(",")]

        if account_config.get("username") and account_config.get("password"):
            connect_options["user"] = account_config["username"]
            connect_options["password"] = account_config["password"]

        # Connect to NATS
        nc = await nats.connect(servers=server_list, **connect_options)

        # Message handler that collects events
        async def message_handler(msg: Msg) -> None:
            try:
                # Handle the message data - decode to string for _raw
                if msg.data:
                    try:
                        data_str = msg.data.decode("utf-8")
                    except UnicodeDecodeError:
                        data_str = base64.b64encode(msg.data).decode("ascii")
                else:
                    data_str = ""

                # Create the Splunk event
                event: dict[str, Any] = {
                    "_time": time.time(),
                    "_raw": data_str,
                    "source": msg.subject,
                    "sourcetype": "nats:json",
                    "host": server_host,
                    "subject": msg.subject,
                }

                events.append(event)
            except Exception as e:
                if logger:
                    logger.error("Failed to process message: %s", str(e))

        # Subscribe to the subject
        sub = await nc.subscribe(subject, cb=message_handler)

        # Wait for messages with timeout
        timeout_duration = 30  # 30 seconds
        start_time = time.time()

        while time.time() - start_time < timeout_duration:
            await asyncio.sleep(0.1)  # Small sleep to allow message processing

            # If we have collected some events, we can break early
            # This prevents the command from waiting the full timeout if messages are received
            if len(events) > 0:
                # Wait a bit more to collect additional messages
                additional_wait = min(5, timeout_duration - (time.time() - start_time))
                if additional_wait > 0:
                    await asyncio.sleep(additional_wait)
                break

        # Unsubscribe
        if sub:
            await sub.unsubscribe()

    except Exception as e:
        if logger:
            logger.error("NATS subscription error: %s", str(e))
        raise
    finally:
        if nc:
            try:
                await nc.close()
            except Exception:
                pass

    return events


def _get_account_config(
    command_instance: Any, account_name: str
) -> dict[str, Any] | None:
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
            raise Exception("Unable to get session key for configuration access")

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
        raise Exception(
            f"Failed to get account configuration '{account_name}': {str(e)}"
        )
