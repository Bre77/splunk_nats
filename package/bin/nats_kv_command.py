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

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

import nats_connection
from nats.errors import Error as NatsError
from nats_kv_common import (
    entry_operation,
    entry_raw,
    entry_time,
    kv_error_message,
    read_history,
)


# Same sourcetype as the nats_kv input default, so input events and command
# results share one props.conf stanza.
SOURCETYPE = "nats:kv"

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


def generate(command_instance):
    """
    Generate function for NATS JetStream KV history command.
    This function will be called by the UCC-generated wrapper.

    Args:
        command_instance: The command instance with options set

    Yields:
        Dict: Event dictionaries for Splunk
    """
    logger = getattr(command_instance, "logger", None)
    bucket = domain = None
    try:
        # Get command arguments
        bucket = getattr(command_instance, "bucket", None)
        key = getattr(command_instance, "key", ">")
        account = getattr(command_instance, "account", None)
        domain = getattr(command_instance, "domain", None)

        # Convert parameters to strings and validate
        if bucket is not None:
            bucket = str(bucket)
        if key is not None:
            key = str(key)
        if account is not None:
            account = str(account)
        if domain is not None:
            domain = str(domain)

        if logger:
            logger.info(
                f"NATS KV command parameters - bucket: {bucket}, key: {key}, account: {account}, domain: {domain}"
            )

        # Validate required parameters
        if not bucket:
            raise ValueError("Bucket parameter is required")

        if not account:
            raise ValueError("Account parameter is required")

        # Get account configuration
        account_config = _get_account_config(command_instance, account)

        events = asyncio.run(
            _get_kv_history(bucket, key, account_config, logger, domain)
        )
    except USER_ERRORS as e:
        if logger:
            logger.error("natskv failed: %s", e)
        # Writes one ERROR message and exits; no generic re-raise location.
        command_instance.error_exit(
            e, kv_error_message(e, bucket, domain, "domain=<domain>")
        )
        return

    yield from events


async def _get_kv_history(bucket, key, account_config, logger=None, domain=None):
    """
    Async function to connect to NATS and retrieve KV history

    Args:
        bucket: KV bucket name
        key: Key to get history for
        account_config: Account configuration dictionary
        logger: Optional logger instance
        domain: Optional JetStream domain

    Returns:
        List of event dictionaries for Splunk
    """
    connection = None
    events = []
    try:
        # Connect to NATS: one bounded attempt that fails with the real cause
        connection = await nats_connection.connect(
            account_config, name="splunk-nats natskv"
        )
        nc = connection.nc

        # Get the connected server URL for the host field
        connected_host = "unknown"
        if nc.connected_url:
            connected_host = nc.connected_url.netloc

        # Get JetStream context and KV bucket
        js = nc.jetstream(domain=domain)
        await js.key_value(bucket)  # BucketNotFoundError if it does not exist

        # Convert the entries (including delete and purge markers) to Splunk events
        for entry in await read_history(js, bucket, key):
            events.append(
                {
                    "_time": entry_time(entry),
                    "_raw": entry_raw(entry),
                    "source": entry.key,
                    "sourcetype": SOURCETYPE,
                    "host": connected_host,
                    "revision": entry.revision,
                    "operation": entry_operation(entry),
                }
            )

    except Exception as e:
        if logger:
            logger.error("NATS connection error: %s", str(e))
        raise
    finally:
        if connection:
            await connection.close()

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
    # Get session key from command instance
    session_key = getattr(command_instance, "session_key", None)
    if not session_key:
        # Try to get from service
        service = getattr(command_instance, "service", None)
        if service:
            session_key = service.token

    if not session_key:
        raise RuntimeError("Unable to get session key for configuration access")

    return nats_connection.load_account(session_key, str(account_name))
