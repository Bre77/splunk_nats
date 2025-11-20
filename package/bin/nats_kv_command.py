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

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

# These imports are not needed for UCC custom search commands
# UCC generates the command wrapper automatically
import base64

from nats.js.errors import KeyNotFoundError, NoKeysError
from solnlib import conf_manager

import nats


def generate(command_instance):
    """
    Generate function for NATS JetStream KV history command.
    This function will be called by the UCC-generated wrapper.

    Args:
        command_instance: The command instance with options set

    Yields:
        Dict: Event dictionaries for Splunk
    """
    try:
        # Get command arguments
        bucket = getattr(command_instance, "bucket", None)
        key = getattr(command_instance, "key", ">")
        account = getattr(command_instance, "account", None)

        # Get logger if available
        logger = getattr(command_instance, "logger", None)

        # Convert parameters to strings and validate
        if bucket is not None:
            bucket = str(bucket)
        if key is not None:
            key = str(key)
        if account is not None:
            account = str(account)

        if logger:
            logger.info(
                f"NATS KV command parameters - bucket: {bucket}, key: {key}, account: {account}"
            )

        # Validate required parameters
        if not bucket:
            raise ValueError("Bucket parameter is required")

        if not account:
            raise ValueError("Account parameter is required")

        # Get account configuration
        account_config = _get_account_config(command_instance, account)
        if not account_config:
            raise ValueError(f"Account configuration '{account}' not found or invalid")

        # Run the async function to get KV history
        try:
            events = asyncio.run(_get_kv_history(bucket, key, account_config, logger))
            for event in events:
                yield event

        except Exception as e:
            if logger:
                logger.error("Error retrieving KV history: %s", str(e))
            raise RuntimeError(f"Failed to retrieve KV history: {str(e)}")

    except Exception as e:
        raise RuntimeError(str(e))


async def _get_kv_history(bucket, key, account_config, logger=None):
    """
    Async function to connect to NATS and retrieve KV history

    Args:
        bucket: KV bucket name
        key: Key to get history for
        account_config: Account configuration dictionary
        logger: Optional logger instance

    Returns:
        List of event dictionaries for Splunk
    """
    nc = None
    events = []
    try:
        # Connection options
        connect_options = {}

        # Get server URLs (comma-separated)
        servers = account_config.get("servers", "nats://localhost:4222")
        server_list = [server.strip() for server in servers.split(",")]

        if account_config.get("username") and account_config.get("password"):
            connect_options["user"] = account_config["username"]
            connect_options["password"] = account_config["password"]

        # Set connection timeout
        connect_options["connect_timeout"] = account_config.get("connect_timeout", 30)

        # Connect to NATS
        nc = await nats.connect(servers=server_list, **connect_options)

        # Get the connected server URL for the host field
        connected_host = "unknown"
        if nc.connected_url:
            connected_host = nc.connected_url.netloc

        # Get JetStream context and KV bucket
        js = nc.jetstream()
        kv = await js.key_value(bucket)

        # Get the history for the key
        try:
            entries = await kv.history(key)

            # Convert entries to Splunk events
            for entry in entries:
                try:
                    # Handle the value - decode to string for _raw
                    if not entry.value:
                        continue
                    try:
                        value_str = entry.value.decode("utf-8")
                    except UnicodeDecodeError:
                        value_str = base64.b64encode(entry.value).decode("ascii")

                    # Create the Splunk event
                    event = {
                        "_time": entry.created.timestamp()
                        if entry.created
                        else time.time(),
                        "_raw": value_str,
                        "source": entry.key,
                        "sourcetype": "nats:json",
                        "host": connected_host,
                        "revision": entry.revision,
                    }

                    events.append(event)

                except Exception as e:
                    if logger:
                        logger.error("Failed to process entry: %s", str(e))

        except (KeyNotFoundError, NoKeysError):
            # No entries found - just return empty list
            pass

    except Exception as e:
        if logger:
            logger.error("NATS connection error: %s", str(e))
        raise
    finally:
        if nc:
            try:
                await nc.close()
            except Exception:
                pass

    return events


def _get_account_config(command_instance, account_name):
    """
    Retrieve account configuration from Splunk configuration using UCC patterns.

    Args:
        command_instance: The command instance
        account_name: Name of the account configuration

    Returns:
        Dictionary containing account configuration or None if not found
    """
    try:
        # Ensure account_name is a string
        if not isinstance(account_name, str):
            account_name = str(account_name)

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
        try:
            cfm = conf_manager.ConfManager(
                session_key,
                "nats",
                realm="__REST_CREDENTIAL__#nats#configs/conf-nats_account",
            )

            account_conf_file = cfm.get_conf("nats_account")
            account_config = account_conf_file.get(account_name)
        except Exception as e:
            raise Exception(
                f"Failed to access configuration manager for account '{account_name}': {str(e)}"
            )

        if not account_config:
            return None

        # Convert account_config to dict and handle any special conversions
        config = dict(account_config)

        # Convert connect_timeout to int if it exists and is not already an int
        if "connect_timeout" in config and config["connect_timeout"] is not None:
            try:
                config["connect_timeout"] = int(config["connect_timeout"])
            except (ValueError, TypeError):
                config["connect_timeout"] = 30  # Default timeout

        return config

    except Exception as e:
        raise Exception(
            f"Failed to get account configuration '{account_name}': {str(e)}"
        )
