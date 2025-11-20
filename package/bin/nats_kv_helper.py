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
NATS JetStream Key-Value Input Helper Module

This module provides the input helper functions for collecting data from NATS JetStream KV buckets.
UCC will call validate_input() during configuration validation and stream_events() during data collection.

Checkpointing:
This module implements checkpointing to prevent reingesting the same data on restart.
- Checkpoints are stored using Splunk's KVStore via solnlib.modular_input.checkpointer
- Each input stores its last processed revision number as the checkpoint
- On startup, the module resumes from the last checkpoint to avoid duplicate events
- The checkpoint is updated after successfully processing each batch of events
- Configuration files required: collections.conf and transforms.conf for the checkpointer
"""

import asyncio
import base64
import logging
import os
import sys
import time
from typing import Any, Dict, Optional

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

# Import Splunk libraries
from solnlib import conf_manager
from solnlib.modular_input import checkpointer
from splunklib import modularinput as smi

import nats

# Set up logger
logger = logging.getLogger(__name__)


def validate_input(definition: smi.ValidationDefinition) -> None:
    """
    Validate the input configuration.

    Args:
        definition: ValidationDefinition object containing input parameters

    Raises:
        Exception: If validation fails
    """
    try:
        # Get input parameters
        bucket = definition.parameters.get("bucket")
        subject = definition.parameters.get("subject")
        account = definition.parameters.get("account")

        # Validate required fields
        if not bucket:
            raise ValueError("Bucket name is required")

        if not subject:
            raise ValueError("Subject is required")

        if not account:
            raise ValueError("Account is required")

        # Validate bucket name (basic validation)
        valid_chars = set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        )
        if not all(c in valid_chars for c in bucket):
            raise ValueError(
                "Bucket name must contain only alphanumeric characters, hyphens, underscores, and periods"
            )

        # Try to get account configuration to validate it exists
        session_key = definition.metadata.get("session_key")
        if session_key:
            account_config = _get_account_config(session_key, account)
            if not account_config:
                raise ValueError(f"Account '{account}' not found or invalid")

    except Exception as e:
        raise Exception(f"Input validation failed: {str(e)}")


def stream_events(inputs: smi.InputDefinition, event_writer: smi.EventWriter) -> None:
    """
    Stream events from NATS JetStream KV bucket.

    Args:
        inputs: InputDefinition object containing all input configurations
        event_writer: EventWriter object for writing events to Splunk
    """
    for input_name, input_item in inputs.inputs.items():
        try:
            # Get input configuration
            bucket = input_item.get("bucket")
            subject = input_item.get("subject", "*")
            account = input_item.get("account")
            sourcetype = input_item.get("sourcetype", "nats:json")

            # Get session key for configuration access
            session_key = inputs.metadata.get("session_key")
            if not session_key:
                raise Exception("Unable to get session key for configuration access")

            # Get account configuration
            account_config = _get_account_config(session_key, account)
            if not account_config:
                raise Exception(f"Account configuration '{account}' not found")

            # Set up checkpointing
            kvstore_checkpointer = None
            current_checkpoint = None
            checkpointer_key_name = None

            try:
                kvstore_checkpointer = checkpointer.KVStoreCheckpointer(
                    "nats_kv_checkpointer",
                    session_key,
                    "nats",
                )

                # Get checkpoint key for this input
                checkpointer_key_name = _get_checkpointer_key_name(input_name)
                current_checkpoint = kvstore_checkpointer.get(checkpointer_key_name)

                if current_checkpoint is not None:
                    logger.info(
                        f"Resuming from checkpoint revision {current_checkpoint} for input {input_name}"
                    )
                else:
                    logger.info(
                        f"No checkpoint found, starting from beginning for input {input_name}"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to initialize checkpointer for input {input_name}: {str(e)}. "
                    "Continuing without checkpointing."
                )
                kvstore_checkpointer = None
                current_checkpoint = None

            # Monitor NATS JetStream KV bucket
            last_revision = asyncio.run(
                _monitor_kv_bucket(
                    bucket=bucket,
                    subject=subject,
                    account_config=account_config,
                    sourcetype=sourcetype,
                    event_writer=event_writer,
                    starting_revision=current_checkpoint,
                )
            )

            # Update checkpoint with the last processed revision
            if (
                last_revision is not None
                and kvstore_checkpointer is not None
                and checkpointer_key_name is not None
            ):
                try:
                    kvstore_checkpointer.update(checkpointer_key_name, last_revision)
                    logger.info(
                        f"Updated checkpoint to revision {last_revision} for input {input_name}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to update checkpoint for input {input_name}: {str(e)}"
                    )
            else:
                if last_revision is None:
                    logger.info(f"No new events processed for input {input_name}")
                else:
                    logger.warning(
                        f"Checkpointer not available, cannot save checkpoint for input {input_name}"
                    )

        except Exception as e:
            # Log error instead of writing as event
            logger.error(f"Failed to monitor NATS KV bucket: {str(e)}")


async def _monitor_kv_bucket(
    bucket: str,
    subject: str,
    account_config: Dict[str, Any],
    sourcetype: str,
    event_writer: smi.EventWriter,
    starting_revision: Optional[int] = None,
) -> Optional[int]:
    """
    Monitor NATS JetStream KV bucket for changes.

    Args:
        bucket: KV bucket name
        subject: Subject pattern to watch
        account_config: Account configuration dictionary
        sourcetype: Sourcetype for events
        event_writer: EventWriter for outputting events
        starting_revision: Optional revision number to start from

    Returns:
        Last processed revision number, or None if no events processed
    """
    nc = None
    try:
        # Connection options
        connect_options = {}

        # Get server URLs (comma-separated)
        servers = account_config.get("servers", "nats://localhost:4222")
        server_list = [server.strip() for server in servers.split(",")]

        if account_config.get("username") and account_config.get("password"):
            connect_options["user"] = account_config["username"]
            connect_options["password"] = account_config["password"]

        # Connect to NATS
        nc = await nats.connect(servers=server_list, **connect_options)

        # Get JetStream context and KV bucket
        js = nc.jetstream()
        kv = await js.key_value(bucket)

        # Get the connected server URL for the host field
        connected_host = "unknown"
        if nc.connected_url:
            connected_host = nc.connected_url.netloc

        # Create a watcher for the subject pattern
        # Always include history to catch up on missed entries when resuming
        watcher = await kv.watch(subject, include_history=True)

        last_revision = None

        # Process watcher events
        async for entry in watcher:
            if entry is None or not entry.value:
                continue

            # Skip entries we've already processed
            if starting_revision is not None and entry.revision <= starting_revision:
                logger.debug(
                    f"Skipping already processed entry at revision {entry.revision}"
                )
                continue

            # Determine the raw value to write
            try:
                raw_value = entry.value.decode("utf-8")
            except UnicodeDecodeError:
                raw_value = base64.b64encode(entry.value).decode("ascii")

            # Create and write the Splunk event
            event = smi.Event(
                data=raw_value,
                time=entry.created or time.time(),
                source=f"{bucket}.{entry.key}",
                sourcetype=sourcetype,
                host=connected_host,
            )

            event_writer.write_event(event)
            last_revision = entry.revision
            logger.debug(
                f"Processed entry at revision {entry.revision} for key {entry.key}"
            )

        return last_revision

    except Exception as e:
        logger.error(
            f"Failed to monitor KV bucket '{bucket}' with pattern '{subject}': {str(e)}"
        )
        return None

    finally:
        if nc:
            try:
                await nc.close()
            except Exception:
                pass


def _get_account_config(
    session_key: str, account_name: str
) -> Optional[Dict[str, Any]]:
    """
    Retrieve account configuration from Splunk configuration using UCC patterns.

    Args:
        session_key: Splunk session key
        account_name: Name of the account configuration

    Returns:
        Dictionary containing account configuration or None if not found
    """
    try:
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


def _get_checkpointer_key_name(input_name: str) -> str:
    """
    Get checkpointer key name for the given input.

    Args:
        input_name: Full input name (e.g., "nats_kv://input_name")

    Returns:
        Checkpointer key name
    """
    # Extract the input name from the full input string
    # Input name format is typically "nats_kv://<input_name>"
    return input_name.split("//")[-1]
