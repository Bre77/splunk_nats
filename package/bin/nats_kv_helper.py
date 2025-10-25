"""
NATS JetStream Key-Value Input Helper Module

This module provides the input helper functions for collecting data from NATS JetStream KV buckets.
UCC will call validate_input() during configuration validation and stream_events() during data collection.
"""

import time
import sys
import os
import asyncio
import base64
import logging
from typing import Dict, Any, Optional

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

# Import Splunk libraries
from splunklib import modularinput as smi
from solnlib import conf_manager
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
        if not bucket.replace("_", "").replace("-", "").replace(".", "").isalnum():
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
            sourcetype = input_item.get("sourcetype", "nats:kv")

            # Get session key for configuration access
            session_key = inputs.metadata.get("session_key")
            if not session_key:
                raise Exception("Unable to get session key for configuration access")

            # Get account configuration
            account_config = _get_account_config(session_key, account)
            if not account_config:
                raise Exception(f"Account configuration '{account}' not found")

            # Monitor NATS JetStream KV bucket
            asyncio.run(
                _monitor_kv_bucket(
                    input_name=input_name,
                    bucket=bucket,
                    subject=subject,
                    account_config=account_config,
                    sourcetype=sourcetype,
                    event_writer=event_writer,
                )
            )

        except Exception as e:
            # Log error instead of writing as event
            logger.error(f"Failed to monitor NATS KV bucket: {str(e)}")


async def _monitor_kv_bucket(
    input_name: str,
    bucket: str,
    subject: str,
    account_config: Dict[str, Any],
    sourcetype: str,
    event_writer: smi.EventWriter,
) -> None:
    """
    Monitor NATS JetStream KV bucket for changes.

    Args:
        input_name: Name of the input
        bucket: KV bucket name
        subject: Subject pattern to watch
        account_config: Account configuration dictionary
        sourcetype: Sourcetype for events
        event_writer: EventWriter for outputting events
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

        # No connection timeout for continuous monitoring

        # Connect to NATS
        nc = await nats.connect(servers=server_list, **connect_options)

        # Get JetStream context and KV bucket
        js = nc.jetstream()
        kv = await js.key_value(bucket)

        # Use KV watcher to monitor changes in real-time
        try:
            # Create a watcher for the subject pattern
            watcher = await kv.watch(subject, include_history=True)

            # Get the connected server URL for the host field
            connected_host = "unknown"
            if nc.connected_url:
                connected_host = nc.connected_url.netloc

            # Write start monitoring event
            start_event = smi.Event(
                data=f"Started watching KV bucket '{bucket}' with pattern '{subject}'",
                time=time.time(),
                source=f"{bucket}.{subject}",
                sourcetype="nats:kv_watch_start",
                host=connected_host,
            )
            event_writer.write_event(start_event)

            # Process watcher events
            async for entry in watcher:
                if entry is None or not entry.value:
                    # Initial callback when starting the watch with no pending updates
                    continue

                # Determine the raw value to write
                try:
                    raw_value = entry.value.decode("utf-8")
                except UnicodeDecodeError:
                    raw_value = base64.b64encode(entry.value).decode("ascii")

                # Create and write the Splunk event
                event = smi.Event(
                    data=raw_value,
                    time=entry.created.timestamp() if entry.created else time.time(),
                    source=f"{bucket}.{entry.key}",
                    sourcetype=sourcetype,
                    host=connected_host,
                )

                event_writer.write_event(event)

        except Exception as e:
            # Log error instead of writing as event
            logger.error(
                f"Failed to watch KV bucket '{bucket}' with pattern '{subject}': {str(e)}"
            )

    except Exception as e:
        # Log error instead of writing as event
        logger.error(
            f"Failed to connect to NATS or access KV bucket '{bucket}' with pattern '{subject}': {str(e)}"
        )

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


# Subject pattern matching is now handled by the NATS KV watcher directly
# The watcher accepts NATS subject patterns and handles wildcards internally
