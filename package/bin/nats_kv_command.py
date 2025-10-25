#!/usr/bin/env python

import sys
import os

import asyncio
import time

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

# These imports are not needed for UCC custom search commands
# UCC generates the command wrapper automatically
import base64
import nats
from nats.js.errors import KeyNotFoundError, NoKeysError
from solnlib import conf_manager


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
        key = getattr(command_instance, "key", None)
        account = getattr(command_instance, "account", None)
        limit = getattr(command_instance, "limit", 100)

        # Validate required parameters
        if not bucket:
            raise ValueError("Bucket parameter is required")

        if not key:
            raise ValueError("Key parameter is required")

        if not account:
            raise ValueError("Account parameter is required")

        # Get logger if available
        logger = getattr(command_instance, "logger", None)
        if logger:
            logger.info(
                "NATS KV command starting: bucket='%s', key='%s', account='%s'",
                bucket,
                key,
                account,
            )

        # Get account configuration
        account_config = _get_account_config(command_instance, account)
        if not account_config:
            raise ValueError(f"Account configuration '{account}' not found or invalid")

        # Run the async function to get KV history
        try:
            entries = asyncio.run(
                _get_kv_history(bucket, key, account_config, limit, logger)
            )

            # Convert entries to Splunk events
            if not entries:
                if logger:
                    logger.info(
                        "No entries found for key '%s' in bucket '%s'",
                        key,
                        bucket,
                    )
                return

            if logger:
                logger.info("Processing %d entries into Splunk events", len(entries))

            # Get server for host field - use first server from servers list
            servers = account_config.get("servers", "nats://localhost:4222")
            server_host = servers.split(",")[0].strip()

            # Convert entries to Splunk events
            event_count = 0
            for entry in entries:
                event = _create_event(entry, bucket, server_host)
                if event:
                    yield event
                    event_count += 1

            if logger:
                logger.info("Successfully processed %d events", event_count)

        except Exception as e:
            if logger:
                logger.error("Error retrieving KV history: %s", str(e))
            raise RuntimeError(f"Failed to retrieve KV history: {str(e)}")

    except Exception as e:
        raise RuntimeError(str(e))


async def _get_kv_history(bucket, key, account_config, limit, logger=None):
    """
    Async function to connect to NATS and retrieve KV history

    Args:
        bucket: KV bucket name
        key: Key to get history for
        account_config: Account configuration dictionary
        limit: Maximum number of entries to retrieve
        logger: Optional logger instance

    Returns:
        List of KV entries
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
            if logger:
                logger.debug("Using authentication credentials")

        # Set connection timeout
        connect_options["connect_timeout"] = account_config.get("connect_timeout", 30)

        # Connect to NATS
        if logger:
            logger.debug("Connecting to NATS servers: %s", server_list)
        nc = await nats.connect(servers=server_list, **connect_options)

        # Get JetStream context and KV bucket
        js = nc.jetstream()
        kv = await js.key_value(bucket)
        if logger:
            logger.debug("Connected to KV bucket: %s", bucket)

        # Get the history for the key
        try:
            entries = await kv.history(key)
            if logger:
                logger.debug(
                    "Retrieved %d historical entries for key '%s'",
                    len(entries),
                    key,
                )

            # Apply limit if specified
            if limit and limit > 0:
                entries = entries[:limit]

            return entries

        except (KeyNotFoundError, NoKeysError):
            if logger:
                logger.debug("Key '%s' not found in bucket '%s'", key, bucket)
            return []

    except Exception as e:
        if logger:
            logger.error("NATS connection error: %s", str(e))
        raise
    finally:
        if nc:
            try:
                await nc.close()
                if logger:
                    logger.debug("NATS connection closed")
            except Exception as e:
                if logger:
                    logger.error("Error closing NATS connection: %s", str(e))


def _create_event(entry, bucket, server):
    """Convert a KV entry to a Splunk event"""
    try:
        # Handle the value - decode to string for _raw
        if entry.value:
            try:
                # Try to decode as UTF-8
                value_str = entry.value.decode("utf-8")
            except UnicodeDecodeError:
                # If it's not valid UTF-8, encode as base64
                value_str = base64.b64encode(entry.value).decode("ascii")
        else:
            value_str = ""

        # Create the Splunk event
        event = {
            "_time": entry.created.timestamp() if entry.created else time.time(),
            "_raw": value_str,
            "source": f"{entry.bucket}.{entry.key}",
            "sourcetype": "nats:kv:history",
            "host": server,
            "bucket": entry.bucket,
            "key": entry.key,
            "revision": entry.revision,
            "operation": entry.operation or "PUT",
        }

        return event

    except Exception as e:
        return {
            "_time": time.time(),
            "_raw": f"Failed to process entry: {str(e)}",
            "source": f"{bucket}.{getattr(entry, 'key', 'unknown')}",
            "sourcetype": "nats:kv:error",
            "host": server,
            "bucket": bucket,
            "key": getattr(entry, "key", "unknown"),
            "error_type": "entry_processing_error",
        }


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

        # Convert connect_timeout to int if it exists
        config = dict(account_config)
        if "connect_timeout" in config:
            config["connect_timeout"] = int(config["connect_timeout"])

        return config

    except Exception as e:
        raise Exception(
            f"Failed to get account configuration '{account_name}': {str(e)}"
        )
