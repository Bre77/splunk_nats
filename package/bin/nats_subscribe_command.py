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
from solnlib import conf_manager


def generate(command_instance):
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
        subject = getattr(command_instance, "subject", None)
        account = getattr(command_instance, "account", None)

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

        # Run the async function to subscribe and stream messages
        try:
            # Get server for host field - use first server from servers list
            servers = account_config.get("servers", "nats://localhost:4222")
            server_host = servers.split(",")[0].strip()

            # Convert entries to Splunk events
            event_count = 0
            for event in asyncio.run(
                _subscribe_to_topic(subject, account_config, server_host, logger)
            ):
                yield event
                event_count += 1

            if logger:
                logger.info("Successfully processed %d events", event_count)

        except Exception as e:
            if logger:
                logger.error("Error in NATS subscription: %s", str(e))
            raise RuntimeError(f"Subscription error: {str(e)}")

    except Exception as e:
        raise RuntimeError(str(e))


async def _subscribe_to_topic(subject, account_config, server_host, logger=None):
    """
    Async function to connect to NATS and subscribe to topic, yielding events as they arrive

    Args:
        subject: NATS subject to subscribe to
        account_config: Account configuration dictionary
        server_host: Server host for event host field
        logger: Optional logger instance

    Yields:
        Event dictionaries for Splunk
    """
    nc = None
    event_queue = asyncio.Queue()

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

        # No connection timeout for continuous monitoring

        # Connect to NATS
        if logger:
            logger.debug("Connecting to NATS servers: %s", server_list)
        nc = await nats.connect(servers=server_list, **connect_options)
        if logger:
            logger.debug("Connected to NATS server")

        # Message handler that immediately queues events
        async def message_handler(msg):
            if logger:
                logger.debug("Received message on subject '%s'", msg.subject)

            message_data = {
                "subject": msg.subject,
                "data": msg.data,
                "reply": msg.reply,
                "headers": dict(msg.headers) if msg.headers else None,
                "received_time": time.time(),
            }
            event = _create_event(message_data, server_host)
            if event:
                await event_queue.put(event)

        # Subscribe to the subject
        if logger:
            logger.debug("Subscribing to subject: %s", subject)
        sub = await nc.subscribe(subject, cb=message_handler)

        if logger:
            logger.debug("Listening for messages continuously")

        # Yield events as they arrive continuously
        try:
            while True:
                event = await event_queue.get()
                yield event
        except asyncio.CancelledError:
            pass

        # Unsubscribe
        await sub.unsubscribe()
        if logger:
            logger.debug("Unsubscribed from subject: %s", subject)

    except Exception as e:
        if logger:
            logger.error("NATS subscription error: %s", str(e))
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


def _create_event(message, server):
    """Convert a NATS message to a Splunk event"""
    try:
        # Handle the message data - decode to string for _raw
        if message["data"]:
            try:
                # Try to decode as UTF-8
                data_str = message["data"].decode("utf-8")
            except UnicodeDecodeError:
                # If it's not valid UTF-8, encode as base64
                data_str = base64.b64encode(message["data"]).decode("ascii")
        else:
            data_str = ""

        # Create the Splunk event
        event = {
            "_time": message["received_time"],
            "_raw": data_str,
            "source": message["subject"],
            "sourcetype": "nats:topic",
            "host": server,
            "subject": message["subject"],
            "reply": message["reply"],
        }

        # Add headers as fields if they exist
        if message["headers"]:
            for key, value in message["headers"].items():
                event[f"header_{key}"] = value

        return event

    except Exception as e:
        return {
            "_time": time.time(),
            "_raw": f"Failed to process message: {str(e)}",
            "source": message.get("subject", "unknown"),
            "sourcetype": "nats:subscribe:error",
            "host": server,
            "subject": message.get("subject", "unknown"),
            "error_type": "message_processing_error",
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

        return dict(account_config)

    except Exception as e:
        raise Exception(
            f"Failed to get account configuration '{account_name}': {str(e)}"
        )


# UCC handles command dispatch automatically
