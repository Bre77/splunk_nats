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
NATS Subscribe Input Helper Module

This module provides the input helper functions for collecting data from NATS topics.
UCC will call validate_input() during configuration validation and stream_events() during data collection.
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
from nats.aio.msg import Msg
from solnlib import conf_manager
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
        subject = definition.parameters.get("subject")
        account = definition.parameters.get("account")

        # Validate required fields
        if not subject:
            raise ValueError("Subject is required")

        if not account:
            raise ValueError("Account is required")

        # Validate subject pattern (basic validation)
        if not subject:
            raise ValueError("Subject pattern cannot be empty")

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
    Stream events from NATS topics.

    Args:
        inputs: InputDefinition object containing all input configurations
        event_writer: EventWriter object for writing events to Splunk
    """
    for input_name, input_item in inputs.inputs.items():
        try:
            # Get input configuration
            subject = input_item.get("subject")
            account = input_item.get("account")
            sourcetype = input_item.get("sourcetype", "nats:message")

            # Get session key for configuration access
            session_key = inputs.metadata.get("session_key")
            if not session_key:
                raise Exception("Unable to get session key for configuration access")

            # Get account configuration
            account_config = _get_account_config(session_key, account)
            if not account_config:
                raise Exception(f"Account configuration '{account}' not found")

            # Subscribe to NATS topic
            asyncio.run(
                _subscribe_to_nats(
                    input_name=input_name,
                    subject=subject,
                    account_config=account_config,
                    sourcetype=sourcetype,
                    event_writer=event_writer,
                )
            )

        except Exception as e:
            # Log error instead of writing as event
            logger.error(f"Failed to subscribe to NATS topic: {str(e)}")


async def _subscribe_to_nats(
    input_name: str,
    subject: str,
    account_config: Dict[str, Any],
    sourcetype: str,
    event_writer: smi.EventWriter,
) -> None:
    """
    Subscribe to NATS topic and stream messages continuously.

    Args:
        input_name: Name of the input
        subject: NATS subject to subscribe to
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

        # Connect to NATS
        nc = await nats.connect(servers=server_list, **connect_options)

        # Message handler that writes events directly
        async def message_handler(msg: Msg):
            try:
                # Get the connected server URL for the host field
                connected_host = "unknown"
                if nc.connected_url:
                    connected_host = nc.connected_url.netloc

                # Handle the message data - decode to string for _raw
                if msg.data:
                    try:
                        # Try to decode as UTF-8
                        data_str = msg.data.decode("utf-8")
                    except UnicodeDecodeError:
                        # If it's not valid UTF-8, encode as base64
                        data_str = base64.b64encode(msg.data).decode("ascii")
                else:
                    data_str = ""

                # Create the Splunk event with only the message data in _raw
                event = smi.Event(
                    data=data_str,
                    time=time.time(),
                    source=msg.subject,
                    sourcetype=sourcetype,
                    host=connected_host,
                )

                event_writer.write_event(event)

            except Exception as e:
                # Log error
                logger.error(
                    f"Failed to process NATS message from subject '{msg.subject}': {str(e)}"
                )

        # Subscribe to the subject
        sub = await nc.subscribe(subject, cb=message_handler)

        # Run continuously until interrupted
        try:
            while True:
                await asyncio.sleep(3600)  # Sleep in 1-hour chunks to keep alive
        except asyncio.CancelledError:
            pass
        finally:
            # Unsubscribe when done
            await sub.unsubscribe()

    except Exception as e:
        # Log error instead of writing as event
        logger.error(f"Failed to subscribe to NATS topic '{subject}': {str(e)}")

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
