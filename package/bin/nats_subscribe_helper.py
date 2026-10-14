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
from nats_decoder import (
    DEFAULT_SOURCETYPE,
    BrokenDecoder,
    DecodeError,
    Decoder,
    DecodeErrorLog,
    DecoderConfigError,
    render,
)

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
            sourcetype = input_item.get("sourcetype") or "nats:message"

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
                    decoder=_create_decoder(input_item),
                    decoded_sourcetype=input_item.get("decoded_sourcetype")
                    or DEFAULT_SOURCETYPE,
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
    decoder: Optional[Decoder] = None,
    decoded_sourcetype: str = DEFAULT_SOURCETYPE,
) -> None:
    """
    Subscribe to NATS topic and stream messages continuously.

    Args:
        input_name: Name of the input
        subject: NATS subject to subscribe to
        account_config: Account configuration dictionary
        sourcetype: Sourcetype for events
        event_writer: EventWriter for outputting events
        decoder: Optional payload decoder
        decoded_sourcetype: Sourcetype for decoded events
    """
    nc = None
    decode_errors = (
        DecodeErrorLog(logger.warning, decoder.name) if decoder is not None else None
    )

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

                # Render the payload for _raw: the decoder's event when a decoder is
                # set, otherwise UTF-8 text or base64. Failed decodes keep the payload.
                rendered = await render(decoder, msg.data, msg.subject, msg.headers)
                if rendered.error is not None and decode_errors is not None:
                    decode_errors.record(msg.subject, rendered.error)

                # Create the Splunk event with only the message data in _raw
                event = smi.Event(
                    data=rendered.raw,
                    time=time.time(),
                    source=msg.subject,
                    sourcetype=decoded_sourcetype if rendered.decoded else sourcetype,
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
        if decoder is not None:
            await decoder.close()


def _create_decoder(input_item: Dict[str, Any]) -> Optional[Decoder]:
    """
    Create the input's payload decoder, if it has one. A decoder problem must not
    stop ingestion: payloads are then kept as base64 and the reason is logged.
    """
    name = (input_item.get("decoder") or "").strip()
    if not name:
        return None
    try:
        decoder = Decoder(name, input_item.get("decoder_args") or "")
    except DecoderConfigError as e:
        logger.error(f"{e}; payloads will be kept as base64")
        return BrokenDecoder(name, str(e))
    try:
        decoder.check()
    except DecodeError as e:
        logger.error(f"{e}; payloads are kept as base64 until it is in place")
    return decoder


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
