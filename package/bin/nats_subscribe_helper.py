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

import logging
import os
import sys
import time
from typing import Any, Dict, Optional

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

# Import Splunk libraries
import nats_connection
import nats_logging
from nats.aio.msg import Msg
from splunklib import modularinput as smi

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
            nats_connection.load_account(session_key, account)

    except Exception as e:
        raise Exception(f"Input validation failed: {str(e)}")


def stream_events(inputs: smi.InputDefinition, event_writer: smi.EventWriter) -> None:
    """
    Stream events from NATS topics.

    Args:
        inputs: InputDefinition object containing all input configurations
        event_writer: EventWriter object for writing events to Splunk
    """
    session_key = inputs.metadata.get("session_key")
    for input_name, input_item in inputs.inputs.items():
        # Splunk runs one process per input, so this is the process's log
        nats_logging.setup_input(input_name, session_key)
        try:
            # Get input configuration
            subject = input_item.get("subject")
            account = input_item.get("account")
            sourcetype = input_item.get("sourcetype") or "nats:message"

            # Get session key for configuration access
            if not session_key:
                raise ValueError("Unable to get session key for configuration access")
            ingestion = nats_logging.Ingestion(
                input_name, input_item.get("index"), account
            )

            # The decoder is started once and kept running across reconnects
            decoder = _create_decoder(input_item)
            decoded_sourcetype = (
                input_item.get("decoded_sourcetype") or DEFAULT_SOURCETYPE
            )

            # Subscribe to NATS topic, reconnecting forever. The account is reloaded
            # before every connection attempt so corrected settings are picked up.
            nats_connection.run_input(
                input_name,
                lambda: nats_connection.load_account(session_key, account),
                lambda connection: _subscribe_to_nats(
                    connection=connection,
                    subject=subject,
                    sourcetype=sourcetype,
                    event_writer=event_writer,
                    decoder=decoder,
                    decoded_sourcetype=decoded_sourcetype,
                    input_name=input_name,
                    ingestion=ingestion,
                ),
                log=logger,
                cleanup=decoder.close if decoder is not None else None,
                housekeeping=nats_logging.InputHousekeeping(ingestion, session_key).run,
            )

        except Exception as e:
            nats_connection.log_failure(
                logger, e, "%s: failed to subscribe to NATS topic", input_name
            )


async def _subscribe_to_nats(
    connection: nats_connection.Connection,
    subject: str,
    sourcetype: str,
    event_writer: smi.EventWriter,
    decoder: Optional[Decoder] = None,
    decoded_sourcetype: str = DEFAULT_SOURCETYPE,
    input_name: str = "nats_subscribe://",
    ingestion: Optional[nats_logging.Ingestion] = None,
) -> None:
    """
    Subscribe to a NATS subject on a live connection and write each message as an event.

    Args:
        connection: Connected NATS connection
        subject: NATS subject to subscribe to
        sourcetype: Sourcetype for events
        event_writer: EventWriter for outputting events
        decoder: Optional payload decoder
        decoded_sourcetype: Sourcetype for decoded events
        input_name: The input's full name, nats_subscribe://<name>
        ingestion: Counts the events written, for the Monitoring Dashboard
    """
    nc = connection.nc
    decode_errors = (
        DecodeErrorLog(logger.warning, decoder.name) if decoder is not None else None
    )

    try:
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
                event_sourcetype = decoded_sourcetype if rendered.decoded else sourcetype
                event = smi.Event(
                    data=rendered.raw,
                    time=time.time(),
                    source=nats_logging.event_source(input_name, msg.subject),
                    sourcetype=event_sourcetype,
                    host=connected_host,
                )

                event_writer.write_event(event)
                if ingestion is not None:
                    ingestion.add(event_sourcetype)

            except Exception as e:
                nats_logging.log_error(
                    logger,
                    "Ingestion Error",
                    "Failed to process NATS message from subject '%s': %s",
                    msg.subject,
                    e,
                )

        # Subscribe and wait for the server to confirm, so a permissions violation
        # on the subject is reported now rather than leaving the input silently idle.
        await nc.subscribe(subject, cb=message_handler)
        await connection.confirm()

    except Exception as e:
        raise nats_connection.NatsConnectError(
            f"Failed to subscribe to NATS topic '{subject}': "
            f"{nats_connection.describe_error(e, connection.servers)}"
        ) from e


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
        nats_logging.log_error(
            logger, "Configuration Error", "%s; payloads will be kept as base64", e
        )
        return BrokenDecoder(name, str(e))
    try:
        decoder.check()
    except DecodeError as e:
        nats_logging.log_error(
            logger,
            "Configuration Error",
            "%s; payloads are kept as base64 until it is in place",
            e,
        )
    return decoder
