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

Every put, delete and purge in the bucket is written as an event whose time is the
entry's created time. Delete and purge markers carry no value, so they are written as
a JSON record with the key, the operation and the revision.

Checkpointing:
- Each input stores the last processed revision (the bucket stream sequence) in the
  Splunk KV Store collection nats_kv_checkpointer via solnlib's KVStoreCheckpointer
- Events are held for at most CHECKPOINT_INTERVAL seconds (and written as soon as
  the initial snapshot has been read), then the checkpoint is saved and only then
  are the events written. The checkpoint therefore always covers exactly the events
  written, so stopping the input never needs a final save.
- During a Splunk restart splunkd stops serving the KV Store well before it stops
  inputs, while still indexing what they write. A save that fails holds the events
  back and pauses reading; if Splunk then stops the input, the held events were
  never written and are read again after the restart, so they are neither lost nor
  duplicated.
- On startup the input asks JetStream for entries after the checkpointed revision, so
  history is not replayed and a restart does not duplicate events. If the KV Store is
  not ready yet (it can still be initializing after Splunk starts), the input waits for
  it rather than reading the bucket from the beginning.
"""

import asyncio
import json
import logging
import os
import sys
import time
from typing import Any, List, Optional

# Add the lib directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

# Import Splunk libraries
import nats_connection
import nats_logging
from solnlib import splunk_rest_client
from solnlib.modular_input import checkpointer
from splunklib import modularinput as smi

import nats
from nats.js import api

from nats_kv_common import (
    entry_from_msg,
    entry_raw,
    entry_time,
    kv_error_message,
    kv_stream,
    kv_subject_prefix,
)

# Set up logger
logger = logging.getLogger(__name__)

# Longest time an event is held before its checkpoint is saved and it is written
CHECKPOINT_INTERVAL = 1

# Seconds between attempts to save the checkpoint once a save has failed
SAVE_RETRY_INTERVAL = 10

# Seconds to wait for an entry when there is nothing to checkpoint
IDLE_TIMEOUT = 60

# Seconds between attempts to read the checkpoint while the KV Store is not ready
KVSTORE_RETRY_INTERVAL = 5


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
        domain = definition.parameters.get("domain")

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
                "Bucket name must contain only letters, digits, hyphens and underscores"
            )

        # Try to get account configuration to validate it exists
        session_key = definition.metadata.get("session_key")
        if session_key:
            nats_connection.load_account(session_key, account)

    except Exception as e:
        raise Exception(f"Input validation failed: {str(e)}")


def stream_events(inputs: smi.InputDefinition, event_writer: smi.EventWriter) -> None:
    """
    Stream events from NATS JetStream KV bucket.

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
            bucket = input_item.get("bucket")
            subject = input_item.get("subject", "*")
            account = input_item.get("account")
            sourcetype = input_item.get("sourcetype") or "nats:kv"
            domain = input_item.get("domain")

            # Get session key for configuration access
            if not session_key:
                raise ValueError("Unable to get session key for configuration access")
            ingestion = nats_logging.Ingestion(
                input_name, input_item.get("index"), account
            )

            checkpoint = _open_checkpoint(session_key, input_name, bucket, domain)

            # Monitor NATS JetStream KV bucket until Splunk stops the input,
            # reconnecting forever. The account is reloaded before every connection
            # attempt so corrected settings are picked up.
            nats_connection.run_input(
                input_name,
                lambda: nats_connection.load_account(session_key, account),
                lambda connection: _monitor_kv_bucket(
                    connection=connection,
                    bucket=bucket,
                    subject=subject,
                    sourcetype=sourcetype,
                    event_writer=event_writer,
                    checkpoint=checkpoint,
                    domain=domain,
                    input_name=input_name,
                    ingestion=ingestion,
                ),
                log=logger,
                housekeeping=nats_logging.InputHousekeeping(ingestion, session_key).run,
            )

        except Exception as e:
            nats_connection.log_failure(
                logger, e, "%s: failed to monitor NATS KV bucket", input_name
            )


class Checkpoint:
    """
    The last processed revision of one input, persisted to the KV Store.

    The stored state records the bucket and domain it belongs to, so editing an
    input to watch a different bucket starts that bucket from the beginning
    instead of skipping its entries.
    """

    def __init__(
        self, store: Any, key: str, bucket: str, domain: Optional[str]
    ) -> None:
        self._store = store
        self._key = key
        self._bucket = bucket
        self._domain = domain or ""
        self.revision: Optional[int] = None
        self.saved_revision: Optional[int] = None
        self.save_failed = False
        self._saved_at = 0.0

    def load(self) -> Optional[int]:
        """
        Read the checkpoint; returns the last processed revision or None.

        Raises:
            Exception: If the KV Store could not be read
        """
        state = self._store.get(self._key)
        if (
            isinstance(state, dict)
            and state.get("bucket") == self._bucket
            and state.get("domain", "") == self._domain
            and isinstance(state.get("revision"), int)
        ):
            self.revision = self.saved_revision = state["revision"]
            logger.info(f"Resuming input {self._key} after revision {self.revision}")
        elif state is not None:
            logger.warning(
                f"Ignoring checkpoint for input {self._key} that does not match "
                f"bucket '{self._bucket}'; reading the bucket from the beginning"
            )
        else:
            logger.info(
                f"No checkpoint found, reading input {self._key} from the beginning"
            )
        return self.revision

    def reset(self) -> None:
        """Forget the revision so the bucket is read from the beginning."""
        self.revision = None
        self.saved_revision = None

    @property
    def unsaved(self) -> bool:
        """Whether the revision changed since the last save."""
        return self.revision is not None and self.revision != self.saved_revision

    def seconds_until_due(self) -> float:
        """Seconds until the next save is due."""
        interval = SAVE_RETRY_INTERVAL if self.save_failed else CHECKPOINT_INTERVAL
        return max(0.0, self._saved_at + interval - time.monotonic())

    def save(self) -> bool:
        """Persist the revision if it changed; returns False if that failed."""
        if not self.unsaved:
            return True
        self._saved_at = time.monotonic()
        state = {
            "bucket": self._bucket,
            "domain": self._domain,
            "revision": self.revision,
        }
        try:
            self._store.update(self._key, state)
        except Exception as e:
            if not self.save_failed:
                logger.warning(
                    f"Failed to save checkpoint for {self._key}, holding events "
                    f"until the KV Store is available: {str(e)}"
                )
            self.save_failed = True
            return False
        self.saved_revision = self.revision
        self.save_failed = False
        logger.debug(f"Saved checkpoint revision {self.revision} for {self._key}")
        return True


def _open_checkpoint(
    session_key: str, input_name: str, bucket: str, domain: Optional[str]
) -> Checkpoint:
    """
    Load the input's checkpoint, waiting until the KV Store can serve it.

    Starting without the checkpoint would index the whole bucket again, so a KV
    Store that is still initializing, or unavailable, delays the input instead.
    """
    last_warning = None
    while True:
        try:
            status = _kvstore_status(session_key)
            if status == "ready":
                store = checkpointer.KVStoreCheckpointer(
                    "nats_kv_checkpointer", session_key, "nats"
                )
                checkpoint = Checkpoint(
                    store, _get_checkpointer_key_name(input_name), bucket, domain
                )
                checkpoint.load()
                return checkpoint
            reason = f"KV Store status is '{status}'"
        except Exception as e:
            reason = str(e)
        if last_warning is None or time.monotonic() - last_warning >= 60:
            last_warning = time.monotonic()
            logger.warning(
                f"Waiting for the KV Store to read the checkpoint of input "
                f"{input_name}: {reason}"
            )
        time.sleep(KVSTORE_RETRY_INTERVAL)


def _kvstore_status(session_key: str) -> str:
    """Return the KV Store status reported by splunkd, e.g. 'ready' or 'starting'."""
    response = splunk_rest_client.SplunkRestClient(session_key, "nats").get(
        "kvstore/status", output_mode="json"
    )
    content = json.loads(response.body.read())["entry"][0]["content"]
    return content["current"]["status"]


async def _monitor_kv_bucket(
    connection: nats_connection.Connection,
    bucket: str,
    subject: str,
    sourcetype: str,
    event_writer: smi.EventWriter,
    checkpoint: Checkpoint,
    domain: Optional[str] = None,
    input_name: str = "nats_kv://",
    ingestion: Optional[nats_logging.Ingestion] = None,
) -> None:
    """
    Write every change in a NATS JetStream KV bucket as an event, starting after
    the checkpointed revision, until cancelled or the connection fails.

    Args:
        connection: Connected NATS connection
        bucket: KV bucket name
        subject: Key filter to watch (NATS wildcards allowed)
        sourcetype: Sourcetype for events
        event_writer: EventWriter for outputting events
        checkpoint: Checkpoint holding the last processed revision
        domain: Optional JetStream domain
        input_name: The input's full name, nats_kv://<name>
        ingestion: Counts the events written, for the Monitoring Dashboard
    """
    nc = connection.nc

    # Events an earlier session held were never written, so read them again.
    checkpoint.revision = checkpoint.saved_revision

    try:
        # Get JetStream context and KV bucket
        js = nc.jetstream(domain=domain)
        kv = await js.key_value(bucket)
        status = await kv.status()

        # Get the connected server URL for the host field
        connected_host = "unknown"
        if nc.connected_url:
            connected_host = nc.connected_url.netloc

        # A checkpoint past the bucket's last revision means the bucket was
        # recreated, so its revisions started again from 1.
        last_revision = status.stream_info.state.last_seq
        if checkpoint.revision is not None and checkpoint.revision > last_revision:
            logger.warning(
                f"Checkpoint revision {checkpoint.revision} is past the last revision "
                f"{last_revision} of bucket '{bucket}' (was it recreated?); "
                "reading the bucket from the beginning"
            )
            checkpoint.reset()

        # Read the bucket stream directly with an ordered consumer, which (unlike
        # kv.watch) can start after the checkpointed revision.
        config = None
        deliver_policy = api.DeliverPolicy.ALL
        if checkpoint.revision is not None:
            deliver_policy = api.DeliverPolicy.BY_START_SEQUENCE
            config = api.ConsumerConfig(opt_start_seq=checkpoint.revision + 1)
        sub = await js.subscribe(
            f"{kv_subject_prefix(bucket)}{subject}",
            stream=kv_stream(bucket),
            config=config,
            ordered_consumer=True,
            deliver_policy=deliver_policy,
            inactive_threshold=5 * 60,
        )

        batch: List[smi.Event] = []
        snapshot_done = False
        while True:
            # Wait for the next entry, but no longer than until the held events
            # are due to be checkpointed and written.
            timeout = IDLE_TIMEOUT
            if checkpoint.unsaved:
                timeout = checkpoint.seconds_until_due()
                if checkpoint.save_failed and timeout > 0:
                    # The KV Store is unavailable (e.g. Splunk is stopping):
                    # stop reading until the held events can be checkpointed.
                    await asyncio.sleep(timeout)
                    continue
                if timeout == 0:
                    _write_batch(batch, checkpoint, event_writer, ingestion)
                    continue
            try:
                msg = await sub.next_msg(timeout=timeout)
            except nats.errors.TimeoutError:
                continue

            entry = entry_from_msg(bucket, msg)
            if entry is not None:
                batch.append(
                    smi.Event(
                        data=entry_raw(entry),
                        time=entry_time(entry),
                        source=nats_logging.event_source(input_name, msg.subject),
                        sourcetype=sourcetype,
                        host=connected_host,
                    )
                )
            checkpoint.revision = msg.metadata.sequence.stream

            if not snapshot_done and msg.metadata.num_pending == 0:
                snapshot_done = True
                logger.info(
                    f"Read bucket '{bucket}' up to revision {checkpoint.revision}; "
                    "watching for changes"
                )
                _write_batch(batch, checkpoint, event_writer, ingestion)

    except nats_connection.InputStopped:
        raise
    except Exception as e:
        # Events still held were not checkpointed, so they are read again next time
        detail = kv_error_message(e, bucket, domain, "the Domain field")
        if detail == str(e):
            detail = nats_connection.describe_error(e, connection.servers)
        raise nats_connection.NatsConnectError(
            f"Failed to monitor KV bucket '{bucket}' with pattern '{subject}': {detail}"
        ) from e


def _write_batch(
    batch: List[smi.Event],
    checkpoint: Checkpoint,
    event_writer: smi.EventWriter,
    ingestion: Optional[nats_logging.Ingestion] = None,
) -> None:
    """
    Save the checkpoint for the held events, then write them. If the save fails
    the events stay held. If writing fails after the save (Splunk closed the
    event pipe), the checkpoint is moved back so the events are read again.
    """
    previous = checkpoint.saved_revision
    if not checkpoint.save():
        return
    try:
        for event in batch:
            event_writer.write_event(event)
    except Exception as e:
        # Revision 0 means "nothing written yet", so the bucket is read in full
        checkpoint.revision = previous if previous is not None else 0
        checkpoint.save()
        raise nats_connection.InputStopped(f"Splunk stopped reading events: {e}") from e
    if ingestion is not None:
        for event in batch:
            ingestion.add(event.sourceType)
    batch.clear()


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
