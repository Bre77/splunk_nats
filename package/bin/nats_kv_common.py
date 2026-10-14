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
Shared helpers for the NATS JetStream Key-Value input and the natskv command:
reading entries from a bucket's stream, turning them into Splunk event payloads,
and turning nats-py errors into messages a Splunk user can act on.
"""

import base64
import datetime
import json
import time
from typing import Any, List, Optional

from nats.js import api
from nats.js.errors import (
    BucketNotFoundError,
    InvalidBucketNameError,
    ServiceUnavailableError,
)
from nats.js.kv import KeyValue

KV_OP_PUT = "PUT"
KV_OP_DEL = "DEL"
KV_OP_PURGE = "PURGE"

# KV protocol headers (ADR-8) and the nats-server 2.11+ marker header for TTL/age expiry
KV_OPERATION_HEADER = "KV-Operation"
KV_MARKER_REASON_HEADER = "Nats-Marker-Reason"
MARKER_REASON_OPERATIONS = {
    "MaxAge": KV_OP_PURGE,
    "Purge": KV_OP_PURGE,
    "Remove": KV_OP_DEL,
}


def kv_stream(bucket: str) -> str:
    """Return the name of the stream that stores a KV bucket."""
    return f"KV_{bucket}"


def kv_subject_prefix(bucket: str) -> str:
    """Return the subject prefix of a KV bucket's keys."""
    return f"$KV.{bucket}."


def entry_from_msg(bucket: str, msg: Any) -> Optional[KeyValue.Entry]:
    """
    Convert a message from a bucket's stream into a KV entry, the way nats-py's
    KV watcher does. Returns None for server markers with an unknown reason.
    """
    headers = msg.headers or {}
    operation = headers.get(KV_OPERATION_HEADER)
    if operation is None and KV_MARKER_REASON_HEADER in headers:
        operation = MARKER_REASON_OPERATIONS.get(headers[KV_MARKER_REASON_HEADER])
        if operation is None:
            return None
    metadata = msg.metadata
    return KeyValue.Entry(
        bucket=bucket,
        key=msg.subject[len(kv_subject_prefix(bucket)) :],
        value=msg.data,
        revision=metadata.sequence.stream,
        delta=metadata.num_pending,
        created=metadata.timestamp,
        operation=operation,
    )


async def read_history(
    js: Any, bucket: str, keys: str, timeout: float = 5.0
) -> List[KeyValue.Entry]:
    """
    Return every entry stored in a bucket for the keys matching `keys` (NATS
    wildcards allowed), oldest first, including delete and purge markers.

    nats-py's KeyValue.history() can return nothing for a non-empty bucket: its
    watcher treats "the consumer has nothing pending" as the end of the history,
    even when the server has already sent every entry and the client has not
    processed any of them yet. This reads the bucket's stream directly and waits
    for as many entries as the consumer reports delivered plus pending.
    """
    sub = await js.subscribe(
        f"{kv_subject_prefix(bucket)}{keys}",
        stream=kv_stream(bucket),
        ordered_consumer=True,
        deliver_policy=api.DeliverPolicy.ALL,
    )
    try:
        info = await sub.consumer_info()
        expected = info.delivered.consumer_seq + info.num_pending
        entries = []
        for _ in range(expected):
            msg = await sub.next_msg(timeout=timeout)
            entry = entry_from_msg(bucket, msg)
            if entry is not None:
                entries.append(entry)
            if msg.metadata.num_pending == 0:
                break
        return entries
    finally:
        await sub.unsubscribe()


def entry_operation(entry: Any) -> str:
    """Return the entry's KV operation; nats-py leaves it None for a put."""
    return entry.operation or KV_OP_PUT


def entry_raw(entry: Any) -> str:
    """
    Return the event payload for a KV entry.

    A put carries its value (UTF-8 text, or base64 for binary values). Delete and
    purge markers have no value, so they are written as a small JSON record that
    names the key, the operation and the revision.
    """
    operation = entry_operation(entry)
    if operation == KV_OP_PUT and entry.value:
        try:
            return entry.value.decode("utf-8")
        except UnicodeDecodeError:
            return base64.b64encode(entry.value).decode("ascii")
    return json.dumps(
        {"key": entry.key, "operation": operation, "revision": entry.revision}
    )


def entry_time(entry: Any) -> float:
    """Return the entry's created time as epoch seconds (now if unknown)."""
    created = entry.created
    if isinstance(created, datetime.datetime):
        return created.timestamp()
    if isinstance(created, (int, float)):
        return float(created)
    return time.time()


def kv_error_message(
    err: Exception, bucket: str, domain: Optional[str], domain_option: str
) -> str:
    """
    Describe a nats-py JetStream KV error in terms of the user's settings.

    Args:
        err: The exception raised by nats-py
        bucket: The KV bucket name that was requested
        domain: The JetStream domain that was used, if any
        domain_option: How the user sets the domain, e.g. "domain=<domain>"
    """
    if isinstance(err, InvalidBucketNameError):
        return (
            f"Invalid KV bucket name '{bucket}': use only letters, digits, "
            "hyphens and underscores."
        )
    if isinstance(err, BucketNotFoundError):
        if domain:
            return (
                f"KV bucket '{bucket}' was not found in JetStream domain '{domain}'. "
                "Check the bucket name and the domain."
            )
        return (
            f"KV bucket '{bucket}' was not found. Check the bucket name. If the "
            f"bucket is in a JetStream domain, set {domain_option}."
        )
    if isinstance(err, ServiceUnavailableError):
        if domain:
            return (
                f"JetStream did not respond in domain '{domain}'. Check that the "
                "domain is correct and that the account has JetStream access."
            )
        return (
            "JetStream did not respond. If the server uses a JetStream domain, "
            f"set {domain_option}. Otherwise check that the account has "
            "JetStream access."
        )
    return str(err)
