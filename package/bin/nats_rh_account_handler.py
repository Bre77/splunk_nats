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
REST handler for the Account configuration.

Every time an Account is created or edited, it test-connects to NATS with the
submitted settings, so wrong servers, credentials or TLS settings are rejected at
configuration time with the real reason instead of failing later in an input.
"""

import asyncio
import logging
from typing import Any, Dict

import nats_connection
from splunktaucclib.rest_handler.admin_external import AdminExternalHandler
from splunktaucclib.rest_handler.error import RestError

logger = logging.getLogger(__name__)

# What splunktaucclib sends back for an unchanged encrypted field.
_PLACEHOLDERS = ("******", "********")


def merge_account(stored: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply an edit payload on top of the stored Account. Encrypted fields that come
    back as the masked placeholder keep their stored value.
    """
    account = dict(stored)
    for field, value in payload.items():
        if field in nats_connection.ENCRYPTED_FIELDS and value in _PLACEHOLDERS:
            continue
        account[field] = value
    return account


def test_account(account: Dict[str, Any]) -> None:
    """
    Raises:
        RestError: 400 with the real reason if NATS does not accept the Account.
    """
    try:
        asyncio.run(nats_connection.check_connection(account))
    except nats_connection.NatsConnectError as e:
        raise RestError(400, f"Unable to connect to NATS with this account: {e}")


class NatsAccountHandler(AdminExternalHandler):
    def create_hook(self, session_key, config_name, stanza_id, payload):
        test_account(payload)
        super().create_hook(session_key, config_name, stanza_id, payload)

    def edit_hook(self, session_key, config_name, stanza_id, payload):
        try:
            stored = nats_connection.load_account(session_key, stanza_id)
        except nats_connection.NatsConnectError as e:
            raise RestError(400, str(e))
        test_account(merge_account(stored, payload))
        super().edit_hook(session_key, config_name, stanza_id, payload)
