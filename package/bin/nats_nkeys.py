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
Pure-Python NATS NKey support.

nats-py signs NKey and credentials-file (JWT) challenges through the optional
`nkeys` package, which depends on PyNaCl. PyNaCl ships platform-specific native
libraries, which would make this add-on platform-dependent and fail Splunk Cloud
AArch64 vetting. This module implements the small part of the `nkeys` API that
nats-py uses (`from_seed()` returning a key pair with `public_key`, `sign()` and
`wipe()`) on top of the pure-Python `ecdsa` package, and `install()` registers
it as `nkeys` when the real package is not available.
"""

import base64
import binascii
import sys
from typing import Tuple, Union

from ecdsa import SigningKey
from ecdsa.curves import Ed25519

PREFIX_BYTE_SEED = 18 << 3
PREFIX_BYTE_PRIVATE = 15 << 3
PREFIX_BYTE_SERVER = 13 << 3
PREFIX_BYTE_CLUSTER = 2 << 3
PREFIX_BYTE_OPERATOR = 14 << 3
PREFIX_BYTE_ACCOUNT = 0
PREFIX_BYTE_USER = 20 << 3

_PUBLIC_PREFIXES = (
    PREFIX_BYTE_SERVER,
    PREFIX_BYTE_CLUSTER,
    PREFIX_BYTE_OPERATOR,
    PREFIX_BYTE_ACCOUNT,
    PREFIX_BYTE_USER,
)


class ErrInvalidSeed(Exception):
    def __str__(self) -> str:
        return "nkeys: invalid seed"


def _crc16(data: bytes) -> int:
    """CRC-16/XMODEM, as used by the NKey encoding."""
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def _b32decode(value: bytes) -> bytes:
    padding = (-len(value)) % 8
    return base64.b32decode(value + b"=" * padding)


def _b32encode(value: bytes) -> bytes:
    return base64.b32encode(value).rstrip(b"=")


def decode_seed(seed: Union[bytes, bytearray, str]) -> Tuple[int, bytes]:
    """
    Decode an encoded NKey seed (for example `SU...`).

    Returns:
        The public-key prefix byte and the raw 32-byte Ed25519 seed.

    Raises:
        ErrInvalidSeed: If the value is not a valid NKey seed.
    """
    if isinstance(seed, str):
        seed = seed.encode()
    try:
        raw = _b32decode(bytes(seed).strip())
    except (binascii.Error, ValueError):
        raise ErrInvalidSeed()
    if len(raw) != 36:
        raise ErrInvalidSeed()
    payload, checksum = raw[:-2], raw[-2:]
    if _crc16(payload) != int.from_bytes(checksum, "little"):
        raise ErrInvalidSeed()
    if payload[0] & 0xF8 != PREFIX_BYTE_SEED:
        raise ErrInvalidSeed()
    prefix = ((payload[0] & 0x07) << 5) | ((payload[1] & 0xF8) >> 3)
    if prefix not in _PUBLIC_PREFIXES:
        raise ErrInvalidSeed()
    return prefix, bytes(payload[2:])


class KeyPair:
    """An NKey key pair compatible with the `nkeys.KeyPair` API used by nats-py."""

    def __init__(self, seed: Union[bytes, bytearray, str]):
        prefix, raw_seed = decode_seed(seed)
        self._prefix = prefix
        self._signing_key = SigningKey.from_string(raw_seed, curve=Ed25519)

    @property
    def public_key(self) -> bytes:
        payload = bytes([self._prefix]) + self._signing_key.get_verifying_key().to_string()
        return _b32encode(payload + _crc16(payload).to_bytes(2, "little"))

    def sign(self, data: bytes) -> bytes:
        return self._signing_key.sign(data)

    def wipe(self) -> None:
        self._signing_key = None


def from_seed(seed: Union[bytes, bytearray, str]) -> KeyPair:
    return KeyPair(seed)


def install() -> None:
    """Make `import nkeys` resolve to this module unless the real package is installed."""
    try:
        import nkeys  # noqa: F401
    except ImportError:
        sys.modules["nkeys"] = sys.modules[__name__]
