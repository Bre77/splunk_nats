"""Account option parsing: NKeys, credentials, PEM handling, TLS settings and secrets."""

import ssl

import nats_connection
import nats_nkeys
import pytest

# Reference values produced by the official `nkeys` package (PyNaCl).
USER_SEED = "SUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU"
USER_PUBLIC = "UCK5N7N66OBOINFXAYC2ACJQYFSOD4VYNU6APEJTAVFZB2SVHLKGEW7L"
USER_SIGNATURE = (
    "752685a6a8ee910d5b2b5d225419f9ca5aab92bf95e2b19b1282452feddf119c"
    "7bb0ae90c59a97b8238d7f9d31f63999aa0caa8b23a16f9a0335cc6aea949d00"
)
ACCOUNT_SEED = "SAANJIBNEKGCRUWJCPIWUXFBFJLR36FJTFKGBGKAT7AQXH2LVFNQWZJMQU"
ACCOUNT_PUBLIC = "ADQ4BYM5KICR5OXDSP3S3WVJ5CYEORGQKT72SVRF2ZDVA7LTFKMCIPGY"


def test_nkey_matches_reference_implementation():
    key = nats_nkeys.from_seed(bytearray(USER_SEED.encode()))
    assert key.public_key.decode() == USER_PUBLIC
    assert key.sign(b"nonce").hex() == USER_SIGNATURE
    assert nats_nkeys.from_seed(ACCOUNT_SEED).public_key.decode() == ACCOUNT_PUBLIC


@pytest.mark.parametrize(
    "seed",
    [
        "",
        "not a seed",
        USER_SEED[:20] + ("A" if USER_SEED[20] != "A" else "B") + USER_SEED[21:],  # bad CRC
        USER_PUBLIC,  # a public key, not a seed
    ],
)
def test_invalid_nkey_seed_is_rejected(seed):
    with pytest.raises(nats_nkeys.ErrInvalidSeed):
        nats_nkeys.decode_seed(seed)
    if seed:
        with pytest.raises(nats_connection.NatsConnectError):
            nats_connection.build_connect_options({"nkey_seed": seed})


def test_installed_as_nkeys_for_nats_py():
    import sys

    nats_nkeys.install()
    assert "nkeys" in sys.modules
    assert hasattr(sys.modules["nkeys"], "from_seed")


def test_minimal_account_options():
    options = nats_connection.build_connect_options({"servers": "nats://a:4222, nats://b:4222"})
    assert options["servers"] == ["nats://a:4222", "nats://b:4222"]
    assert options["connect_timeout"] == nats_connection.DEFAULT_CONNECT_TIMEOUT
    assert "tls" not in options
    assert "user" not in options and "token" not in options


def test_credentials_and_nkey_seed_are_exclusive():
    with pytest.raises(nats_connection.NatsConnectError, match="not both"):
        nats_connection.build_connect_options(
            {"nkey_seed": USER_SEED, "credentials": "x"}
        )


def test_credentials_require_jwt_and_seed():
    with pytest.raises(nats_connection.NatsConnectError, match=".creds"):
        nats_connection.build_connect_options({"credentials": "just some text"})


def test_credentials_are_normalized():
    creds = (
        "-----BEGIN NATS USER JWT----- eyJ0.eyJ1.c2ln ------END NATS USER JWT------ "
        f"-----BEGIN USER NKEY SEED----- {USER_SEED} ------END USER NKEY SEED------"
    )
    normalized = nats_connection.normalize_credentials(creds)
    lines = normalized.splitlines()
    assert lines[1] == "eyJ0.eyJ1.c2ln"
    assert lines[lines.index("-----BEGIN USER NKEY SEED-----") + 1] == USER_SEED


def test_pem_is_reflowed():
    body = "A" * 130
    text = f"-----BEGIN CERTIFICATE----- {body} -----END CERTIFICATE-----"
    assert nats_connection.normalize_pem(text, "x").splitlines() == [
        "-----BEGIN CERTIFICATE-----",
        "A" * 64,
        "A" * 64,
        "AA",
        "-----END CERTIFICATE-----",
    ]
    with pytest.raises(nats_connection.NatsConnectError, match="PEM"):
        nats_connection.normalize_pem("garbage", "TLS CA certificates")


def test_tls_verify_off_disables_verification():
    context = nats_connection.build_tls_context({"tls_verify": "0"})
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False


def test_tls_defaults_leave_tls_to_server_url():
    assert nats_connection.build_tls_context({"tls_verify": "1"}) is None
    assert nats_connection.build_tls_context({}) is None


def test_tls_client_certificate_requires_key():
    with pytest.raises(nats_connection.NatsConnectError, match="together"):
        nats_connection.build_tls_context(
            {"tls_cert": "-----BEGIN CERTIFICATE-----\nAA\n-----END CERTIFICATE-----"}
        )


def test_invalid_ca_bundle_is_reported():
    with pytest.raises(nats_connection.NatsConnectError, match="CA"):
        nats_connection.build_tls_context(
            {"tls_ca": "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----"}
        )


@pytest.mark.parametrize("value", ["0", "-1", "301", "soon"])
def test_invalid_connect_timeout_is_rejected(value):
    with pytest.raises(nats_connection.NatsConnectError):
        nats_connection.connect_timeout({"connect_timeout": value})


def test_secret_fields_match_encrypted_account_fields():
    import json
    import os

    path = os.path.join(os.path.dirname(__file__), "..", "..", "globalConfig.json")
    with open(path) as f:
        config = json.load(f)
    account = config["pages"]["configuration"]["tabs"][0]
    encrypted = {e["field"] for e in account["entity"] if e.get("encrypted")}
    assert encrypted == set(nats_connection.ENCRYPTED_FIELDS)
