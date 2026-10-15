"""
F5 / F7 regression tests: one bounded connection attempt that fails with the real
cause, and every authentication and TLS option of an Account, against a real
throwaway nats-server.
"""

import asyncio
import socket
import time

import nats_connection
import nats_nkeys
import natsutil
import pytest

PASSWORD_AUTH = 'authorization { users: [ {user: "splunk", password: "s3cret", permissions: {subscribe: {allow: ["telemetry.>", "_INBOX.>"]}}} ] }'


def check(account):
    asyncio.run(nats_connection.check_connection(account))


def connect_error(account) -> "tuple[str, float]":
    started = time.monotonic()
    with pytest.raises(nats_connection.NatsConnectError) as excinfo:
        check(account)
    return str(excinfo.value), time.monotonic() - started


def test_password_auth_succeeds(start_nats_server):
    server = start_nats_server(PASSWORD_AUTH)
    check({"servers": server.url, "username": "splunk", "password": "s3cret"})


def test_wrong_password_reports_authorization_violation_fast(start_nats_server):
    # Before the fix this took about 120 s and said "no servers available for connection".
    server = start_nats_server(PASSWORD_AUTH)
    message, elapsed = connect_error(
        {"servers": server.url, "username": "splunk", "password": "wrong"}
    )
    assert "Authorization violation" in message
    assert server.url in message
    assert elapsed < 5


def test_connection_refused_is_reported_fast():
    port = natsutil.free_port()
    message, elapsed = connect_error({"servers": f"nats://127.0.0.1:{port}"})
    assert "Connection refused" in message
    assert elapsed < 5


def test_unresponsive_server_times_out_at_connect_timeout():
    # A socket that accepts TCP connections but never speaks NATS.
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)
    try:
        port = listener.getsockname()[1]
        message, elapsed = connect_error(
            {"servers": f"nats://127.0.0.1:{port}", "connect_timeout": "2"}
        )
    finally:
        listener.close()
    assert "Timed out" in message
    assert elapsed < 5


def test_every_server_is_tried_and_last_real_cause_reported(start_nats_server):
    server = start_nats_server(PASSWORD_AUTH)
    dead = f"nats://127.0.0.1:{natsutil.free_port()}"
    # A dead server first does not hide a working one.
    check({"servers": f"{dead},{server.url}", "username": "splunk", "password": "s3cret"})


def test_permissions_violation_on_subject_is_reported(start_nats_server):
    # Before the fix a subscription the user may not make returned 0 results, no error.
    server = start_nats_server(PASSWORD_AUTH)

    async def run():
        connection = await nats_connection.connect(
            {"servers": server.url, "username": "splunk", "password": "s3cret"}
        )
        try:
            await connection.nc.subscribe("$SYS.>")
            await connection.confirm()
        finally:
            await connection.close()

    with pytest.raises(nats_connection.NatsConnectError) as excinfo:
        asyncio.run(run())
    assert "Permissions violation" in str(excinfo.value)
    assert "$sys.>" in str(excinfo.value).lower()


def test_token_auth(start_nats_server):
    server = start_nats_server('authorization { token: "t0ken" }')
    check({"servers": server.url, "token": "t0ken"})
    message, _ = connect_error({"servers": server.url, "token": "nope"})
    assert "Authorization violation" in message


def test_nkey_seed_auth(start_nats_server):
    seed, public = natsutil.new_nkey(nats_nkeys.PREFIX_BYTE_USER)
    server = start_nats_server(f"authorization {{ users: [ {{nkey: {public}}} ] }}")
    check({"servers": server.url, "nkey_seed": seed})
    other_seed, _ = natsutil.new_nkey(nats_nkeys.PREFIX_BYTE_USER)
    message, _ = connect_error({"servers": server.url, "nkey_seed": other_seed})
    assert "Authorization violation" in message


def test_credentials_file_auth(start_nats_server):
    config, creds = natsutil.operator_setup()
    server = start_nats_server(config)
    check({"servers": server.url, "credentials": creds})
    # Whitespace mangled the way a single-line field or .conf storage might.
    check({"servers": server.url, "credentials": " ".join(creds.split())})


def test_credentials_with_unknown_account_are_rejected(start_nats_server):
    config, _ = natsutil.operator_setup()
    _, other_creds = natsutil.operator_setup()
    server = start_nats_server(config)
    message, _ = connect_error({"servers": server.url, "credentials": other_creds})
    assert "Authorization violation" in message


@pytest.fixture
def certs(tmp_path):
    return natsutil.make_certs(tmp_path)


def tls_server(start_nats_server, certs, verify=False, handshake_first=False):
    tls = (
        f'tls {{ cert_file: "{certs["server.pem"]}", key_file: "{certs["server.key"]}", '
        f'ca_file: "{certs["ca.pem"]}", verify: {str(verify).lower()}'
        f'{", handshake_first: true" if handshake_first else ""} }}'
    )
    return start_nats_server(tls)


def test_tls_with_private_ca(start_nats_server, certs):
    server = tls_server(start_nats_server, certs)
    check({"servers": f"tls://127.0.0.1:{server.port}", "tls_ca": natsutil.read(certs["ca.pem"])})


def test_tls_untrusted_server_certificate_is_reported(start_nats_server, certs):
    server = tls_server(start_nats_server, certs)
    message, elapsed = connect_error({"servers": f"tls://127.0.0.1:{server.port}"})
    assert "TLS certificate verification failed" in message
    assert elapsed < 5


def test_tls_verify_off_accepts_untrusted_server(start_nats_server, certs):
    server = tls_server(start_nats_server, certs)
    check({"servers": f"tls://127.0.0.1:{server.port}", "tls_verify": "0"})


def test_mutual_tls(start_nats_server, certs):
    server = tls_server(start_nats_server, certs, verify=True)
    base = {"servers": f"tls://127.0.0.1:{server.port}", "tls_ca": natsutil.read(certs["ca.pem"])}

    # Depending on timing the client sees the server's certificate_required alert
    # or only the connection closing right after the handshake; both say why.
    message, elapsed = connect_error(base)
    assert "client certificate" in message
    assert elapsed < 5

    check(
        dict(
            base,
            tls_cert=natsutil.read(certs["client.pem"]),
            tls_key=natsutil.read(certs["client.key"]),
        )
    )


def test_tls_handshake_first(start_nats_server, certs):
    server = tls_server(start_nats_server, certs, handshake_first=True)
    base = {"servers": f"tls://127.0.0.1:{server.port}", "tls_ca": natsutil.read(certs["ca.pem"])}
    check(dict(base, tls_handshake_first="1"))
