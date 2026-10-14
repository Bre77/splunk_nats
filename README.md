# NATS Add-on for Splunk

Stream data from [NATS](https://nats.io/) into Splunk and query NATS on demand.
The add-on is built with the [Splunk Add-on UCC Framework](https://splunk.github.io/addonfactory-ucc-generator/)
(`ucc-gen`) and ships two modular inputs and two custom search commands.

## Features

### Modular inputs

- **NATS Subscribe** (`nats_subscribe`) - subscribes to a NATS subject and indexes
  each received message. Subjects are dot-separated tokens: `*` matches exactly
  one token and `>` matches all remaining tokens, so `telemetry.*` matches
  `telemetry.a` but not `telemetry.a.data`, while `telemetry.>` matches both.
  An optional [payload decoder](#payload-decoders) turns binary payloads, such as
  protobuf, into JSON events.
- **NATS KV** (`nats_kv`) - watches a NATS JetStream Key-Value bucket and indexes
  entries as they change, timestamped with each entry's created time. Deletes and
  purges are indexed too, as JSON records with `key`, `operation` (`DEL` or
  `PURGE`) and `revision`. Progress is checkpointed to the Splunk KV Store (via
  `collections.conf` / `transforms.conf`) so a restart resumes from the last
  processed revision instead of re-ingesting history.

### Custom search commands

- `| natssubscribe subject=<subject> account=<account> [count=<int>] [duration=<int>] [decoder=<decoder>] [decoder_args=<arguments>]` -
  connects to NATS and returns the messages published to `subject` until `count`
  messages arrived (default 100, `0` for no limit) or `duration` seconds passed
  (default 10, at most 3600), whichever comes first. NATS headers are returned as
  `headers.<name>` fields. With `decoder`, payloads are decoded by a
  [payload decoder](#payload-decoders).
- `| natskv account=<account> bucket=<bucket> key=<key> [domain=<domain>]` -
  returns the revision history for a key in a JetStream KV bucket, including
  deletes and purges (each result has an `operation` field: `PUT`, `DEL` or
  `PURGE`). If JetStream does not respond, the server may use a JetStream domain:
  pass `domain=<domain>`.

### Sourcetypes

Inputs and search commands use the same sourcetypes, with the same `source`
(the subject or key), `host` (the connected NATS server's `host:port`) and
`props.conf` settings (one event per message, `_time` set by the add-on rather
than parsed from the payload):

| Sourcetype | Used for | Search-time fields |
|---|---|---|
| `nats:message` | `nats_subscribe` input and `natssubscribe` default | automatic (`key=value` and JSON) |
| `nats:json` | JSON message payloads (set it as the input's Source Type) and decoded payloads | JSON |
| `nats:kv` | `nats_kv` input and `natskv` default | JSON |

Binary (non UTF-8) payloads are stored base64-encoded.

## Configuration

1. Open the add-on's **Configuration** page and create an **Account** with the
   NATS server URL(s) (comma-separated, e.g.
   `nats://demo.nats.io:4222,tls://demo.nats.io:4443`) and how to authenticate:
   - **Username / Password**, **Token**, **NKey Seed** (`SU...`), or
     **Credentials File** (the full content of a `.creds` file, for decentralized
     JWT authentication). Use either an NKey seed or a credentials file, not both.
   - **TLS**: CA certificates (PEM; empty uses the system trust store), a client
     certificate and unencrypted key for mutual TLS, **Verify Server Certificate**
     (on by default) and **TLS Handshake First** for servers configured with
     `handshake_first`. TLS is used for `tls://` URLs, when any TLS option is set,
     or when the server requires it.
   - **Connect Timeout** (seconds per server, default 10).

   Passwords, tokens, NKey seeds, credentials files and TLS client keys are stored
   encrypted in Splunk's credential store. Saving an Account test-connects to NATS,
   so a wrong server, credential or TLS setting is rejected with the reason
   (for example `Authorization violation`, `Connection refused` or a TLS error).
2. On the **Inputs** page, create a **NATS Subscribe** or **NATS KV** input and
   select the account to use.
3. Optionally, set a [payload decoder](#payload-decoders) on a NATS Subscribe input.

## Payload decoders

Without a decoder, a payload is indexed as-is when it is UTF-8 text and as base64
otherwise, so binary formats such as protobuf, MessagePack or compressed data arrive
with no fields. A decoder is an executable you provide that turns each payload into
an event, normally JSON. It is written in any language and brings its own
libraries: the add-on itself ships none.

### Setting up a decoder

1. Place the executable in `$SPLUNK_HOME/etc/apps/nats/local/decoders/`, readable
   and executable by the user Splunk runs as. Splunk keeps the add-on's `local`
   directory when the add-on is upgraded. Only executables in this directory can be
   used: a decoder is set by its file name, never by a path.
2. On a NATS Subscribe input, open **Payload decoder** and set **Decoder** to the file
   name. **Decoder Arguments** are passed to it, quoted like a shell command line
   (no shell is used). **Decoded Source Type** (default `nats:json`) is the source
   type of decoded events.
3. To try a decoder from search, pass it to `natssubscribe` and extract the fields
   with `spath` (Splunk does not extract fields from a generating command's results):

   ```
   | natssubscribe subject="telemetry.>" account=my_account decoder=tesla | spath
   ```

Each input, and each `natssubscribe` search, starts the decoder once and keeps it
running. It runs as the Splunk user, in the `local/decoders` directory, with
Splunk's environment except the variables Splunk sets for its own Python, OpenSSL and
libraries (`LD_LIBRARY_PATH`, `PYTHONPATH`, `PYTHONHOME`, `OPENSSL_CONF` and similar),
so a system or virtualenv interpreter works as it does from a shell.

### Decoder protocol

The add-on and the decoder exchange one line per message:

1. The add-on writes a JSON object and a newline to the decoder's stdin:

   ```
   {"subject":"telemetry.<VIN>.data","headers":{"traceparent":"00-..."},"payload":"Cg0IBBIJKQAAAAAAAPA/..."}
   ```

   `payload` is the message payload in base64, and `headers` holds the NATS message
   headers (`{}` when there are none).
2. The decoder writes the event and a newline to stdout, then flushes. The event is
   normally a JSON object, which must fit on one line. An empty line means the
   payload cannot be decoded.
3. When its stdin closes, the decoder exits.

Anything the decoder writes to stderr is diagnostic: the latest line is added to the
error the add-on records. The request is JSON because a decoder already writes JSON
events, so reading it needs nothing more, and fields can be added later without
breaking existing decoders.

### When decoding fails

A payload is never dropped. When the decoder returns an empty line, exits, does not
answer within 10 seconds, writes a line that is not UTF-8, or cannot be started, the
payload is indexed as base64 with the input's own **Source Type** (`nats:message` by
default), as it would be without a decoder. The error is logged to `splunkd.log`, the
first at once and then at most one summary a minute:

```
index=_internal sourcetype=splunkd "Decoder 'tesla' failed to decode"
```

`natssubscribe` adds the error to each such result in a `decode_error` field, and
fails with the reason when the decoder setting is invalid or the executable is
missing.

A decoder that exits or stops answering is stopped and started again for the next
message, at most once a second; messages in between keep their base64 payload. An
input whose decoder is not in place yet keeps indexing payloads as base64 and starts
using the decoder as soon as it exists.

Decoded events can be larger than Splunk's default 10,000-byte limit (Tesla alerts
reach 16 KB); the `nats:json` source type allows 2 MiB. Give a custom **Decoded
Source Type** the same `props.conf` settings as `nats:json`.

### Throughput

Keeping one decoder running is what makes decoding keep up with busy subjects.
Measured on Splunk 10.4.1 with the Tesla example below and live `telemetry.>`
traffic (about 125 small protobuf messages a second):

| | per message | messages a second |
|---|---|---|
| Starting a fresh decoder process for each message | 34 ms | about 29, too slow for this stream |
| One running decoder process (what the add-on does) | 0.05 ms (p99 0.3 ms) | about 19,000 |

The input decodes messages one at a time in arrival order, so a decoder that takes
longer per message than messages arrive falls behind, and NATS eventually drops
messages for a consumer that is too slow.

### Splunk Cloud Platform

Splunk Cloud Platform does not let you place executables on its instances, so
decoders cannot run there. Run inputs that need a decoder on a heavy forwarder you
manage and forward the decoded events, or leave **Decoder** empty to index payloads
as base64.

### Example: Tesla Fleet Telemetry

[Tesla Fleet Telemetry](https://github.com/teslamotors/fleet-telemetry) publishes
protobuf on subjects such as `telemetry.<VIN>.data`, `.alerts`, `.errors` and
`.connectivity`, each with its own message type (see `telemetry/record.go` in that
repository). This example decoder is not part of the add-on. It keeps its own
Python environment, outside Splunk, with the protobuf modules the repository
publishes:

```
sudo mkdir -p /opt/nats-decoders/tesla && cd /opt/nats-decoders/tesla
sudo git clone --depth 1 https://github.com/teslamotors/fleet-telemetry.git
sudo python3 -m venv venv
sudo venv/bin/pip install protobuf
```

Save this as `$SPLUNK_HOME/etc/apps/nats/local/decoders/tesla`, owned by the Splunk
user, and make it executable (`chmod 755`):

```python
#!/opt/nats-decoders/tesla/venv/bin/python
"""Tesla Fleet Telemetry decoder for the NATS Add-on for Splunk."""
import base64
import json
import sys

sys.path.insert(0, "/opt/nats-decoders/tesla/fleet-telemetry/protos/python")

from google.protobuf.json_format import MessageToDict
import vehicle_alert_pb2, vehicle_connectivity_pb2, vehicle_data_pb2, vehicle_error_pb2

# The last subject token names the record type: telemetry.<VIN>.data and so on
MESSAGES = {
    "data": vehicle_data_pb2.Payload,
    "alerts": vehicle_alert_pb2.VehicleAlerts,
    "errors": vehicle_error_pb2.VehicleErrors,
    "connectivity": vehicle_connectivity_pb2.VehicleConnectivity,
}


def decode(subject, payload):
    message = MESSAGES[subject.rsplit(".", 1)[-1]].FromString(payload)
    event = MessageToDict(message, preserving_proto_field_name=True)
    # One field per signal ("VehicleSpeed": 42.0) instead of a list of key/value
    # pairs. Signals the vehicle marks invalid are left out.
    for datum in event.pop("data", []):
        value = datum.get("value", {})
        if "invalid" not in value:
            event[datum["key"]] = next(iter(value.values()), None)
    return json.dumps(event)


for line in sys.stdin:
    request = json.loads(line)
    try:
        event = decode(request["subject"], base64.b64decode(request["payload"]))
    except Exception as e:
        print(f"{type(e).__name__}: {e}", file=sys.stderr, flush=True)
        event = ""  # an empty line keeps this payload as base64
    print(event, flush=True)
```

Then create a NATS Subscribe input on `telemetry.>` with **Decoder** `tesla`. A
vehicle data event has one field per signal:

```
{"created_at": "2026-10-14T22:05:44.515Z", "vin": "...", "VehicleSpeed": 28.6, "Gear": "ShiftStateD", "BatteryLevel": 76.5, "Location": {"latitude": ..., "longitude": ...}}
```

so searches such as these work directly:

```
index=nats source="*.data" VehicleSpeed=* | timechart avg(VehicleSpeed) by vin
index=nats source="*.connectivity" | stats count by status network_interface
index=nats source="*.alerts" | stats count by alerts{}.name
```

Inputs keep running through connection problems: when the connection drops or
cannot be made they retry forever with capped exponential backoff and jitter
(up to 20 seconds between attempts), re-reading the Account before every attempt
so a corrected Account takes effect without restarting Splunk. Each disconnect,
reconnect and new failure reason is logged once. Messages published while an
input is disconnected are not received (core NATS has no replay). The search
commands make one connection attempt bounded by the connect timeout and fail with
the real cause.

## Requirements

- Splunk Enterprise 9.x or Splunk Cloud.
- Python dependencies are vendored into `lib/` at build time from
  `package/lib/requirements.txt` (`splunktaucclib`, `splunk-sdk`, `solnlib`,
  `nats-py`, `ecdsa`). NKey signing uses the pure-Python `ecdsa` package rather
  than `nkeys`/PyNaCl, whose native libraries would make the add-on
  platform-specific. Supported Splunk Python runtimes: 3.9 and 3.13.

## Building

```
./build.sh <version>
```

This runs `ucc-gen build` followed by `ucc-gen package`, producing an installable
package under `output/`.

## Testing

- `tests/unit` (pytest) runs the add-on code from `package/bin` against the
  dependencies in `tests/requirements.txt`. The KV and connection tests start
  throwaway `nats-server` processes, so put `nats-server` on `PATH` (the connection
  tests also accept `NATS_SERVER_BIN`); the connection tests need `openssl` to
  create a test CA. They skip without `nats-server`, except in CI where they fail.
- `tests/package` (stdlib unittest) imports every module of the built add-on in
  `output/nats` with its vendored `lib/`, so build first and run it on each
  supported runtime: `python3.9 -m unittest discover -s tests/package` (and 3.13).
- `tests/e2e/search_commands_python.sh <package.tar.gz> <python-version> [image]`
  pins the search commands to that Splunk Python in a disposable container,
  imports every `bin/` module there and runs `natssubscribe` and `natskv`.
- `tests/e2e/decoder_e2e.sh <package.tar.gz> [image]` runs a payload decoder in a
  disposable Splunk container next to a NATS server, and checks that decodable
  payloads are indexed as JSON and undecodable ones as base64 with the failure
  logged.

## Continuous integration & publishing

CI is **credential-free** and validation-only; publishing is a **separate, manual**
step so Splunkbase credentials never live in GitHub secrets.

- **CI - `Validate`** (`.github/workflows/validate.yml`) runs on every PR and push
  to `main`: `ucc-gen` build + package + **local AppInspect** (no credentials, no
  publish). It calls the reusable workflow
  `.github/workflows/_reusable-build-appinspect.yml`, which other add-ons can reuse
  via `uses:` (see the header comment in that file).
- **Publish - `bin/publish-splunkbase.sh`** is run by a human on demand, never by CI.
  It pulls Splunkbase credentials from 1Password at runtime (`op item get`), builds
  the package, and uploads a new release to Splunkbase. It refuses until the add-on
  has a Splunkbase app id:

  ```
  bin/publish-splunkbase.sh --app-id <id> [--version x.y.z] [--public] [--dry-run]
  ```

A brand-new add-on has no Splunkbase app id yet: create the initial listing once via
the Splunkbase web UI, then pass `--app-id` (or export `SPLUNKBASE_APP_ID`) so
subsequent releases upload via the API.

## License

Apache License 2.0. See [LICENSES/LICENSE.txt](package/LICENSES/LICENSE.txt).
