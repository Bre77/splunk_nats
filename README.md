# NATS Add-on for Splunk

Stream data from [NATS](https://nats.io/) into Splunk and query NATS on demand.
The add-on is built with the [Splunk Add-on UCC Framework](https://splunk.github.io/addonfactory-ucc-generator/)
(`ucc-gen`) and ships two modular inputs and two custom search commands.

## Features

### Modular inputs

- **NATS Subscribe** (`nats_subscribe`) - subscribes to a NATS subject (wildcards
  such as `*.events` or `foo.>` are supported) and indexes each received message.
- **NATS KV** (`nats_kv`) - watches a NATS JetStream Key-Value bucket and indexes
  entries as they change. Progress is checkpointed to the Splunk KV Store (via
  `collections.conf` / `transforms.conf`) so a restart resumes from the last
  processed revision instead of re-ingesting history.

### Custom search commands

- `| natssubscribe subject=<subject> account=<account>` - connects to NATS,
  collects messages published to `subject` for a short window, and returns them
  as events.
- `| natskv account=<account> bucket=<bucket> key=<key> [domain=<domain>]` -
  returns the revision history for a key in a JetStream KV bucket.

## Configuration

1. Open the add-on's **Configuration** page and create an **Account** with the
   NATS server URL(s) (comma-separated, e.g.
   `nats://demo.nats.io:4222,tls://demo.nats.io:4443`). Username/password are
   optional; the password is stored encrypted in Splunk's credential store.
2. On the **Inputs** page, create a **NATS Subscribe** or **NATS KV** input and
   select the account to use.

## Requirements

- Splunk Enterprise 9.x or Splunk Cloud.
- Python dependencies are vendored into `lib/` at build time from
  `package/lib/requirements.txt` (`splunktaucclib`, `splunk-sdk`, `solnlib`,
  `nats-py`). Supported Splunk Python runtimes: 3.9 and 3.13.

## Building

```
./build.sh <version>
```

This runs `ucc-gen build` followed by `ucc-gen package`, producing an installable
package under `output/`.

## License

Apache License 2.0. See [LICENSES/LICENSE.txt](package/LICENSES/LICENSE.txt).
