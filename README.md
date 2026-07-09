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
