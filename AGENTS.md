# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- This is a `ucc-gen` add-on. Source of truth is `globalConfig.json` (UI, inputs, custom search commands) plus `package/`. Build with `./build.sh <version>` (runs `ucc-gen build` then `ucc-gen package`); generated output lands in `output/nats/` and `nats-<version>.tar.gz` (both gitignored).
- Extension points: two modular inputs (`nats_subscribe`, `nats_kv`) with helpers in `package/bin/*_helper.py`, and two generating custom search commands (`natssubscribe`, `natskv`) whose logic lives in `package/bin/nats_*_command.py`. UCC generates the search-command dispatch wrappers (`bin/natssubscribe.py`, `bin/natskv.py`) at build time - do not commit them to `package/bin/`.
- `ucc-gen build` shells out to whichever `python3` is first on PATH to pip-install `package/lib/requirements.txt`. On Debian the system python3 is externally-managed and pip fails; build from a venv with the venv's `bin` on PATH.
- `package/lib/requirements.txt` caps `splunk-sdk>=2.1.1,<3` (maintained standard) and `globalConfig.json` `meta.supportedPythonVersion` declares `["3.9","3.13"]`.
- `package/lib/exclude.txt` drops `solnlib`'s optional OpenTelemetry chain (grpcio/protobuf/opentelemetry-*). Those ship x86_64-only native `.so` files that fail Splunk Cloud AArch64 vetting; the add-on never imports `solnlib.observability`, so excluding them is safe and keeps the package platform-independent.
- Splunkbase readiness: `splunk-appinspect inspect nats-<version>.tar.gz --mode precert` passes with 0 failures; remaining warnings are vendored-library / framework-level (TLS validation in solnlib/splunktaucclib REST, SplunkJS telemetry notices).
- CI/publish split: `.github/workflows/validate.yml` (via reusable `_reusable-build-appinspect.yml`, using `VatsalJagani/splunk-app-action`) runs credential-free build + local AppInspect on every push/PR - no Splunkbase secrets in GitHub. Publishing is manual: `bin/publish-splunkbase.sh` pulls creds from 1Password (`op item get Splunkbase --vault CLI`) at runtime and POSTs to Splunkbase `app/<id>/new_release/`. It refuses until an app id exists (nats has no Splunkbase listing yet; the initial listing must be created in the web UI first).

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
