#!/usr/bin/env bash
#
# publish-splunkbase.sh - human-triggered Splunkbase release upload for this add-on.
#
# This is NOT run by CI. CI (.github/workflows) only builds + AppInspects; publishing
# is deliberately a separate, manual step so Splunkbase credentials never live in
# GitHub secrets. Credentials are pulled from 1Password at runtime (op item get) into
# local shell variables, used for the single API call, and discarded - never echoed,
# never written to disk, never placed in argv (curl reads them from a --config fd).
#
# Requires: a signed-in `op` (1Password CLI) session or service-account token, `curl`,
# and a package built by ./build.sh. Works unchanged wherever `op` is configured.
#
# Splunkbase rate limit: no more than 20 new_release POSTs per app per hour.
set -euo pipefail

# ---- config (overridable via env) -------------------------------------------------
OP_ITEM="${OP_ITEM:-Splunkbase}"      # 1Password item holding the Splunkbase login
OP_VAULT="${OP_VAULT:-CLI}"           # 1Password vault name
APP_NAME="nats"                       # ucc-gen add-on name -> nats-<version>.tar.gz

# ---- defaults / args --------------------------------------------------------------
APP_ID="${SPLUNKBASE_APP_ID:-}"
VERSION=""
SPLUNK_VERSIONS="9.1,9.2,9.3,9.4"
CIM_VERSIONS=""
VISIBILITY="false"                    # false = private/unlisted, true = public
DRY_RUN="false"
ASSUME_YES="false"

usage() {
  cat >&2 <<'EOF'
Usage: bin/publish-splunkbase.sh [options]

  --app-id <id>            Splunkbase numeric app id (or set SPLUNKBASE_APP_ID)
  --version <x.y.z>        Version to publish (default: meta.version in globalConfig.json)
  --splunk-versions <csv>  Supported Splunk versions (default: 9.1,9.2,9.3,9.4)
  --cim-versions <csv>     Supported CIM versions (default: none)
  --public                 Publish publicly (visibility=true). Default is private.
  --private                Publish privately/unlisted (visibility=false). Default.
  --dry-run                Build/resolve everything and print the request, but do NOT upload.
  --yes                    Skip the interactive confirmation prompt.
  -h, --help               Show this help.

Credentials come from 1Password: `op item get "$OP_ITEM" --vault "$OP_VAULT"`
(override with OP_ITEM / OP_VAULT env vars).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --app-id)          APP_ID="$2"; shift 2 ;;
    --version)         VERSION="$2"; shift 2 ;;
    --splunk-versions) SPLUNK_VERSIONS="$2"; shift 2 ;;
    --cim-versions)    CIM_VERSIONS="$2"; shift 2 ;;
    --public)          VISIBILITY="true"; shift ;;
    --private)         VISIBILITY="false"; shift ;;
    --dry-run)         DRY_RUN="true"; shift ;;
    --yes)             ASSUME_YES="true"; shift ;;
    -h|--help)         usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

cd "$(dirname "$0")/.."

# ---- first-listing guard ----------------------------------------------------------
# A brand-new add-on has no Splunkbase app id. The initial listing must be created
# once in the Splunkbase web UI; only then can releases be uploaded via the API.
if [ -z "$APP_ID" ] || [ "$APP_ID" = "0" ]; then
  cat >&2 <<EOF
No Splunkbase app id set - refusing to publish.

$APP_NAME has no Splunkbase listing yet. Create the initial listing once via the
Splunkbase web UI (https://splunkbase.splunk.com), then re-run with the numeric
app id, e.g.:

  bin/publish-splunkbase.sh --app-id 1234

or export SPLUNKBASE_APP_ID=1234 first. Subsequent releases upload automatically.
EOF
  exit 2
fi

# ---- resolve version + package ----------------------------------------------------
if [ -z "$VERSION" ]; then
  VERSION="$(python3 -c 'import json;print(json.load(open("globalConfig.json"))["meta"]["version"])')"
fi
PKG="${APP_NAME}-${VERSION}.tar.gz"

if [ ! -f "$PKG" ]; then
  echo "Package $PKG not found - building via ./build.sh $VERSION"
  ./build.sh "$VERSION"
fi
[ -f "$PKG" ] || { echo "Build did not produce $PKG" >&2; exit 1; }

ENDPOINT="https://splunkbase.splunk.com/api/v1/app/${APP_ID}/new_release/"

echo "About to upload:"
echo "  package         : $PKG"
echo "  splunkbase app  : $APP_ID"
echo "  splunk_versions : $SPLUNK_VERSIONS"
echo "  cim_versions    : ${CIM_VERSIONS:-<none>}"
echo "  visibility      : $VISIBILITY (true=public, false=private)"
echo "  endpoint        : $ENDPOINT"

if [ "$DRY_RUN" = "true" ]; then
  echo "[dry-run] Skipping 1Password fetch and upload."
  exit 0
fi

if [ "$ASSUME_YES" != "true" ]; then
  read -r -p "Type the version ($VERSION) to confirm upload: " confirm
  [ "$confirm" = "$VERSION" ] || { echo "Confirmation mismatch - aborting."; exit 1; }
fi

# ---- fetch credentials from 1Password (runtime only, never persisted) -------------
command -v op >/dev/null || { echo "1Password CLI 'op' not found on PATH." >&2; exit 1; }
SB_USER="$(op item get "$OP_ITEM" --vault "$OP_VAULT" --fields label=username --reveal)"
SB_PASS="$(op item get "$OP_ITEM" --vault "$OP_VAULT" --fields label=password --reveal)"
if [ -z "$SB_USER" ] || [ -z "$SB_PASS" ]; then
  echo "Could not read Splunkbase credentials from 1Password item '$OP_ITEM'." >&2
  exit 1
fi

# Pass credentials via a curl --config fd (process substitution), never via argv or a
# file on disk, so they can't leak through `ps` or a stray temp file.
cim_arg=()
[ -n "$CIM_VERSIONS" ] && cim_arg=(-F "cim_versions=${CIM_VERSIONS}")

set +e
out="$(curl -sS --fail-with-body \
  --config <(printf 'user = "%s:%s"\n' "$SB_USER" "$SB_PASS") \
  --request POST "$ENDPOINT" \
  -F "files[]=@${PKG}" \
  -F "filename=$(basename "$PKG")" \
  -F "splunk_versions=${SPLUNK_VERSIONS}" \
  "${cim_arg[@]}" \
  -F "visibility=${VISIBILITY}" \
  -w $'\n%{http_code}')"
rc=$?
set -e
unset SB_USER SB_PASS

http_code="${out##*$'\n'}"   # last line
body="${out%$'\n'*}"         # everything before it
if [ "$rc" -ne 0 ]; then
  echo "Upload failed (curl rc=$rc, HTTP ${http_code}):" >&2
  echo "$body" >&2
  exit 1
fi
echo "Splunkbase responded HTTP ${http_code}"
echo "$body"
