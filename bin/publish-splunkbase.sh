#!/usr/bin/env bash
#
# publish-splunkbase.sh - human-triggered Splunkbase release upload. This is the
# fleet publish tool: it publishes any add-on's pre-built package, and also keeps
# the splunk_nats build-and-publish default working as-is.
#
# Two modes:
#   (1) general - pass --package <file.spl|.tar.gz> to upload an already-built
#       package as-is. No build runs, no repo layout is assumed; the package path
#       is resolved independently of where this script lives.
#   (2) nats default - no --package: build via ./build.sh and upload
#       nats-<version>.tar.gz, exactly as before.
#
# This is NOT run by CI. CI (.github/workflows) only builds + AppInspects; publishing
# is deliberately a separate, manual step so Splunkbase credentials never live in
# GitHub secrets. Credentials are pulled from 1Password at runtime (op item get) into
# local shell variables, used for the single API call, and discarded - never echoed,
# never written to disk, never placed in argv (curl reads them from a --config fd).
#
# Requires: a signed-in `op` (1Password CLI) session or service-account token, `curl`,
# and either --package or a package built by ./build.sh. Works unchanged wherever
# `op` is configured.
#
# Splunkbase rate limit: no more than 20 new_release POSTs per app per hour.
set -euo pipefail

# ---- config (overridable via env) -------------------------------------------------
OP_ITEM="${OP_ITEM:-Splunkbase}"      # 1Password item holding the Splunkbase login
OP_VAULT="${OP_VAULT:-CLI}"           # 1Password vault name

# ---- defaults / args --------------------------------------------------------------
APP_ID="${SPLUNKBASE_APP_ID:-}"
VERSION=""
PACKAGE=""                            # explicit pre-built package -> general mode
APP_NAME_ARG=""                       # display/validation name (general mode only)
SPLUNK_VERSIONS="9.1,9.2,9.3,9.4"
CIM_VERSIONS=""
VISIBILITY="false"                    # false = private/unlisted, true = public
DRY_RUN="false"
ASSUME_YES="false"

usage() {
  cat >&2 <<'EOF'
Usage: bin/publish-splunkbase.sh [options]

  --app-id <id>            Splunkbase numeric app id (or set SPLUNKBASE_APP_ID)
  --package <path>         Pre-built package (.spl or .tar.gz) to upload as-is.
                           When set, nothing is built and no repo layout is assumed.
                           Omit to build+publish nats from this repo (default).
  --app-name <name>        App name for messaging/validation (general mode only).
  --version <x.y.z>        Version to publish. General mode: parsed from the package
                           filename if omitted. nats default: meta.version in
                           globalConfig.json if omitted.
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
    --package)         PACKAGE="$2"; shift 2 ;;
    --app-name)        APP_NAME_ARG="$2"; shift 2 ;;
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

# Parse "<name>-<x.y.z>" out of a package basename (extension stripped). Emits
# "<name>\t<version>" when a version token is unambiguous, nothing otherwise.
parse_name_version() {
  local base="$1" ver name
  case "$base" in
    *.tar.gz) base="${base%.tar.gz}" ;;
    *.tgz)    base="${base%.tgz}" ;;
    *.spl)    base="${base%.spl}" ;;
  esac
  ver="${base##*-}"
  # A version token needs at least one dot (x.y[.z...][-suffix]); anything else is
  # treated as "no version in the name" so we never silently guess wrong.
  if [ "$ver" != "$base" ] && [[ "$ver" =~ ^[0-9]+\.[0-9]+([.-][0-9A-Za-z]+)*$ ]]; then
    name="${base%-"$ver"}"
    printf '%s\t%s' "$name" "$ver"
  fi
}

# ---- mode selection ---------------------------------------------------------------
if [ -n "$PACKAGE" ]; then
  # ---- general mode: upload the given package as-is ----
  [ -f "$PACKAGE" ] || { echo "Package not found: $PACKAGE" >&2; exit 1; }
  # Resolve to an absolute path so we never depend on this script's own location.
  PKG="$(cd "$(dirname -- "$PACKAGE")" && pwd -P)/$(basename -- "$PACKAGE")"

  pkg_base="$(basename -- "$PKG")"
  nv="$(parse_name_version "$pkg_base")"
  pkg_name="${nv%$'\t'*}"; [ "$pkg_name" = "$nv" ] && pkg_name=""
  pkg_ver="${nv#*$'\t'}";  [ "$pkg_ver" = "$nv" ] && pkg_ver=""

  APP_NAME="${APP_NAME_ARG:-${pkg_name:-$pkg_base}}"

  if [ -z "$VERSION" ]; then
    if [ -n "$pkg_ver" ]; then
      VERSION="$pkg_ver"
    else
      echo "Could not parse a version from package filename '$pkg_base'." >&2
      echo "Pass an explicit --version <x.y.z>." >&2
      exit 2
    fi
  fi
else
  # ---- nats default mode: build from this repo ----
  [ -z "$APP_NAME_ARG" ] || { echo "--app-name requires --package." >&2; exit 2; }
  cd "$(dirname "$0")/.."
  APP_NAME="nats"                     # ucc-gen add-on name -> nats-<version>.tar.gz
fi

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

# ---- resolve version + package (nats default mode) --------------------------------
if [ -z "$PACKAGE" ]; then
  if [ -z "$VERSION" ]; then
    VERSION="$(python3 -c 'import json;print(json.load(open("globalConfig.json"))["meta"]["version"])')"
  fi
  PKG="${APP_NAME}-${VERSION}.tar.gz"

  if [ ! -f "$PKG" ]; then
    echo "Package $PKG not found - building via ./build.sh $VERSION"
    ./build.sh "$VERSION"
  fi
  [ -f "$PKG" ] || { echo "Build did not produce $PKG" >&2; exit 1; }
fi

ENDPOINT="https://splunkbase.splunk.com/api/v1/app/${APP_ID}/new_release/"

echo "About to upload:"
echo "  app_name        : $APP_NAME"
echo "  package         : $PKG"
echo "  version         : $VERSION"
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
