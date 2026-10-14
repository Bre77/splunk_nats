#!/usr/bin/env bash
# End-to-end check that the add-on runs on a given Splunk Python runtime: every
# shipped bin/ module imports with that Splunk Python, and natssubscribe and
# natskv, pinned to it with python.required, return results.
#
# Starts a throwaway nats-server (JetStream enabled) and a disposable
# splunk/splunk container with the built package installed on a private docker
# network. Both containers and their volumes are always removed.
#
# Usage:
#   tests/e2e/search_commands_python.sh <package.tar.gz> <python-version> [splunk-image]
#
# Optional live check against a real server (credentials go on stdin only):
#   LIVE_NATS_SERVERS=nats://host.docker.internal:4222 LIVE_NATS_USERNAME=...
#   LIVE_NATS_PASSWORD=... LIVE_NATS_SUBJECT='telemetry.>'
#   [LIVE_NATS_KV_BUCKET=... LIVE_NATS_KV_DOMAIN=...]
set -euo pipefail

pkg="$(realpath "${1:?package path required}")"
pyver="${2:?python version required, e.g. 3.9}"
image="${3:-splunk/splunk:latest}"

tag="nats-e2e-py${pyver//./}-$$"
net="$tag-net"
nats_name="$tag-nats"
splunk_name="$tag-splunk"
admin_pw="E2e-$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"

# shellcheck disable=SC2329 # invoked by the EXIT trap
cleanup() {
  docker rm -fv "$splunk_name" "$nats_name" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$net" >/dev/null
docker run -d --name "$nats_name" --network "$net" nats:2.12-alpine -js >/dev/null

echo "Starting $image as $splunk_name"
docker run -d --name "$splunk_name" --network "$net" \
  --add-host=host.docker.internal:host-gateway \
  -p 127.0.0.1::8089 \
  -e SPLUNK_START_ARGS=--accept-license \
  -e SPLUNK_GENERAL_TERMS=--accept-sgt-current-at-splunk-com \
  -e SPLUNK_PASSWORD="$admin_pw" \
  -e SPLUNK_APPS_URL=/tmp/nats.tgz \
  -v "$pkg:/tmp/nats.tgz:ro" \
  "$image" >/dev/null

port="$(docker port "$splunk_name" 8089/tcp | head -1 | sed 's/.*://')"
base="https://127.0.0.1:$port"

# The admin password goes through a curl config on a pipe, never argv.
rest() {
  local path="$1"
  shift
  curl -sk --config <(printf 'user = "admin:%s"\n' "$admin_pw") "$@" "$base$path"
}

rest_ok() {
  local body code
  body="$(rest "$@" -w '\n%{http_code}')"
  code="${body##*$'\n'}"
  if [ "${code:0:1}" != 2 ]; then
    echo "REST $1 failed with HTTP $code: ${body%$'\n'*}" >&2
    return 1
  fi
}

nats_cli() {
  docker run --rm -i --network "$net" natsio/nats-box:latest \
    nats -s "nats://$nats_name:4222" "$@" >/dev/null
}

# Runs a blocking search job and prints its sid.
job() {
  rest /services/search/v2/jobs -d exec_mode=blocking -d output_mode=json \
    --data-urlencode "search=$1" | jq -r .sid
}
job_content() { rest "/services/search/v2/jobs/$1" -G -d output_mode=json | jq -c '.entry[0].content'; }
job_results() { rest "/services/search/v2/jobs/$1/results" -G -d output_mode=json -d count=0 | jq -c '.results'; }

status=0
check() { # label, jq filter that must be true, json
  if jq -e "$2" <<<"$3" >/dev/null; then
    echo "PASS $1"
  else
    echo "FAIL $1: $3" >&2
    status=1
  fi
}

# Runs a command search and checks it neither failed nor came back empty.
check_command() { # label, search
  local sid content results
  sid="$(job "$2")"
  content="$(job_content "$sid")"
  results="$(job_results "$sid")"
  check "$1 succeeds on python $pyver" \
    '(.isFailed | not) and all(.messages[]?; .type != "ERROR" and .type != "FATAL")' "$content"
  check "$1 returns results on python $pyver" 'length > 0' "$results"
}

echo "Waiting for splunkd and the nats app"
for _ in $(seq 1 120); do
  if rest /services/apps/local/nats -o /dev/null -w '%{http_code}' 2>/dev/null | grep -q 200 &&
    docker inspect -f '{{.State.Health.Status}}' "$splunk_name" 2>/dev/null | grep -q healthy; then
    break
  fi
  sleep 5
done
rest /services/apps/local/nats -o /dev/null -f

for cmd in natssubscribe natskv; do
  rest_ok "/servicesNS/nobody/nats/configs/conf-commands/$cmd" -d "python.required=$pyver"
done
pins="$(docker exec -u splunk "$splunk_name" /opt/splunk/bin/splunk btool --app=nats commands list |
  jq -Rsc '[split("\n")[] | select(startswith("python.required"))]')"
check "both commands pinned to python $pyver" \
  "length == 2 and all(.[]; . == \"python.required = $pyver\")" "$pins"

# A script file inside bin/, not python -c: the UCC REST handlers derive the
# app name from __main__.__file__.
imports="$(docker exec -u splunk -w /opt/splunk/etc/apps/nats/bin "$splunk_name" bash -c '
  check=_e2e_import_check.py
  trap "rm -f $check" EXIT
  for f in *.py; do
    m="${f%.py}"
    [ "$f" = "$check" ] && continue
    printf "import sys\nsys.path[:0] = [\"%s\", \"%s\"]\nimport %s\n" \
      "$PWD" "$PWD/../lib" "$m" >"$check"
    if out="$(/opt/splunk/bin/splunk cmd "python$1" "$check" 2>&1)"; then
      echo "ok $m"
    else
      echo "FAIL $m: $(tail -1 <<<"$out")"
    fi
  done' _ "$pyver")"
printf '  %s\n' "$imports"
if grep -qx "ok nats_subscribe_command" <<<"$imports" && ! grep -q "^FAIL" <<<"$imports"; then
  echo "PASS every bin/ module imports on python $pyver"
else
  echo "FAIL every bin/ module imports on python $pyver" >&2
  status=1
fi

rest_ok /servicesNS/nobody/nats/nats_account -d name=local \
  --data-urlencode "servers=nats://$nats_name:4222"

(for i in $(seq 1 20); do nats_cli pub demo.loop "{\"n\":$i}"; done) &
publisher=$!
sleep 2
check_command natssubscribe '| natssubscribe account=local subject="demo.loop" count=3 duration=60'
wait "$publisher"

nats_cli kv add cfg
nats_cli kv put cfg a '{"x": 1}'
check_command natskv '| natskv account=local bucket=cfg'

if [ -n "${LIVE_NATS_SERVERS:-}" ]; then
  {
    printf 'name=live&servers=%s' "$(jq -rn --arg v "$LIVE_NATS_SERVERS" '$v|@uri')"
    if [ -n "${LIVE_NATS_USERNAME:-}" ]; then
      printf '&username=%s&password=%s' \
        "$(jq -rn --arg v "$LIVE_NATS_USERNAME" '$v|@uri')" \
        "$(jq -rn --arg v "${LIVE_NATS_PASSWORD:-}" '$v|@uri')"
    fi
  } | rest_ok /servicesNS/nobody/nats/nats_account --data-binary @-
  check_command "live natssubscribe" \
    "| natssubscribe account=live subject=\"${LIVE_NATS_SUBJECT:->}\" count=50"
  if [ -n "${LIVE_NATS_KV_BUCKET:-}" ]; then
    domain_arg=""
    [ -n "${LIVE_NATS_KV_DOMAIN:-}" ] && domain_arg=" domain=\"$LIVE_NATS_KV_DOMAIN\""
    check_command "live natskv" \
      "| natskv account=live bucket=\"$LIVE_NATS_KV_BUCKET\"$domain_arg"
  fi
fi

exit "$status"
