#!/usr/bin/env bash
# End-to-end check of the add-on's logging and Monitoring Dashboard, the way an
# end user sees them:
# - every input and search command writes its own log file under
#   var/log/splunk, which Splunk indexes into _internal, one line per record;
# - nothing the inputs log reaches splunkd.log through stderr;
# - the Logging level set on the Configuration page takes effect in running
#   inputs without a restart;
# - the Monitoring Dashboard's ingestion, license and error queries (copied
#   from the UCC dashboard definitions) return data;
# - events keep the NATS subject as the nats_subject field.
#
# Starts a throwaway nats-server (JetStream enabled) and a disposable
# splunk/splunk container with the built package installed on a private docker
# network. Both containers and their volumes are removed at the end, unless
# E2E_KEEP=1 (then remove them with `docker rm -fv`).
#
# Usage:
#   tests/e2e/logging.sh <package.tar.gz> [splunk-image]
#
# Optional live subscription on a real server (credentials go on stdin only):
#   LIVE_NATS_SERVERS=nats://host.docker.internal:4222 LIVE_NATS_USERNAME=...
#   LIVE_NATS_PASSWORD=... LIVE_NATS_SUBJECT='telemetry.>'
set -euo pipefail

pkg="$(realpath "${1:?package path required}")"
image="${2:-splunk/splunk:latest}"

tag="nats-e2e-logging-$$"
net="$tag-net"
nats_name="$tag-nats"
splunk_name="$tag-splunk"
admin_pw="${SPLUNK_PASSWORD:-E2e-$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')}"
published=30

cleanup() {
  if [ "${E2E_KEEP:-}" = 1 ]; then
    echo "Keeping $splunk_name and $nats_name (remove with: docker rm -fv $splunk_name $nats_name; docker network rm $net)"
    return
  fi
  docker rm -fv "$splunk_name" "$nats_name" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$net" >/dev/null
docker run -d --name "$nats_name" --network "$net" nats:2.12-alpine -js >/dev/null

echo "Starting $image as $splunk_name"
docker run -d --name "$splunk_name" --network "$net" \
  --add-host=host.docker.internal:host-gateway \
  -p 127.0.0.1::8089 -p 127.0.0.1::8000 \
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
    nats -s "nats://$nats_name:4222" "$@" >/dev/null 2>&1
}

# Runs a search over the given time range and prints the results as JSON.
results() {
  local search="$1" earliest="${2:-0}"
  local sid
  sid="$(rest /services/search/v2/jobs -d exec_mode=blocking -d output_mode=json \
    -d earliest_time="$earliest" --data-urlencode "search=$search" | jq -r .sid)"
  rest "/services/search/v2/jobs/$sid/results" -G -d output_mode=json -d count=0 | jq -c '.results'
}

# Polls a search until the jq filter holds on its results, or gives up after
# about $3 seconds, and prints the last results.
await_results() {
  local search="$1" filter="$2" timeout="${3:-180}" out
  for _ in $(seq 1 $((timeout / 10))); do
    out="$(results "$search" "${4:-0}")"
    if jq -e "$filter" <<<"$out" >/dev/null 2>&1; then
      echo "$out"
      return
    fi
    sleep 10
  done
  echo "$out"
}

status=0
check() { # label, jq filter that must be true, json
  if jq -e "$2" <<<"$3" >/dev/null; then
    echo "PASS $1"
  else
    echo "FAIL $1: $3" >&2
    status=1
  fi
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

rest_ok /services/data/indexes -d name=nats
rest_ok /servicesNS/nobody/nats/nats_account -d name=local \
  --data-urlencode "servers=nats://$nats_name:4222"
# Nothing listens on this port, so inputs using the Account keep failing. Saving
# an Account tests the connection, so this one is written to the .conf directly,
# like an Account whose server went away after it was saved.
rest_ok /servicesNS/nobody/nats/configs/conf-nats_account -d name=broken \
  --data-urlencode "servers=nats://$nats_name:4999"

nats_cli kv add cfg
nats_cli kv put cfg a '{"x": 1}'
nats_cli kv put cfg b '{"x": 2}'

rest_ok /servicesNS/nobody/nats/nats_nats_subscribe -d name=demo -d account=local \
  -d index=nats --data-urlencode 'subject=demo.>'
rest_ok /servicesNS/nobody/nats/nats_nats_kv -d name=cfg -d account=local \
  -d index=nats -d bucket=cfg --data-urlencode 'subject=>'
rest_ok /servicesNS/nobody/nats/nats_nats_subscribe -d name=bad -d account=broken \
  -d index=nats --data-urlencode 'subject=demo.>'

if [ -n "${LIVE_NATS_SERVERS:-}" ]; then
  {
    printf 'name=live&servers=%s' "$(jq -rn --arg v "$LIVE_NATS_SERVERS" '$v|@uri')"
    if [ -n "${LIVE_NATS_USERNAME:-}" ]; then
      printf '&username=%s&password=%s' \
        "$(jq -rn --arg v "$LIVE_NATS_USERNAME" '$v|@uri')" \
        "$(jq -rn --arg v "${LIVE_NATS_PASSWORD:-}" '$v|@uri')"
    fi
  } | rest_ok /servicesNS/nobody/nats/nats_account --data-binary @-
  rest_ok /servicesNS/nobody/nats/nats_nats_subscribe -d name=live -d account=live \
    -d index=nats --data-urlencode "subject=${LIVE_NATS_SUBJECT:->}"
fi
sleep 15

for i in $(seq 1 "$published"); do
  nats_cli pub "demo.n$((i % 3))" "{\"n\":$i}"
done

# natssubscribe: one search that works and one whose Account cannot connect.
results '| natssubscribe account=local subject="demo.>" duration=2' >/dev/null
results '| natssubscribe account=broken subject="demo.>"' >/dev/null

echo "Waiting for the ingestion and license logs"
ingested="$(await_results \
  'search index=_internal source=*nats* action=events_ingested | stats sum(n_events) as n by modular_input_name sourcetype_ingested event_index' \
  "any(.[]; .modular_input_name == \"nats_subscribe://demo\" and .n == \"$published\") and any(.[]; .modular_input_name == \"nats_kv://cfg\")")"
check "ingestion panel counts the subscribe input's events" \
  "any(.[]; .modular_input_name == \"nats_subscribe://demo\" and .n == \"$published\" and .sourcetype_ingested == \"nats:message\" and .event_index == \"nats\")" "$ingested"
check "ingestion panel counts the KV input's events" \
  'any(.[]; .modular_input_name == "nats_kv://cfg" and .n == "2" and .sourcetype_ingested == "nats:kv")' "$ingested"

license="$(await_results \
  'search index=_internal source=*license_usage.log type=Usage (s IN (nats_subscribe*,nats_kv*)) | stats sum(b) as b by s idx' \
  'any(.[]; .s == "nats_subscribe://demo") and any(.[]; .s == "nats_kv://cfg")' 300)"
check "license panel matches the inputs by source" \
  'any(.[]; .s == "nats_subscribe://demo" and .idx == "nats") and any(.[]; .s == "nats_kv://cfg")' "$license"

errors="$(results 'search index=_internal source=*nats* log_level IN (ERROR) | stats count by exc_l source')"
check "errors panel shows the failing input's connection errors" \
  'any(.[]; .exc_l == "Connection Error" and (.source | endswith("nats_subscribe_bad.log")))' "$errors"
check "errors panel shows the failing search command" \
  'any(.[]; .exc_l == "Connection Error" and (.source | endswith("nats_natssubscribe.log")))' "$errors"

files="$(results 'search index=_internal source=*nats_*.log | stats count max(linecount) as lines by source')"
check "each input and command has its own log file in _internal" \
  '[.[].source | split("/") | last] as $f | ["nats_subscribe_demo.log", "nats_kv_cfg.log", "nats_subscribe_bad.log", "nats_natssubscribe.log"] - $f == []' "$files"
check "every log record is one line" 'all(.[]; .lines == "1")' "$files"

# splunkd logs each stderr line of an input as ExecProcessor "message from <script>"
stderr="$(results 'search index=_internal sourcetype=splunkd component=ExecProcessor "message from" (nats_subscribe.py OR nats_kv.py) | stats count')"
check "inputs write nothing to splunkd.log through stderr" '.[0].count == "0"' "$stderr"

events="$(results 'search index=nats | stats count by source nats_subject')"
check "subscribe events have the input as source and the subject as nats_subject" \
  "[.[] | select(.source == \"nats_subscribe://demo\") | .count | tonumber] | add == $published" "$events"
check "subjects are kept" \
  '[.[] | select(.source == "nats_subscribe://demo") | .nats_subject] | sort == ["demo.n0", "demo.n1", "demo.n2"]' "$events"
check "KV events have the input as source and the entry subject as nats_subject" \
  '[.[] | select(.source == "nats_kv://cfg") | .nats_subject] | sort == ["$KV.cfg.a", "$KV.cfg.b"]' "$events"
kv="$(results 'search index=nats source="nats_kv://cfg" nats_kv_key=a | table nats_kv_bucket nats_kv_key')"
check "KV events have bucket and key fields" '. == [{"nats_kv_bucket": "cfg", "nats_kv_key": "a"}]' "$kv"
subject_search="$(results 'search index=nats nats_subject=demo.n1 | stats count')"
check "nats_subject is searchable" ".[0].count == \"$((published / 3))\"" "$subject_search"

debug="$(results 'search index=_internal source=*nats_subscribe_bad.log log_level=DEBUG | stats count')"
check "no DEBUG records at the default INFO level" '.[0].count == "0"' "$debug"

echo "Setting the Logging level to DEBUG"
changed="$(date +%s)"
rest_ok /servicesNS/nobody/nats/nats_settings/logging -d loglevel=DEBUG
debug="$(await_results 'search index=_internal source=*nats_subscribe_bad.log log_level=DEBUG | stats count' \
  '.[0].count != "0"' 180 "$changed")"
check "DEBUG takes effect in a running input without a restart" '.[0].count != "0"' "$debug"

if [ -n "${LIVE_NATS_SERVERS:-}" ]; then
  live="$(await_results \
    'search index=_internal source=*nats* action=events_ingested modular_input_name="nats_subscribe://live" | stats sum(n_events) as n' \
    '.[0].n != null' 180)"
  check "live: ingestion panel counts the live input" '.[0].n | tonumber > 0' "$live"
  live="$(results 'search index=nats source="nats_subscribe://live" | stats count dc(nats_subject) as subjects')"
  check "live: events keep their subjects" '.[0].subjects | tonumber > 0' "$live"
  echo "live: $live"
fi

exit "$status"
