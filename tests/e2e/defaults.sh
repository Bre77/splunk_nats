#!/usr/bin/env bash
# End-to-end check of the add-on's defaults and command behaviour, the way an
# end user hits them: sourcetype/host consistency between the nats_subscribe
# input and natssubscribe, props (one event per message, _time from the input
# rather than a timestamp guessed from the payload), NATS headers as fields,
# natssubscribe count=/duration=, single clean command errors, and natskv's
# sourcetype.
#
# Starts a throwaway nats-server (JetStream enabled) and a disposable
# splunk/splunk container with the built package installed on a private docker
# network. Both containers and their volumes are always removed.
#
# Usage:
#   tests/e2e/defaults.sh <package.tar.gz> [splunk-image]
#
# Optional live check against a real server (credentials go on stdin only):
#   LIVE_NATS_SERVERS=nats://host.docker.internal:4222 LIVE_NATS_USERNAME=...
#   LIVE_NATS_PASSWORD=... LIVE_NATS_SUBJECT='telemetry.>'
set -euo pipefail

pkg="$(realpath "${1:?package path required}")"
image="${2:-splunk/splunk:latest}"

tag="nats-e2e-defaults-$$"
net="$tag-net"
nats_name="$tag-nats"
splunk_name="$tag-splunk"
admin_pw="E2e-$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"

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
    -d earliest_time=0 --data-urlencode "search=$1" | jq -r .sid
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

# The REST handler, like the UI, has no subject default any more.
code="$(rest /servicesNS/nobody/nats/nats_nats_subscribe -d name=nosubject \
  -d account=local -d index=nats -o /dev/null -w '%{http_code}')"
check "subscribe input requires a subject" '. == 400' "$code"

rest_ok /servicesNS/nobody/nats/nats_nats_subscribe -d name=demo -d account=local \
  -d index=nats --data-urlencode 'subject=demo.>'
sleep 15

# A multi-line JSON payload that contains a timestamp, and a text payload with
# another one: neither must become _time.
nats_cli pub demo.json -H traceparent:00-e2e-01 \
  "$(printf '{\n  "a": 1,\n  "time": "2020-01-01T00:00:00Z"\n}')"
nats_cli pub demo.text "$(printf 'line one 2019-05-05 10:00:00\nline two')"
sleep 10

sid="$(job 'search index=nats nats_subject=demo.* | eval lag=abs(_indextime-_time) | table nats_subject sourcetype host linecount lag a')"
results="$(job_results "$sid")"
check "input indexed both messages" 'length == 2' "$results"
check "input sourcetype is nats:message" 'all(.[]; .sourcetype == "nats:message")' "$results"
check "input host is the NATS server host:port" "all(.[]; .host == \"$nats_name:4222\")" "$results"
check "multi-line payload stays one event" 'map(select(.nats_subject == "demo.json"))[0].linecount == "4"' "$results"
check "_time is the receive time, not a payload timestamp" 'all(.[]; (.lag | tonumber) < 60)' "$results"
check "JSON payload fields extract" 'map(select(.nats_subject == "demo.json"))[0].a == "1"' "$results"

# natssubscribe: count= returns early, results match the input's fields and
# carry headers.
(for i in $(seq 1 40); do nats_cli pub demo.loop -H "traceparent:00-t$i-01" "{\"n\":$i}"; done) &
publisher=$!
sleep 2
sid="$(job '| natssubscribe account=local subject="demo.loop" count=5 duration=60')"
content="$(job_content "$sid")"
results="$(job_results "$sid")"
wait "$publisher"
check "count=5 returns 5 results" 'length == 5' "$results"
check "count=5 returns before duration" '.runDuration < 30' "$content"
check "command sourcetype matches the input" 'all(.[]; .sourcetype == "nats:message")' "$results"
check "command host matches the input" "all(.[]; .host == \"$nats_name:4222\")" "$results"
check "headers become fields" 'all(.[]; ."headers.traceparent" | startswith("00-t"))' "$results"

sid="$(job '| natssubscribe account=local subject="quiet.subject" duration=2')"
content="$(job_content "$sid")"
check "duration=2 bounds an idle subscription" '.runDuration < 10 and .resultCount == 0' "$content"

sid="$(job '| natssubscribe account=local subject="bad subject"')"
content="$(job_content "$sid")"
check "command error is clean and names the real cause" \
  '([.messages[].text] | unique) == ["Error in '"'"'natssubscribe'"'"' command: nats: invalid subject"]' "$content"

nats_cli kv add cfg
nats_cli kv put cfg a '{"x": 1}'
sid="$(job '| natskv account=local bucket=cfg')"
results="$(job_results "$sid")"
check "natskv sourcetype matches the KV input" 'length == 1 and .[0].sourcetype == "nats:kv"' "$results"
check "natskv host form matches" ".[0].host == \"$nats_name:4222\"" "$results"

if [ -n "${LIVE_NATS_SERVERS:-}" ]; then
  {
    printf 'name=live&servers=%s' "$(jq -rn --arg v "$LIVE_NATS_SERVERS" '$v|@uri')"
    if [ -n "${LIVE_NATS_USERNAME:-}" ]; then
      printf '&username=%s&password=%s' \
        "$(jq -rn --arg v "$LIVE_NATS_USERNAME" '$v|@uri')" \
        "$(jq -rn --arg v "${LIVE_NATS_PASSWORD:-}" '$v|@uri')"
    fi
  } | rest_ok /servicesNS/nobody/nats/nats_account --data-binary @-
  sid="$(job "| natssubscribe account=live subject=\"${LIVE_NATS_SUBJECT:->}\" count=50")"
  content="$(job_content "$sid")"
  results="$(job_results "$sid")"
  check "live: count=50 returns 50 results" 'length == 50' "$results"
  check "live: returns quickly" '.runDuration < 30' "$content"
  echo "live: header fields seen: $(jq -c '[.[] | keys[] | select(startswith("headers."))] | unique' <<<"$results")"
fi

exit "$status"
