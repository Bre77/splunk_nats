#!/usr/bin/env bash
# End-to-end test of payload decoders in a disposable Splunk container.
#
#   tests/e2e/decoder_e2e.sh <nats-x.y.z.tar.gz> [splunk image, default splunk/splunk:latest]
#
# Starts a NATS server and Splunk with the built add-on on a private Docker network,
# places a decoder in local/decoders, and creates a NATS Subscribe input that uses
# it. Publishes gzip-compressed JSON payloads, which the decoder turns into events,
# and payloads it cannot decode, which must be indexed as base64. Needs only Docker;
# everything is removed afterwards, volumes included.
set -euo pipefail

package=$(realpath "${1:?usage: $0 <nats-x.y.z.tar.gz> [splunk image]}")
image=${2:-splunk/splunk:latest}
run=nats-decoder-e2e-$$
password="E2e-$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
decoded=50
undecodable=5

cleanup() {
  docker rm -fv "$run-splunk" "$run-nats" >/dev/null 2>&1 || true
  docker network rm "$run" >/dev/null 2>&1 || true
}
trap cleanup EXIT

in_splunk() { docker exec -i -u splunk "$run-splunk" "$@"; }
rest() { in_splunk curl -sSk -u "admin:$password" "https://localhost:8089/servicesNS/nobody/nats/$1" "${@:2}"; }
search() {
  in_splunk curl -sSk -u "admin:$password" https://localhost:8089/services/search/v2/jobs/export \
    -d output_mode=csv -d earliest_time=-1h --data-urlencode "search=search $1" | tail -n +2 | tr -d '"\r'
}

echo "Starting NATS and $image"
docker network create "$run" >/dev/null
docker run -d --name "$run-nats" --network "$run" --network-alias nats nats:2.12-alpine -m 8222 >/dev/null
docker run -d --name "$run-splunk" --network "$run" \
  -e SPLUNK_START_ARGS=--accept-license -e SPLUNK_GENERAL_TERMS=--accept-sgt-current-at-splunk-com \
  -e SPLUNK_PASSWORD="$password" -e SPLUNK_APPS_URL=/tmp/nats.tgz \
  -v "$package:/tmp/nats.tgz:ro" "$image" >/dev/null
for _ in $(seq 120); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$run-splunk")" = healthy ] && break
  sleep 5
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$run-splunk")" = healthy ]

echo "Placing the decoder and creating the input"
in_splunk mkdir -p /opt/splunk/etc/apps/nats/local/decoders
in_splunk sh -c 'cat > /opt/splunk/etc/apps/nats/local/decoders/gzip_json && chmod 755 /opt/splunk/etc/apps/nats/local/decoders/gzip_json' <<'EOF'
#!/usr/bin/env python3
import base64, gzip, json, sys

for line in sys.stdin:
    request = json.loads(line)
    try:
        event = json.loads(gzip.decompress(base64.b64decode(request["payload"])))
        event["subject"] = request["subject"]
        print(json.dumps(event), flush=True)
    except Exception as e:
        print(f"not gzip JSON: {e}", file=sys.stderr, flush=True)
        print(flush=True)
EOF
in_splunk curl -sSk -u "admin:$password" https://localhost:8089/services/data/indexes -d name=e2e >/dev/null
rest nats_account -d name=local -d servers=nats://nats:4222 >/dev/null
rest nats_nats_subscribe -d name=decoded -d account=local -d 'subject=e2e.>' -d index=e2e \
  -d sourcetype=nats:message -d decoder=gzip_json >/dev/null

echo "Waiting for the input to subscribe"
for _ in $(seq 60); do
  in_splunk curl -s 'http://nats:8222/subsz?subs=1' | grep -q '"subject": *"e2e\.' && break
  sleep 2
done
in_splunk curl -s 'http://nats:8222/subsz?subs=1' | grep -q '"subject": *"e2e\.'

echo "Publishing $decoded decodable and $undecodable undecodable payloads"
in_splunk /opt/splunk/bin/splunk cmd python3 - "$decoded" "$undecodable" <<'EOF'
import asyncio, gzip, json, sys
sys.path.insert(0, "/opt/splunk/etc/apps/nats/lib")
import nats

async def main(decoded, undecodable):
    nc = await nats.connect("nats://nats:4222")
    for n in range(decoded):
        await nc.publish(f"e2e.{n % 3}.data", gzip.compress(json.dumps({"n": n}).encode()))
    for n in range(undecodable):
        await nc.publish("e2e.bad", bytes([0xFF, 0x00, n]))
    await nc.flush()
    await nc.close()

asyncio.run(main(int(sys.argv[1]), int(sys.argv[2])))
EOF

echo "Checking the indexed events"
for _ in $(seq 60); do
  counts=$(search 'index=e2e | stats count by sourcetype | eval c=sourcetype."=".count | stats values(c) as c | eval c=mvjoin(c, " ")')
  [ "$counts" = "nats:json=$decoded nats:message=$undecodable" ] && break
  sleep 5
done
echo "indexed: $counts"
[ "$counts" = "nats:json=$decoded nats:message=$undecodable" ]

fields=$(search 'index=e2e sourcetype=nats:json | stats dc(n) as n dc(subject) as subjects | eval r=n."/".subjects | fields r')
echo "decoded fields: $fields distinct n/subjects"
[ "$fields" = "$decoded/3" ]

raw=$(search 'index=e2e sourcetype=nats:message | head 1 | eval payload=_raw | table payload')
echo "undecodable payload kept as base64: $raw"
[[ "$raw" =~ ^/wA[A-E]$ ]]

# splunkd.log reaches _internal a little after the events
for _ in $(seq 30); do
  logged=$(search 'index=_internal sourcetype=splunkd "failed to decode" gzip_json "not gzip JSON" | stats count')
  [ "$logged" -ge 1 ] && break
  sleep 5
done
echo "decode failure log lines: $logged"
[ "$logged" -ge 1 ]

echo "PASS"
