#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Runs one demo scenario against Iggy for a fixed time and prints the averaged results,
# so a change can be measured before and after.
#
# Usage: scripts/demo-bench.sh [label]
# Settings come from the environment. Only those set are sent; the rest keep the demo's defaults.
#   CLIENT=kafka                          (kafka: this library's Kafka clients; iggy: the Iggy SDK directly)
#   Both: SIZE RATE PRODUCERS PARTITIONS SERVICES TOPICS_PER_SERVICE SKEW LINGER  (RATE=0 is unlimited, LINGER in ms)
#   kafka: TOPICS INSTANCES CATCH_ALL POLL_IDLE  (POLL_IDLE in ms)
#   iggy: STREAMS TOPICS_PER_STREAM CONSUMERS POLL_INTERVAL BATCH  (CONSUMERS is per service, POLL_INTERVAL in ms)
#   WARMUP=20 DURATION=60                 (seconds)
#   BOOTSTRAP=localhost:18090 PORT=8089
#   RESULTS=bench-results.jsonl           (one JSON line per run is appended here)
#   JAVA_HOME_21                          (JDK to run with; else JAVA_HOME, else the macOS java_home lookup)
#
# Examples:
#   CLIENT=iggy scripts/demo-bench.sh event-bus
#   CLIENT=iggy RATE=0 scripts/demo-bench.sh event-bus-max
set -euo pipefail

cd "$(dirname "$0")/.."
LABEL=${1:-run}
CLIENT=${CLIENT:-kafka}
case "$CLIENT" in
    kafka) MAIN=org.apache.iggy.kafka.demo.DemoServer ;;
    iggy) MAIN=org.apache.iggy.kafka.demo.IggySdkDemoServer ;;
    *) echo "CLIENT must be kafka or iggy"; exit 1 ;;
esac
WARMUP=${WARMUP:-20} DURATION=${DURATION:-60}
BOOTSTRAP=${BOOTSTRAP:-localhost:18090} PORT=${PORT:-8089}
RESULTS=${RESULTS:-bench-results.jsonl}
URL=http://localhost:$PORT
LOG=$(mktemp "${TMPDIR:-/tmp}/demo-bench.XXXXXX")

if [ -n "${JAVA_HOME_21:-}" ]; then
    export JAVA_HOME=$JAVA_HOME_21
elif [ -z "${JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then
    export JAVA_HOME=$(/usr/libexec/java_home -v 21)
fi

echo "Building"
mvn -q -Pdemo compile

echo "Starting the $CLIENT demo server on port $PORT (log: $LOG)"
mvn -q -Pdemo exec:java -Ddemo.main="$MAIN" -Ddemo.port="$PORT" -Ddemo.bootstrap="$BOOTSTRAP" >"$LOG" 2>&1 &
SERVER=$!

stop() {
    # The server's shutdown hook stops the run and waits while it deletes the run's topics.
    kill -TERM "$SERVER" 2>/dev/null || true
    wait "$SERVER" 2>/dev/null || true
}
trap stop EXIT

for _ in $(seq 1 120); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' "$URL/" 2>/dev/null)" = 200 ]; then break; fi
    if ! kill -0 "$SERVER" 2>/dev/null; then echo "Demo server exited:"; cat "$LOG"; exit 1; fi
    sleep 1
done

FORM=""
add() {
    # add VARIABLE field: sends the setting only if the variable is set.
    if [ -n "${!1:-}" ]; then FORM="$FORM${FORM:+&}$2=${!1}"; fi
}
add SIZE size; add RATE rate; add PRODUCERS producers; add PARTITIONS partitions
add SERVICES services; add TOPICS_PER_SERVICE topicsPerService; add SKEW skew
add TOPICS topics; add INSTANCES instances; add CATCH_ALL catchAll; add POLL_IDLE pollIdle
add STREAMS streams; add TOPICS_PER_STREAM topicsPerStream; add CONSUMERS consumers
add LINGER linger; add BATCH batch; add POLL_INTERVAL pollInterval
echo "Starting the run: $FORM"
curl -sf -X POST "$URL/start" -d "$FORM"
echo

echo "Warming up for ${WARMUP}s, then measuring for ${DURATION}s"
read -r -d '' SUMMARISE <<'PY' || true
import json, statistics, sys
label, warmup, results, client = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
samples = []
for line in sys.stdin:
    if not line.startswith("data:"):
        continue
    s = json.loads(line[5:])
    if s.get("running"):
        samples.append(s)
measured = samples[warmup:]
if not measured:
    sys.exit("No samples: the run did not start")

def mean(key, rows=measured):
    values = [r[key] for r in rows if r.get(key) is not None]
    return statistics.fmean(values) if values else None

def highest(key, rows=measured):
    values = [r[key] for r in rows if r.get(key) is not None]
    return max(values) if values else None

last = measured[-1]
summary = {
    "label": label,
    "client": client,
    "settings": {k: last[k] for k in ("size", "rate", "producers", "streams", "topicsPerStream", "topicCount",
                                      "partitions", "services", "topicsPerService", "instances", "consumers", "pollInterval", "catchAll", "skew", "linger", "batch")
                 if k in last},
    "seconds": len(measured),
    "produced": mean("produced"), "consumed": mean("consumed"), "mbps": mean("mbps"),
    "p50": mean("p50"), "p99": mean("p99"), "p99Max": highest("p99"), "lag": mean("lag"),
    "sendLatencyAvg": mean("sendLatencyAvg"), "fetchLatencyAvg": mean("fetchLatencyAvg"),
    "batchAvg": mean("batchAvg"),
    "errors": last["errors"] - measured[0]["errors"], "lastError": last["lastError"],
    "trimmed": last["trimmed"] - measured[0]["trimmed"],
    "services": [],
}
for sv in last.get("serviceStats", []):
    rows = [next((x for x in r["serviceStats"] if x["number"] == sv["number"]), {}) for r in measured]
    summary["services"].append({
        "number": sv["number"], "topics": len(sv["topics"]), "instances": sv.get("instances", sv.get("consumers")),
        "consumed": mean("consumed", rows), "lag": mean("lag", rows),
        "p50": mean("p50", rows), "p99": mean("p99", rows),
    })

def f(v, d=0):
    return "-" if v is None else f"{v:,.{d}f}"
print()
print(f"{label} ({client}): {summary['seconds']}s measured")
print(f"  produced {f(summary['produced'])} msg/s, consumed {f(summary['consumed'])} msg/s, {f(summary['mbps'], 2)} MB/s")
print(f"  latency p50 {f(summary['p50'], 1)} ms, p99 {f(summary['p99'], 1)} ms (mean per second, "
      f"worst second {f(summary['p99Max'], 1)} ms), lag {f(summary['lag'])}")
print(f"  send request {f(summary['sendLatencyAvg'], 2)} ms" + ("" if summary["batchAvg"] is None else f" with {f(summary['batchAvg'])} messages") + f", fetch request {f(summary['fetchLatencyAvg'], 2)} ms")
print(f"  errors {summary['errors']}, trimmed {summary['trimmed']}")
for sv in summary["services"]:
    print(f"  S{sv['number']}: {sv['topics']} topics, {sv['instances']} consumers, "
          f"{f(sv['consumed'])} msg/s, lag {f(sv['lag'])}, p50 {f(sv['p50'], 1)} ms, p99 {f(sv['p99'], 1)} ms")
with open(results, "a") as out:
    out.write(json.dumps(summary) + "\n")
print(f"Appended to {results}")
PY

# --max-time ends the stream on purpose, and curl reports that as exit code 28.
{ curl -s -N --max-time $((WARMUP + DURATION)) "$URL/events" 2>/dev/null || [ $? -eq 28 ]; } \
    | python3 -c "$SUMMARISE" "$LABEL" "$WARMUP" "$RESULTS" "$CLIENT"
