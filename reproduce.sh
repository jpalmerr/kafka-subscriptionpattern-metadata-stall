#!/usr/bin/env bash
# Reproduces a Java consumer stall: a KIP-848 consumer subscribed with SubscriptionPattern
# (broker-side regex) stops refreshing metadata for good after a topic assigned to it is deleted.
#
# Starts a Kafka cluster in Docker (stock apache/kafka image, default settings except
# auto.create.topics.enable=false), runs five consumers in parallel, deletes an assigned topic,
# creates a new matching topic, and prints a verdict per consumer.
#
# Usage: ./reproduce.sh [--brokers 1|3] [--broker-version 4.3.1] [--observe 120] [--auto-commit false|true]
#                       [--out DIR]
# Needs: Docker, Java 17+, curl. Downloads kafka-clients jars from Maven Central. Jars and run
# output go under $TMPDIR/kafka-regex-stall, never into this directory.
set -euo pipefail

BROKERS=1
BROKER_VERSION=4.3.1
OBSERVE=120
AUTO_COMMIT=false  # on 4.2+, auto-commit retrying the deleted topic hides the heartbeat loop
OUT=
while [ $# -gt 0 ]; do
  case $1 in
    --brokers) BROKERS=$2; shift 2 ;;
    --broker-version) BROKER_VERSION=$2; shift 2 ;;
    --observe) OBSERVE=$2; shift 2 ;;
    --auto-commit) AUTO_COMMIT=$2; shift 2 ;;
    --out) OUT=$2; shift 2 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
case $BROKERS in 1|3) ;; *) echo "--brokers must be 1 or 3" >&2; exit 2 ;; esac

HERE=$(cd "$(dirname "$0")" && pwd)
WORK="${TMPDIR:-/tmp}/kafka-regex-stall"
OUT=${OUT:-"$WORK/out/$(date -u +%Y%m%dT%H%M%SZ)-brokers$BROKERS-$BROKER_VERSION-autocommit-$AUTO_COMMIT"}
mkdir -p "$OUT"
CACHE="$WORK/jars"
mkdir -p "$CACHE"
IMAGE="apache/kafka:$BROKER_VERSION"
CLIENT_VERSIONS="4.0.0 4.1.0 4.2.0 4.3.1"
NET=stall-net
CLUSTER_ID=stall-repro-cluster-01  # any 22-char base64-safe id

log() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$OUT/timeline.txt"; }

# Prints a consumer's first "Use errorsByTopicId" throw: the ERROR line (local time) with the
# exception and the first stack frames. Returns 1 if it hasn't thrown.
show_throw() { # name
  local f="$OUT/$1.log"
  grep -q "Use errorsByTopicId" "$f" 2>/dev/null || return 1
  grep -m1 -B1 -A3 "Use errorsByTopicId" "$f" | sed -E "s/^/    /; 1s/^    ([0-9:.]+).*/    \1 (consumer local time) ERROR Unexpected error caught in consumer network thread/"
}

# --- prerequisites -------------------------------------------------------------------------
command -v docker >/dev/null || { echo "docker not found" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl not found" >&2; exit 1; }
JAVA_MAJOR=$(java -XshowSettings:properties -version 2>&1 | awk -F' = ' '/java.specification.version/ {print $2}')
[ "${JAVA_MAJOR%%.*}" -ge 17 ] || { echo "Java 17+ required (found $JAVA_MAJOR)" >&2; exit 1; }

fetch() { # group-path artifact version
  local file="$CACHE/$2-$3.jar"
  [ -s "$file" ] || curl -fsSL -o "$file" "https://repo1.maven.org/maven2/$1/$2/$3/$2-$3.jar"
  echo "$file"
}
SLF4J="$(fetch org/slf4j slf4j-api 2.0.13):$(fetch org/slf4j slf4j-simple 2.0.13)"
for v in $CLIENT_VERSIONS; do fetch org/apache/kafka kafka-clients "$v" >/dev/null; done

# --- cluster -------------------------------------------------------------------------------
CONTAINERS=()
cleanup() {
  for c in "${CONTAINERS[@]:-}"; do
    [ -n "$c" ] && docker logs "$c" >"$OUT/$c.log" 2>&1 || true
    [ -n "$c" ] && docker rm -f "$c" >/dev/null 2>&1 || true
  done
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap 'kill $(jobs -p) 2>/dev/null || true; cleanup' EXIT

docker rm -f stall-kafka stall-c1 stall-b1 stall-b2 stall-b3 >/dev/null 2>&1 || true
docker network rm "$NET" >/dev/null 2>&1 || true
docker network create "$NET" >/dev/null

COMMON=(
  -e CLUSTER_ID=$CLUSTER_ID
  -e KAFKA_HEAP_OPTS="-Xmx512m -Xms512m"
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false
)

if [ "$BROKERS" = 1 ]; then
  log "start 1 combined broker/controller, $IMAGE"
  CONTAINERS+=(stall-kafka)
  docker run -d --name stall-kafka --network "$NET" -p 39092:9094 "${COMMON[@]}" \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://stall-kafka:9092,EXTERNAL://localhost:39092 \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@stall-kafka:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    "$IMAGE" >/dev/null
  CLI=stall-kafka
  # topic -> replica assignment (broker ids)
  ASSIGN_A=1; ASSIGN_B=1; ASSIGN_C=1
else
  log "start 1 controller and 3 brokers, $IMAGE"
  CONTAINERS+=(stall-c1)
  docker run -d --name stall-c1 --network "$NET" "${COMMON[@]}" \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=controller \
    -e KAFKA_LISTENERS=CONTROLLER://:9093 \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@stall-c1:9093 \
    "$IMAGE" >/dev/null
  for i in 1 2 3; do
    CONTAINERS+=(stall-b$i)
    docker run -d --name stall-b$i --network "$NET" -p 3909$((i + 1)):9094 "${COMMON[@]}" \
      -e KAFKA_NODE_ID=$((i + 1)) \
      -e KAFKA_PROCESS_ROLES=broker \
      -e KAFKA_LISTENERS=PLAINTEXT://:9092,EXTERNAL://:9094 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://stall-b$i:9092,EXTERNAL://localhost:3909$((i + 1)) \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@stall-c1:9093 \
      "$IMAGE" >/dev/null
  done
  CLI=stall-b1
  # The probes bootstrap from broker 2 (stall-b1). Lead each topic from a different broker,
  # and the deleted topic from one the probes did not bootstrap from.
  ASSIGN_A=2:3:4; ASSIGN_B=4:2:3; ASSIGN_C=3:4:2
fi
BOOT=localhost:39092
KT="docker exec $CLI /opt/kafka/bin"
BS=$CLI:9092

until $KT/kafka-topics.sh --bootstrap-server "$BS" --list >/dev/null 2>&1; do sleep 1; done
if [ "$BROKERS" = 3 ]; then
  until [ "$($KT/kafka-broker-api-versions.sh --bootstrap-server "$BS" 2>/dev/null | grep -c '^stall-b')" -ge 3 ]; do sleep 1; done
fi
log "cluster ready"

create() { $KT/kafka-topics.sh --bootstrap-server "$BS" --create --topic "$1" --replica-assignment "$2" >/dev/null; }
produce() {
  docker exec -d "$CLI" /opt/kafka/bin/kafka-producer-perf-test.sh --topic "$1" \
    --num-records 100000000 --throughput 5 --record-size 10 \
    --producer-props bootstrap.servers="$BS"
}
create stall-a "$ASSIGN_A"; produce stall-a
create stall-b "$ASSIGN_B"; produce stall-b
log "created and producing stall-a, stall-b"

# --- consumers -----------------------------------------------------------------------------
PROBES=()
probe() { # name client-version mode target
  local name=$1 version=$2 mode=$3 target=$4
  java -cp "$CACHE/kafka-clients-$version.jar:$SLF4J" \
    -Dorg.slf4j.simpleLogger.defaultLogLevel=info \
    -Dorg.slf4j.simpleLogger.log.org.apache.kafka.clients.Metadata=debug \
    -Dorg.slf4j.simpleLogger.log.org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager=warn \
    -Dorg.slf4j.simpleLogger.showDateTime=true \
    -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS \
    "$HERE/StallProbe.java" "$BOOT" "g-$name-$$" "$mode" "$target" $((OBSERVE + 300)) "$AUTO_COMMIT" \
    >"$OUT/$name.log" 2>&1 &
  PROBES+=("$name")
  log "consumer $name: kafka-clients $version, $mode $target"
}
probe regex-4.0.0 4.0.0 regex 'stall-.*'
probe regex-4.1.0 4.1.0 regex 'stall-.*'
probe regex-4.2.0 4.2.0 regex 'stall-.*'
probe regex-4.3.1 4.3.1 regex 'stall-.*'
probe names-4.1.0 4.1.0 names 'stall-a,stall-b'

sleep 45
# Remember where each log was when the topic was deleted, to count what happens after.
for p in "${PROBES[@]}"; do wc -l <"$OUT/$p.log" | tr -d ' ' >"$OUT/.$p.mark"; done
log "delete stall-b"
$KT/kafka-topics.sh --bootstrap-server "$BS" --delete --topic stall-b

sleep 30
for p in "${PROBES[@]}"; do
  if show_throw "$p" >/dev/null; then
    log "$p threw after the delete:"
    show_throw "$p" | tee -a "$OUT/timeline.txt"
  else
    log "$p: no throw"
  fi
done
create stall-c "$ASSIGN_C"; produce stall-c
log "created and producing stall-c; observing ${OBSERVE}s"
sleep "$OBSERVE"
log "stop"

# --- verdict -------------------------------------------------------------------------------
verdict() { # name
  local name=$1 f="$OUT/$1.log" mark after threw died meta newtopic hb rate result
  mark=$(cat "$OUT/.$name.mark")
  after=$(tail -n +"$((mark + 1))" "$f")
  threw=$(grep -c "Use errorsByTopicId" "$f" || true)
  died=$(grep -c "PROBE-ERROR" "$f" || true)
  meta=$(printf '%s\n' "$after" | grep -c "Updated cluster metadata" || true)
  newtopic=$(printf '%s\n' "$after" | grep '^PROBE' | grep -c 'received={[^}]*stall-c=' || true)
  # Heartbeat rate over the observation, from the cumulative counter the consumer reports every 5s.
  rate=$(printf '%s\n' "$after" | grep '^PROBE' | sed -E 's/.*heartbeats=([0-9]+).*/\1/' |
    awk 'NR==1 {first=$1} {last=$1; n=NR} END {if (n > 1) printf "%.0f", (last-first)/((n-1)*5); else print "?"}')
  case $name in
    names-*) if [ "$threw" = 0 ] && [ "$meta" -gt 0 ]; then result=OK; else result=UNEXPECTED; fi ;;
    *) if [ "$newtopic" -gt 0 ]; then result=OK
       elif [ "$threw" -gt 0 ] && [ "$meta" = 0 ]; then result=STALLED
       else result=INCONCLUSIVE; fi ;;
  esac
  [ "$died" -gt 0 ] && result="$result+DIED"
  printf '%-12s %-6s %-22s %-17s %-15s %s\n' "$name" "$threw" "$meta" \
    "$([ "$newtopic" -gt 0 ] && echo yes || echo no)" "$rate" "$result"
  echo "$name $result" >>"$OUT/.results"
}

{
  echo
  echo "Evidence: the throw in each consumer that threw"
  for p in "${PROBES[@]}"; do
    if show_throw "$p" >/dev/null; then echo "  $p"; show_throw "$p"; fi
  done
} | tee "$OUT/evidence.txt"

{
  echo
  echo "Brokers: $BROKERS x $IMAGE    Auto-commit: $AUTO_COMMIT    Observed ${OBSERVE}s after stall-c was created"
  echo
  printf '%-12s %-6s %-22s %-17s %-15s %s\n' consumer threw "metadata updates after" "stall-c consumed" "heartbeats/s" verdict
  printf '%-12s %-6s %-22s %-17s %-15s %s\n' "" "" "delete" "" "" ""
  for p in "${PROBES[@]}"; do verdict "$p"; done
} | tee "$OUT/verdict.txt"

expected=1
grep -q "^regex-4.0.0 OK" "$OUT/.results" || expected=0
grep -q "^names-4.1.0 OK" "$OUT/.results" || expected=0
for v in 4.1.0 4.2.0 4.3.1; do grep -q "^regex-$v STALLED" "$OUT/.results" || expected=0; done
if [ $expected = 1 ]; then
  echo "REPRODUCED: kafka-clients 4.1.0+ regex consumers stalled; the 4.0.0 regex consumer and the" | tee -a "$OUT/verdict.txt"
  echo "name-subscribed consumer kept going. Logs in $OUT" | tee -a "$OUT/verdict.txt"
else
  echo "NOT REPRODUCED as expected. See the table above and the logs in $OUT" | tee -a "$OUT/verdict.txt"
fi
