#!/usr/bin/env bash
# Produce synthetic gateway.access events straight to Kafka (tests SSE consumer / Admin UI).
# Run on the Linode box:
#   bash scripts/flood-kafka-access.sh 1000
#
# Optional:
#   KAFKA_BOOTSTRAP=127.0.0.1:9092 KAFKA_TOPIC=byz.gateway.access bash scripts/flood-kafka-access.sh 1000
#   KAFKA_BIN=/opt/kafka/bin/kafka-console-producer.sh bash scripts/flood-kafka-access.sh 1000
#   KAFKA_DOCKER=kafka   # if broker runs in a container named kafka

set -euo pipefail

COUNT="${1:-1000}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${KAFKA_TOPIC:-byz.gateway.access}"
PRODUCER="${KAFKA_BIN:-}"

find_producer() {
  if [[ -n "$PRODUCER" ]]; then
    return 0
  fi
  local c
  for c in kafka-console-producer.sh kafka-console-producer \
           /opt/kafka/bin/kafka-console-producer.sh \
           /usr/local/kafka/bin/kafka-console-producer.sh \
           /opt/bitnami/kafka/bin/kafka-console-producer.sh; do
    if command -v "$c" >/dev/null 2>&1 || [[ -x "$c" ]]; then
      PRODUCER="$c"
      return 0
    fi
  done
  return 1
}

gen_events() {
  local i rid eid ms path now
  local paths=(/iam/api/v1/login /notifications/api/v1/notifications /directory/api/v1/me /files/actuator/health)
  for i in $(seq 1 "$COUNT"); do
    rid=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen 2>/dev/null || echo "req-$i-$RANDOM")
    eid=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen 2>/dev/null || echo "evt-$i-$RANDOM")
    ms=$(( (RANDOM % 200) + 1 ))
    path="${paths[$((i % ${#paths[@]}))]}"
    now=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")
    printf '%s\n' "{\"eventId\":\"$eid\",\"type\":\"gateway.request.completed\",\"occurredAt\":\"$now\",\"requestId\":\"$rid\",\"method\":\"GET\",\"path\":\"$path\",\"status\":200,\"durationMs\":$ms,\"clientIp\":\"127.0.0.1\",\"routeId\":\"load-test\"}"
  done
}

echo "Producing $COUNT events → $TOPIC @ $BOOTSTRAP"

if [[ -n "${KAFKA_DOCKER:-}" ]]; then
  # Pipe into kafka-console-producer inside the container
  gen_events | docker exec -i "$KAFKA_DOCKER" kafka-console-producer \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC"
elif find_producer; then
  # Prefer modern flag; fall back to --broker-list for older Kafka
  if "$PRODUCER" --help 2>&1 | grep -q -- '--bootstrap-server'; then
    gen_events | "$PRODUCER" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
  else
    gen_events | "$PRODUCER" --broker-list "$BOOTSTRAP" --topic "$TOPIC"
  fi
else
  echo "kafka-console-producer not found." >&2
  echo "Set KAFKA_BIN=/path/to/kafka-console-producer.sh" >&2
  echo "Or: KAFKA_DOCKER=<container-name> bash $0 $COUNT" >&2
  exit 1
fi

echo "Done. Keep Admin System Health open — Live Requests / RPS should move."
echo "Note: recent ring keeps ~50 events; you still exercised the Go consumer with all $COUNT."
