#!/usr/bin/env bash
set -euo pipefail

# Env vars consumed:
#   QUERY              SQL file under queries/ (without extension). Default: q7
#   PARALLELISM        Flink default parallelism. Default: 10
#   TASKMANAGER_COUNT  Number of taskmanager replicas. Default: ceil(PARALLELISM/2)
#   COMPOSE            docker compose command. Default: auto-detect (docker compose / docker-compose)
#
# All other experiment knobs (EVENTS_PER_SECOND, N_NODES, etc.) are read by docker compose
# from a `.env` file in this directory.

QUERY="${QUERY:-q7}"
PARALLELISM="${PARALLELISM:-10}"
TASKMANAGER_COUNT="${TASKMANAGER_COUNT:-$(( (PARALLELISM + 1) / 2 ))}"

if [[ -z "${COMPOSE:-}" ]]; then
  if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
  else
    echo "Neither 'docker compose' nor 'docker-compose' is available." >&2
    exit 1
  fi
fi

COMPOSE_FILE="docker-compose.flink.yml"

QUERY_FILE="queries/${QUERY}.sql"
if [[ ! -f "$QUERY_FILE" ]]; then
  echo "Query file not found: $QUERY_FILE" >&2
  exit 1
fi

wait_until() {
  local label="$1"; shift
  local timeout="${TIMEOUT:-180}"
  local elapsed=0
  echo " .. waiting for $label (timeout ${timeout}s)"
  until "$@" >/dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    if (( elapsed >= timeout )); then
      echo " !! timed out waiting for $label" >&2
      return 1
    fi
  done
  echo " ✓ $label ready (${elapsed}s)"
}

echo "==> STEP 1: Tear down any old containers"
$COMPOSE -f "$COMPOSE_FILE" down --remove-orphans

echo "==> STEP 2: Start Kafka"
$COMPOSE -f "$COMPOSE_FILE" up -d kafka

echo "==> STEP 3: Wait for Kafka broker"
wait_until "Kafka broker" \
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

echo "==> STEP 4: Initialise Kafka topics"
$COMPOSE -f "$COMPOSE_FILE" up --build --abort-on-container-exit init-kafka

echo "==> STEP 5: Bring up full stack (detached) — taskmanager x ${TASKMANAGER_COUNT}"
$COMPOSE -f "$COMPOSE_FILE" up -d --build --scale "taskmanager=${TASKMANAGER_COUNT}"

echo "==> STEP 6: Wait for Flink JobManager"
wait_until "Flink JobManager" curl -sf http://localhost:8081/overview

echo "==> STEP 7: Wait for producer-start-gate"
# /ready returns 503 until /start is hit; we just want the gate process to be reachable.
wait_until "producer-start-gate" \
  bash -c 'code=$(curl -sf -o /dev/null -w "%{http_code}" http://localhost:8090/ready 2>/dev/null || echo 000); [[ "$code" == "200" || "$code" == "503" ]]'

echo "==> STEP 8: Submit query: $QUERY (parallelism=$PARALLELISM)"
docker exec -i flink-jobmanager bash -c \
  "bin/sql-client.sh -Dparallelism.default=${PARALLELISM} -f /opt/flink/queries/${QUERY}.sql"

echo "==> STEP 9: Wait for job to reach RUNNING state"
wait_until "Flink job RUNNING" \
  bash -c 'curl -sf http://localhost:8081/jobs | grep -q "\"status\":\"RUNNING\""'

echo "==> Stack is up; query submitted."
echo "    Producers will announce themselves to the start gate; once all"
echo "    EXPECTED_PRODUCERS (=PRODUCER_COUNT) have announced, the gate"
echo "    auto-flips and producers begin emitting."
echo "    Flink UI:     http://<host>:8081"
echo "    Kafka UI:     http://<host>:8080"
echo "    Gate status:  curl http://<host>:8090/announced"
echo "    Manual flip:  curl -X POST http://<host>:8090/start  (override)"
