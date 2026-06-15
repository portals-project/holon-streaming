#!/usr/bin/env bash
set -euo pipefail

# HOLON remote deployment orchestrator (mirrors flink-remote-java/deploy.sh).
#
# Unlike Flink there is no SQL job to submit and no taskmanager scaling: the
# number of Holon nodes (N_NODES) and producers (PRODUCER_COUNT) drive in-container
# loops (start-nodes.sh / start-producers.sh), so we just bring the stack up.
#
# Env vars consumed:
#   COMPOSE   docker compose command. Default: auto-detect (docker compose / docker-compose)
#
# All experiment knobs (WORKLOAD, N_NODES, PRODUCER_COUNT, RUNTIME, ...) are read
# by docker compose from a `.env` file in this directory.

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

COMPOSE_FILE="docker-compose.holon.yml"

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
# Clean slate. This is a dedicated experiment box (the runner wipes the remote
# dir each run), so remove ANY leftover containers and unused networks — including
# ones created under a previous compose project name, which `down` above won't
# catch. Without this, leftovers cause "container name already in use" (fixed
# container_names like kafka) or "Pool overlaps" (lingering networks) errors.
leftovers=$(docker ps -aq)
[ -n "$leftovers" ] && docker rm -f $leftovers >/dev/null 2>&1 || true
docker network prune -f >/dev/null 2>&1 || true

echo "==> STEP 2: Start Kafka"
$COMPOSE -f "$COMPOSE_FILE" up -d kafka

echo "==> STEP 3: Wait for Kafka broker"
wait_until "Kafka broker" \
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

echo "==> STEP 4: Initialise Kafka topics"
$COMPOSE -f "$COMPOSE_FILE" up --abort-on-container-exit init-kafka

echo "==> STEP 5: Bring up full stack (detached)"
$COMPOSE -f "$COMPOSE_FILE" up -d

echo "==> STEP 6: Wait for producer-start-gate to be reachable"
# /ready returns 503 until the gate flips and 200 after. We only need the gate
# process reachable here — the actual "wait for all producers to announce" is
# done by the experiment runner (06-run.sh), which also has a force-start
# fallback. So accept BOTH 503 and 200 (no `curl -f`, which would reject 503).
wait_until "producer-start-gate" \
  bash -c 'code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8090/ready 2>/dev/null); [[ "$code" == "200" || "$code" == "503" ]]'

echo "==> STEP 7: Confirm Holon nodes container is up"
# `ps` prints "running" (compose v2) / "Up" (compose v1) in the status column.
wait_until "holon-nodes container" \
  bash -c "$COMPOSE -f '$COMPOSE_FILE' ps holon-nodes | grep -qiE 'running|Up'"

echo "==> Stack is up."
echo "    Producers announce themselves to the start gate; once all"
echo "    EXPECTED_PRODUCERS (=PRODUCER_COUNT) have announced, the gate"
echo "    auto-flips and producers begin emitting."
echo "    Kafka UI:     http://<host>:8080"
echo "    Gate status:  curl http://<host>:8090/announced"
echo "    Manual flip:  curl -X POST http://<host>:8090/start  (override)"
