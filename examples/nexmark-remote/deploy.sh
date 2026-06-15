#!/usr/bin/env bash
set -euo pipefail

# HOLON remote deployment orchestrator — SBT "on-the-fly" variant.
# Mirrors nexmark-remote-java/deploy.sh, but the stack compiles the bind-mounted
# source at run time (holon-build one-shot) instead of using assembled JARs, so
# the bring-up blocks on a one-time `sbt compile` (minutes). Timeouts are sized
# accordingly.
#
# Env vars consumed:
#   COMPOSE   docker compose command. Default: auto-detect.
#   TIMEOUT   per-wait timeout in seconds (default 900 to cover the compile).
#
# All experiment knobs are read by docker compose from a `.env` file here.

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

COMPOSE_FILE="docker-compose.holon-sbt.yml"

wait_until() {
  local label="$1"; shift
  local timeout="${TIMEOUT:-900}"
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
# Clean slate (dedicated experiment box). Remove ANY leftover containers and
# unused networks — including ones from a previous compose project name — to avoid
# "container name already in use" / "Pool overlaps" errors.
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

echo "==> STEP 5: Bring up full stack (compiles source via holon-build, then nodes/producers/consumer)"
# `up -d` blocks until holon-build completes (service_completed_successfully) before
# starting the run services, so this step covers the one-time sbt compile.
$COMPOSE -f "$COMPOSE_FILE" up -d

echo "==> STEP 6: Wait for producer-start-gate to be reachable"
# /ready returns 503 until the gate flips and 200 after; accept BOTH (the runner
# does the real "all producers announced" wait, with a force-start fallback).
wait_until "producer-start-gate" \
  bash -c 'code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8090/ready 2>/dev/null); [[ "$code" == "200" || "$code" == "503" ]]'

echo "==> STEP 7: Confirm Holon nodes container is up"
wait_until "holon-nodes container" \
  bash -c "$COMPOSE -f '$COMPOSE_FILE' ps holon-nodes | grep -qiE 'running|Up'"

echo "==> Stack is up."
echo "    Source was compiled on the box (holon-build); nodes/producers/consumer"
echo "    run from the freshly compiled classpath."
echo "    Kafka UI:     http://<host>:8080"
echo "    Gate status:  curl http://<host>:8090/announced"
echo "    Manual flip:  curl -X POST http://<host>:8090/start  (override)"
