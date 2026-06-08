#!/usr/bin/env bash
# Sourced by run-experiment.sh — do not execute directly.
# Phase 4: write remote .env, run deploy.sh, confirm Flink RUNNING, trigger start gate.

log "Generating per-run .env on remote"
remote_env=""
for k in QUERY WORKLOAD PARALLELISM TASKMANAGER_MEMORY JOBMANAGER_MEMORY \
         EVENTS_PER_SECOND N_NODES PARTITIONS_PER_NODE \
         PRODUCER_COUNT PRODUCER_SLEEP_MS PRODUCER_BATCH_SIZE WINDOW_LENGTH RUNTIME \
         ENABLE_RATE_LIMIT PROCESSING_RATE_LIMIT GARBAGE_COLLECTION_OFFSET \
         FLUSH_THRESHOLD TOPIC_METRICS_INTERVAL; do
  remote_env+="${k}=${KNOBS[$k]}"$'\n'
done
remote_env+="KAFKA_BOOTSTRAP_SERVERS=kafka:9092"$'\n'
remote_env+="START_GATE_URL=http://producer-start-gate:8090"$'\n'
remote_env+="USE_LOG_FILE=true"$'\n'
remote_env+="RUN_FLINK_PRODUCER=true"$'\n'
remote_env+="RUN_MAX_THROUGHPUT_PRODUCER=false"$'\n'
remote_env+="SLEEP_BETWEEN_POLLS=0"$'\n'

if (( DRY_RUN )); then
  echo "$(c_yellow '[dry-run]') would write to $REMOTE_BASE/.env:"
  echo "$remote_env" | sed 's/^/      /'
else
  printf '%s' "$remote_env" | ssh "${SSH_OPTS[@]}" "$HOST" "cat > '$REMOTE_BASE/.env'"
  ok ".env written"
fi

log "Running deploy.sh on remote"
deploy_cmd="cd '$REMOTE_BASE' && QUERY='${KNOBS[QUERY]}' PARALLELISM='${KNOBS[PARALLELISM]}' TASKMANAGER_COUNT='${KNOBS[TASKMANAGER_COUNT]}' COMPOSE='$REMOTE_COMPOSE' bash deploy.sh"
if (( DRY_RUN )); then
  run_cmd ssh "${SSH_OPTS[@]}" "$HOST" "$deploy_cmd"
else
  ssh "${SSH_OPTS[@]}" "$HOST" "$deploy_cmd" | sed 's/^/   /'
fi

log "Confirming Flink job is RUNNING"
if (( ! DRY_RUN )); then
  for attempt in $(seq 1 30); do
    if ssh "${SSH_OPTS[@]}" "$HOST" "curl -sf http://localhost:8081/jobs | grep -q '\"status\":\"RUNNING\"'" 2>/dev/null; then
      ok "Flink job RUNNING"
      break
    fi
    sleep 2
    if (( attempt == 30 )); then
      err "Flink job did not reach RUNNING within 60s"
      ssh "${SSH_OPTS[@]}" "$HOST" "curl -s http://localhost:8081/jobs" | sed 's/^/   /'
      exit 1
    fi
  done
fi

log "Releasing producer start gate (POST :8090/start)"
ssh_remote "curl -fsS -X POST http://localhost:8090/start"
ok "Producers released — data is flowing"
