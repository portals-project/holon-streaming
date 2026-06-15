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
remote_env+="START_GATE_URL=http://producer-start-gate:8090"$'\n'
remote_env+="USE_LOG_FILE=true"$'\n'
remote_env+="RUN_MAX_THROUGHPUT_PRODUCER=false"$'\n'
remote_env+="SLEEP_BETWEEN_POLLS=0"$'\n'
if [[ "$PLATFORM" == "holon" ]]; then
  # Holon: the native Holon producer (not the Flink JSON producer); the broker's
  # in-cluster listener is kafka:9093. NUM_NODES drives the holon-nodes loop.
  remote_env+="KAFKA_BOOTSTRAP_SERVERS=kafka:9093"$'\n'
  remote_env+="RUN_FLINK_PRODUCER=false"$'\n'
  remote_env+="NUM_NODES=${KNOBS[N_NODES]}"$'\n'
else
  remote_env+="KAFKA_BOOTSTRAP_SERVERS=kafka:9092"$'\n'
  remote_env+="RUN_FLINK_PRODUCER=true"$'\n'
fi

if (( DRY_RUN )); then
  echo "$(c_yellow '[dry-run]') would write to $REMOTE_BASE/.env:"
  echo "$remote_env" | sed 's/^/      /'
else
  printf '%s' "$remote_env" | ssh "${SSH_OPTS[@]}" "$HOST" "cat > '$REMOTE_BASE/.env'"
  ok ".env written"
fi

log "Running $REMOTE_DEPLOY_SCRIPT on remote"
if [[ "$PLATFORM" == "holon" ]]; then
  # Holon has no SQL job to submit and no taskmanager scaling — deploy.sh just
  # brings the stack up; N_NODES/PRODUCER_COUNT drive in-container loops.
  deploy_cmd="cd '$REMOTE_BASE' && COMPOSE='$REMOTE_COMPOSE' bash $REMOTE_DEPLOY_SCRIPT"
else
  deploy_cmd="cd '$REMOTE_BASE' && QUERY='${KNOBS[QUERY]}' PARALLELISM='${KNOBS[PARALLELISM]}' TASKMANAGER_COUNT='${KNOBS[TASKMANAGER_COUNT]}' COMPOSE='$REMOTE_COMPOSE' bash $REMOTE_DEPLOY_SCRIPT"
fi
if (( DRY_RUN )); then
  run_cmd ssh "${SSH_OPTS[@]}" "$HOST" "$deploy_cmd"
else
  ssh "${SSH_OPTS[@]}" "$HOST" "$deploy_cmd" | sed 's/^/   /'
fi

if [[ "$PLATFORM" == "holon" ]]; then
  log "Confirming Holon nodes are running"
  if (( ! DRY_RUN )); then
    for attempt in $(seq 1 30); do
      if ssh "${SSH_OPTS[@]}" "$HOST" "cd '$REMOTE_BASE' && $REMOTE_COMPOSE -f '$REMOTE_COMPOSE_FILE' ps holon-nodes | grep -qiE 'running|Up'" 2>/dev/null; then
        ok "Holon nodes running"
        break
      fi
      sleep 2
      if (( attempt == 30 )); then
        err "Holon nodes container did not reach running within 60s"
        ssh "${SSH_OPTS[@]}" "$HOST" "cd '$REMOTE_BASE' && $REMOTE_COMPOSE -f '$REMOTE_COMPOSE_FILE' ps" | sed 's/^/   /'
        exit 1
      fi
    done
  fi
else
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
fi

log "Waiting for all producers to announce readiness to the start gate"
# The gate auto-flips once EXPECTED_PRODUCERS (=PRODUCER_COUNT) producers have
# POSTed /announce, so every producer is up and blocked on /ready before any
# data flows. We only fall back to the manual /start override if some producers
# never check in within GATE_WAIT_TIMEOUT.
expected="${KNOBS[PRODUCER_COUNT]}"
GATE_WAIT_TIMEOUT="${GATE_WAIT_TIMEOUT:-300}"
if (( DRY_RUN )); then
  run_cmd ssh "${SSH_OPTS[@]}" "$HOST" "curl -s http://localhost:8090/announced"
  run_cmd ssh "${SSH_OPTS[@]}" "$HOST" "curl -fsS -X POST http://localhost:8090/start  # fallback"
else
  gate_deadline=$(( SECONDS + GATE_WAIT_TIMEOUT ))
  last_count=-1
  while true; do
    resp=$(ssh_remote_q "curl -fsS http://localhost:8090/announced" 2>/dev/null || echo '')
    count=$(printf '%s' "$resp" | grep -o '"count":[0-9]*'    | grep -o '[0-9]*' | head -n1)
    started=$(printf '%s' "$resp" | grep -o '"started":[a-z]*' | grep -o 'true\|false' | head -n1)
    count=${count:-0}
    started=${started:-false}
    if [[ "$count" != "$last_count" ]]; then
      log "Producers ready: ${count}/${expected} announced to start gate"
      last_count="$count"
    fi
    if [[ "$started" == "true" ]] || (( count >= expected )); then
      ok "All ${expected} producers up and announced — start gate released; data is flowing"
      break
    fi
    if (( SECONDS >= gate_deadline )); then
      warn "Only ${count}/${expected} producers announced after ${GATE_WAIT_TIMEOUT}s — forcing start gate"
      ssh_remote "curl -fsS -X POST http://localhost:8090/start"
      ok "Start gate forced with ${count}/${expected} producers — data is flowing"
      break
    fi
    sleep 3
  done
fi
