#!/usr/bin/env bash
# Sourced by run-experiment.sh — do not execute directly.
# Phase 5: runtime progress loop with graceful Ctrl-C abort.

RUNTIME_MS="${KNOBS[RUNTIME]}"
RUNTIME_SECS=$(( RUNTIME_MS / 1000 ))

fmt_hms() { printf '%02d:%02d:%02d' $(( $1 / 3600 )) $(( ($1 % 3600) / 60 )) $(( $1 % 60 )); }

log "Experiment running for $(fmt_hms "$RUNTIME_SECS") (RUNTIME=${RUNTIME_MS} ms)"
log "Live web UIs:"
(( HAS_FLINK_UI )) && printf '     %-12s http://%s:8081\n' "Flink:" "$PUBLIC_IP"
printf '     %-12s http://%s:8080\n' "Kafka:" "$PUBLIC_IP"
log "Press Ctrl-C to abort and collect partial results."

aborted=0
start_ts=0
trap 'aborted=1' INT

if (( ! DRY_RUN )); then
  start_ts=$(date +%s)
  while :; do
    now=$(date +%s)
    elapsed=$(( now - start_ts ))
    remaining=$(( RUNTIME_SECS - elapsed ))
    (( remaining < 0 )) && remaining=0

    if (( aborted )); then
      echo
      warn "Aborted at $(fmt_hms "$elapsed"). Collecting partial results."
      break
    fi
    if (( elapsed >= RUNTIME_SECS )); then
      echo
      ok "Runtime elapsed."
      break
    fi
    if (( elapsed % 30 == 0 )); then
      printf '\r  [%s elapsed / %s remaining]   ' "$(fmt_hms "$elapsed")" "$(fmt_hms "$remaining")"
    fi
    sleep 1
  done
  echo
fi

trap - INT
