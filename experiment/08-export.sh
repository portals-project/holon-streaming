#!/usr/bin/env bash
# Sourced by run-experiment.sh — do not execute directly.
# Phase 6: collect results from remote, write summary. EC2 stop is handled by the EXIT trap.

ts=$(date +%Y%m%d_%H%M%S)
safe_label=$(echo "$TEMPLATE_NAME_LABEL" | tr ' /' '__')
EXPERIMENT_LABEL="${safe_label}_${ts}"
RESULTS_DIR="$REPO_ROOT/${LOCAL_RESULTS_DIR#./}/$EXPERIMENT_LABEL"

log "Collecting results to $RESULTS_DIR"
run_cmd mkdir -p "$RESULTS_DIR"
if (( ! DRY_RUN )); then
  scp -i "$KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
      -r "$HOST:$REMOTE_BASE/$LOGS_DIRNAME/" "$RESULTS_DIR/" || warn "Some files may not have been copied."
  if [[ -n "$TEMPLATE_FILE" && -f "$TEMPLATE_FILE" ]]; then
    cp "$TEMPLATE_FILE" "$RESULTS_DIR/template.env"
  else
    {
      echo "NAME=\"$TEMPLATE_NAME_LABEL\""
      echo "DESCRIPTION=\"$TEMPLATE_DESC\""
      for k in "${!KNOBS[@]}"; do echo "$k=${KNOBS[$k]}"; done
    } > "$RESULTS_DIR/template.env"
  fi
  echo "$EXPERIMENT_LABEL" > "$RESULTS_DIR/label.txt"
  ok "Results collected"
fi

# Parse the raw log into base-result CSVs (best-effort — never aborts the run).
# Same metrics consumer + logback (output.log) for both platforms, so one parser
# covers Flink and Holon; tags a platform doesn't emit yield header-only CSVs.
metrics_report=""
if (( ! DRY_RUN )); then
  LOG="$RESULTS_DIR/$LOGS_DIRNAME/output.log"
  if [[ ! -f "$LOG" ]]; then
    LOG=$(ls "$RESULTS_DIR/$LOGS_DIRNAME"/*.log 2>/dev/null | head -n1 || true)
  fi
  PY="$(command -v python3 || command -v python || true)"
  if [[ -z "$PY" ]]; then
    warn "Python not found — skipping metric CSVs (raw logs + summary still saved)."
  elif [[ -z "$LOG" || ! -f "$LOG" ]]; then
    warn "No log under $RESULTS_DIR/$LOGS_DIRNAME — skipping metric CSVs."
  else
    log "Parsing metrics from $(basename "$LOG") -> metrics/"
    if metrics_report=$("$PY" "$REPO_ROOT/scripts/analysis/parse_output_log.py" \
          --input "$LOG" --out-dir "$RESULTS_DIR/metrics" --base-name "$EXPERIMENT_LABEL" 2>&1); then
      echo "$metrics_report" | sed 's/^/   /'
      ok "Metric CSVs written to $RESULTS_DIR/metrics"
    else
      warn "Metric parsing failed (run is otherwise complete):"
      echo "$metrics_report" | sed 's/^/   /'
    fi
  fi
fi

# Summary — built once, printed to console AND saved to summary.txt for the package.
elapsed_line=""
if (( ! DRY_RUN )) && (( start_ts > 0 )); then
  elapsed_line=$(fmt_hms "$(( $(date +%s) - start_ts ))")
fi

build_summary() {
  printf 'Experiment summary\n'
  printf '  %-18s : %s\n' "Label"       "$EXPERIMENT_LABEL"
  printf '  %-18s : %s\n' "Template"    "$TEMPLATE_NAME_LABEL"
  [[ -n "$TEMPLATE_DESC" ]] && printf '  %-18s : %s\n' "Description" "$TEMPLATE_DESC"
  printf '  %-18s : %s\n' "Platform"    "$PLATFORM"
  printf '  %-18s : %s\n' "Deployment"  "${DEPLOYMENT:-}"
  printf '  %-18s : %s\n' "Query"       "${KNOBS[QUERY]}"
  printf '  %-18s : %s ms\n' "Runtime"  "${KNOBS[RUNTIME]}"
  [[ -n "$elapsed_line" ]] && printf '  %-18s : %s\n' "Elapsed" "$elapsed_line"
  printf '  %-18s : %s\n' "Instance"    "${INSTANCE_ID:-} @ ${PUBLIC_IP:-}"
  printf '  %-18s : %s\n' "Timestamp"   "$ts"
  printf '  ----\n  Knobs:\n'
  for k in $(printf '%s\n' "${!KNOBS[@]}" | sort); do
    printf '    %-24s = %s\n' "$k" "${KNOBS[$k]}"
  done
  if [[ -n "$metrics_report" ]]; then
    printf '  ----\n'
    printf '%s\n' "$metrics_report" | sed 's/^/  /'
  fi
}

echo
log "Experiment complete"
build_summary | sed 's/^/   /'
if (( ! DRY_RUN )); then
  build_summary > "$RESULTS_DIR/summary.txt"
  ok "Summary written to $RESULTS_DIR/summary.txt"
fi
printf '   %-18s : %s\n' "Results"     "$RESULTS_DIR"
echo
