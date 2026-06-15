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

# Summary
echo
log "Experiment complete"
printf '   %-18s : %s\n' "Label"       "$EXPERIMENT_LABEL"
printf '   %-18s : %s\n' "Template"    "$TEMPLATE_NAME_LABEL"
[[ -n "$TEMPLATE_DESC" ]] && printf '   %-18s : %s\n' "Description" "$TEMPLATE_DESC"
printf '   %-18s : %s\n' "Query"       "${KNOBS[QUERY]}"
printf '   %-18s : %s ms\n' "Runtime"  "${KNOBS[RUNTIME]}"
if (( ! DRY_RUN )) && (( start_ts > 0 )); then
  elapsed_secs=$(( $(date +%s) - start_ts ))
  printf '   %-18s : %s\n' "Elapsed"   "$(fmt_hms "$elapsed_secs")"
fi
printf '   %-18s : %s\n' "Results"     "$RESULTS_DIR"
echo
