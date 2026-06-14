#!/usr/bin/env bash
# Sourced by run-experiment.sh — do not execute directly.
# Phase 9: ask whether to ramp the EBS volume back down to baseline.
#
# AWS allows only ~one volume modification per 6h, so ramping down is a deliberate
# "I'm done for the session" choice — NOT something we do automatically. Auto-ramping
# down after every run would lock you out of raising the volume again for your next
# experiment (and most experiments need it raised). So we force an explicit yes/no
# here instead, right after the results are collected, so it can't be forgotten.

# Nothing to decide if we never raised the volume this run.
(( VOLUME_RAISED )) || { return 0 2>/dev/null || exit 0; }

# Record that the user was asked, so the EXIT trap's safety-net warning stays quiet
# on the "no, keep it up" path (the message below already covers it).
VOLUME_PROMPTED=1

echo
log "Volume teardown"
warn "The EBS volume is at benchmark performance (12000 IOPS / 750 MB/s) and costs extra."
warn "AWS only allows ramping it back UP once per ~6h — so only ramp DOWN when you are"
warn "actually done running experiments for this session."
echo
read -r -p "Is this your last experiment? Ramp the volume DOWN to baseline now? [y/N]: " yn

if [[ "$yn" =~ ^[Yy]$ ]]; then
  ebs_args=(down)
  (( DRY_RUN )) && ebs_args+=(--dry-run)
  if bash "$REPO_ROOT/scripts/aws/ebs-perf.sh" "${ebs_args[@]}"; then
    VOLUME_RAISED=0
    ok "Volume ramped down to baseline."
  else
    warn "Ramp-down failed — ramp it down manually when you can:"
    warn "  bash scripts/aws/ebs-perf.sh down"
  fi
else
  log "Leaving the volume at benchmark performance for your next experiment."
  warn "Remember to ramp it down when you are done for the session:"
  warn "  bash scripts/aws/ebs-perf.sh down"
fi
