#!/usr/bin/env bash
# Sourced by run-experiment.sh — do not execute directly.
# Phase 4: ramp the gp3 EBS volume up to benchmark performance if the selected
# template asks for it (INCREASE_VOLUME=true). The matching ramp-down happens in
# the cleanup() EXIT trap in 01-setup.sh. Reuses scripts/aws/ebs-perf.sh as the
# single source of truth for the IOPS/throughput numbers and the wait logic.

increase_volume="${KNOBS[INCREASE_VOLUME]:-true}"

# Anything other than an explicit true/yes/1 means "leave the volume alone".
case "$(echo "$increase_volume" | tr '[:upper:]' '[:lower:]')" in
  true|yes|1) ;;
  *)
    log "Template opts out of the volume bump (INCREASE_VOLUME=$increase_volume) — skipping."
    return 0 2>/dev/null || exit 0
    ;;
esac

if [[ -z "${VOLUME_ID:-}" ]]; then
  err "INCREASE_VOLUME=true but VOLUME_ID is not set in $CONFIG_FILE."
  err "Add VOLUME_ID=vol-... to $CONFIG_FILE, or set INCREASE_VOLUME=false on the template."
  exit 1
fi

log "Ramping EBS volume up to benchmark performance ($VOLUME_ID)"
ebs_args=(up)
(( DRY_RUN )) && ebs_args+=(--dry-run)

# ebs-perf.sh is idempotent (no-ops if already at benchmark perf) and waits until
# the volume is usable. It exits non-zero on the AWS modification cooldown — guard
# the call so it doesn't silently abort this sourced run under `set -e`.
if bash "$REPO_ROOT/scripts/aws/ebs-perf.sh" "${ebs_args[@]}"; then
  VOLUME_RAISED=1
  ok "Volume at benchmark performance — will ramp back down on exit."
else
  warn "Could not ramp the volume up (e.g. AWS modification cooldown)."
  warn "Check current state: bash scripts/aws/ebs-perf.sh status"
  read -r -p "  Continue on the baseline (throttled) volume anyway? [y/N]: " yn
  [[ "$yn" =~ ^[Yy]$ ]] || { log "Aborted — volume not at benchmark performance."; exit 1; }
  warn "Continuing on a throttled volume — I/O-bound results may be skewed."
fi
