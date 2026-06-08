#!/usr/bin/env bash
#
# Toggle gp3 EBS volume performance to control benchmarking costs.
#
# Run this manually at the start and end of a benchmarking day:
#   - Morning: ramp the volume UP to benchmark performance.
#   - Evening: ramp the volume DOWN to baseline (free-tier) performance.
#
# Usage:
#   ./scripts/aws/ebs-perf.sh up      # ramp to benchmark perf (12000 IOPS, 750 MB/s)
#   ./scripts/aws/ebs-perf.sh on      #   alias for "up"
#   ./scripts/aws/ebs-perf.sh down    # ramp to baseline (3000 IOPS, 125 MB/s) — free tier
#   ./scripts/aws/ebs-perf.sh off     #   alias for "down"
#   ./scripts/aws/ebs-perf.sh status  # show current IOPS/throughput and modification state
#
# Options:
#   --volume ID        Override VOLUME_ID for this run
#   --region NAME      Override AWS_REGION for this run
#   --dry-run          Print the modify-volume call without executing it
#   -h, --help         Show this help
#
# Config is read from .holon.env (or the environment):
#   VOLUME_ID         (required) e.g. vol-0381a645bd44d3363
#   AWS_REGION        (required) e.g. eu-central-1
#   HIGH_IOPS         (default: 12000)
#   HIGH_THROUGHPUT   (default: 750)
#   LOW_IOPS          (default: 3000)
#   LOW_THROUGHPUT    (default: 125)
#   DRY_RUN           (default: 0)
#
# IMPORTANT: AWS enforces a ~6-hour cooldown between modifications of the
# same volume. If you ramp up at 09:00, you cannot ramp down until ~15:00.
# This fits the "up in the morning, down in the evening" workflow fine, but
# don't try to toggle multiple times per day.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG_FILE="$REPO_ROOT/.holon.env"

# ------------------------------ helpers -------------------------------------

c_red()    { printf '\033[31m%s\033[0m' "$*"; }
c_green()  { printf '\033[32m%s\033[0m' "$*"; }
c_yellow() { printf '\033[33m%s\033[0m' "$*"; }
c_cyan()   { printf '\033[36m%s\033[0m' "$*"; }

log()  { echo "$(c_cyan '[ebs-perf]') $*"; }
ok()   { echo "$(c_green '  ✓') $*"; }
warn() { echo "$(c_yellow '  ⚠') $*" >&2; }
err()  { echo "$(c_red '  ✗') $*" >&2; }

usage() {
  # Print the leading comment block (everything after the shebang up to the
  # first blank/non-comment line), stripping the leading "# ".
  sed -n '2,/^[^#]/p' "${BASH_SOURCE[0]}" | sed -e '/^[^#]/d' -e 's/^#\{1,\} \{0,1\}//'
}

# ----------------------------- arg parsing ----------------------------------

mode=""
VOLUME_ID_OVERRIDE=""
REGION_OVERRIDE=""

while (($#)); do
  case "$1" in
    up|on)        mode="up";     shift ;;
    down|off)     mode="down";   shift ;;
    status)       mode="status"; shift ;;
    --volume)     VOLUME_ID_OVERRIDE="${2:-}"; shift 2 ;;
    --region)     REGION_OVERRIDE="${2:-}";    shift 2 ;;
    --dry-run)    DRY_RUN=1; shift ;;
    -h|--help)    usage; exit 0 ;;
    *)            err "Unknown argument: $1"; usage; exit 2 ;;
  esac
done

if [[ -z "$mode" ]]; then
  err "Usage: $(basename "$0") {up|on|down|off|status} [--volume ID] [--region NAME] [--dry-run]"
  exit 2
fi

# ----------------------------- config ---------------------------------------

if [[ -f "$CONFIG_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
  log "Loaded config from $CONFIG_FILE"
fi

# CLI overrides win over .holon.env / environment.
[[ -n "$VOLUME_ID_OVERRIDE" ]] && VOLUME_ID="$VOLUME_ID_OVERRIDE"
[[ -n "$REGION_OVERRIDE" ]]    && AWS_REGION="$REGION_OVERRIDE"

DRY_RUN="${DRY_RUN:-0}"
HIGH_IOPS="${HIGH_IOPS:-12000}"
HIGH_THROUGHPUT="${HIGH_THROUGHPUT:-750}"
LOW_IOPS="${LOW_IOPS:-3000}"
LOW_THROUGHPUT="${LOW_THROUGHPUT:-125}"

: "${VOLUME_ID:?VOLUME_ID is required — set it in $CONFIG_FILE or pass --volume (e.g. vol-0381a645bd44d3363)}"
: "${AWS_REGION:?AWS_REGION is required — set it in $CONFIG_FILE or pass --region (e.g. eu-central-1)}"

if ! command -v aws >/dev/null 2>&1; then
  err "aws CLI not found on PATH. Install from https://aws.amazon.com/cli/"
  exit 1
fi

# ----------------------------- volume ops -----------------------------------

current_perf() {
  aws ec2 describe-volumes \
    --volume-ids "$VOLUME_ID" \
    --region "$AWS_REGION" \
    --query 'Volumes[0].[Iops,Throughput,Size,VolumeType]' \
    --output text
}

show_status() {
  log "Volume: $VOLUME_ID  Region: $AWS_REGION"
  read -r iops tput size vtype < <(current_perf)
  printf '   Size:       %s GiB\n' "$size"
  printf '   Type:       %s\n' "$vtype"
  printf '   IOPS:       %s\n' "$iops"
  printf '   Throughput: %s MB/s\n' "$tput"
  state=$(aws ec2 describe-volumes-modifications \
    --volume-ids "$VOLUME_ID" \
    --region "$AWS_REGION" \
    --query 'VolumesModifications[0].ModificationState' \
    --output text 2>/dev/null || echo "none")
  if [[ "$state" != "none" && "$state" != "None" ]]; then
    printf '   Last mod:   %s\n' "$state"
  fi
}

modify_and_wait() {
  local target_iops="$1"
  local target_tput="$2"
  local label="$3"

  if (( DRY_RUN )); then
    log "Would modify $VOLUME_ID → $target_iops IOPS, $target_tput MB/s ($label)"
    printf '   %s aws ec2 modify-volume --volume-id %s --iops %s --throughput %s --region %s\n' \
      "$(c_yellow '[dry-run]')" "$VOLUME_ID" "$target_iops" "$target_tput" "$AWS_REGION"
    return 0
  fi

  read -r cur_iops cur_tput _ _ < <(current_perf)
  if [[ "$cur_iops" == "$target_iops" && "$cur_tput" == "$target_tput" ]]; then
    ok "Already at $label ($target_iops IOPS, $target_tput MB/s) — nothing to do"
    return 0
  fi

  log "Modifying $VOLUME_ID → $target_iops IOPS, $target_tput MB/s ($label)"

  # Capture stderr so we can detect the cooldown error cleanly.
  if ! err_msg=$(aws ec2 modify-volume \
        --volume-id "$VOLUME_ID" \
        --iops "$target_iops" \
        --throughput "$target_tput" \
        --region "$AWS_REGION" 2>&1 >/dev/null); then
    if [[ "$err_msg" == *"VolumeModificationRateExceeded"* ]]; then
      err "AWS 6-hour cooldown is active — this volume was modified recently."
      err "Check when it's safe to retry:"
      err "  aws ec2 describe-volumes-modifications --volume-ids $VOLUME_ID --region $AWS_REGION"
      exit 1
    fi
    err "modify-volume failed:"
    printf '%s\n' "$err_msg" | sed 's/^/    /' >&2
    exit 1
  fi

  log "Waiting for modification (volume usable once state = 'optimizing')…"
  local elapsed=0
  local max_wait=900   # 15 min safety cap
  while (( elapsed < max_wait )); do
    read -r state progress < <(aws ec2 describe-volumes-modifications \
      --volume-ids "$VOLUME_ID" \
      --region "$AWS_REGION" \
      --query 'VolumesModifications[0].[ModificationState,Progress]' \
      --output text)
    case "$state" in
      optimizing|completed)
        echo
        ok "State: $state (${progress}%) — volume ready"
        return 0
        ;;
      modifying)
        printf '\r   modifying… %s%% (%ss elapsed)' "$progress" "$elapsed"
        ;;
      failed)
        echo
        err "Modification failed. Check the AWS console for details."
        exit 1
        ;;
    esac
    sleep 5
    elapsed=$((elapsed + 5))
  done
  echo
  warn "Timed out after ${max_wait}s — modification may still be in progress."
  return 1
}

case "$mode" in
  up)     modify_and_wait "$HIGH_IOPS" "$HIGH_THROUGHPUT" "benchmark" ;;
  down)   modify_and_wait "$LOW_IOPS"  "$LOW_THROUGHPUT"  "baseline"  ;;
  status) show_status ;;
esac
