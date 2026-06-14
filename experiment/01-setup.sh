#!/usr/bin/env bash
# Sourced by run-experiment.sh — do not execute directly.
# Phase 1: helpers, argument parsing, config loading, AWS check, EXIT trap.

# Global state (initialized here; mutated by later phases)
DRY_RUN=0
KEEP_RUNNING=0
ASCII_BANNER=0
TEMPLATE_NAME=""
LIST_ONLY=0
PLATFORM=""

INSTANCE_STARTED=0
VOLUME_RAISED=0
HOST=""
PUBLIC_IP=""
EXPERIMENT_LABEL=""
RESULTS_DIR=""

CONFIG_FILE="$REPO_ROOT/.holon.env"

# ------------------------------ helpers -------------------------------------

c_red()    { printf '\033[31m%s\033[0m' "$*"; }
c_green()  { printf '\033[32m%s\033[0m' "$*"; }
c_yellow() { printf '\033[33m%s\033[0m' "$*"; }
c_cyan()   { printf '\033[36m%s\033[0m' "$*"; }
c_bold()   { printf '\033[1m%s\033[0m' "$*"; }

log()  { echo "$(c_cyan "[holon]") $*"; }
ok()   { echo "$(c_green '  ✓') $*"; }
warn() { echo "$(c_yellow '  ⚠') $*" >&2; }
err()  { echo "$(c_red '  ✗') $*" >&2; }

run_cmd() {
  if (( DRY_RUN )); then
    printf '%s' "$(c_yellow '[dry-run]') "
    printf '%q ' "$@"
    echo
    return 0
  fi
  "$@"
}

usage() {
  cat <<EOF
HOLON unified experiment runner.

Usage:
  $(basename "$0") [options]

Options:
  --platform NAME    Select platform (flink|holon); skips the interactive prompt
  --template NAME    Use template NAME (without .env extension)
  --list-templates   List available templates and exit
  --dry-run          Print every external command without running it
  --keep-running     Do not stop the EC2 instance on exit
  --ascii            Force plain-ASCII banner
  -h, --help         Show this help

Templates live in:
  $REPO_ROOT/templates
EOF
}

print_banner() {
  local supports_unicode=1
  if (( ASCII_BANNER )); then
    supports_unicode=0
  elif [[ "${LANG:-}${LC_ALL:-}" != *UTF-8* && "${LANG:-}${LC_ALL:-}" != *utf8* ]]; then
    supports_unicode=0
  fi

  echo
  if (( supports_unicode )); then
    cat <<'BANNER'
  ██╗  ██╗ ██████╗ ██╗      ██████╗ ███╗   ██╗
  ██║  ██║██╔═══██╗██║     ██╔═══██╗████╗  ██║
  ███████║██║   ██║██║     ██║   ██║██╔██╗ ██║
  ██╔══██║██║   ██║██║     ██║   ██║██║╚██╗██║
  ██║  ██║╚██████╔╝███████╗╚██████╔╝██║ ╚████║
  ╚═╝  ╚═╝ ╚═════╝ ╚══════╝ ╚═════╝ ╚═╝  ╚═══╝
BANNER
  else
    cat <<'BANNER'
   _   _  ___  _     ___  _   _
  | | | |/ _ \| |   / _ \| \ | |
  | |_| | | | | |  | | | |  \| |
  |  _  | |_| | |__| |_| | |\  |
  |_| |_|\___/|_____\___/|_| \_|
BANNER
  fi
  echo "          Streaming Experiment Runner"
  echo
}

# ----------------------------- arg parsing ----------------------------------

while (($#)); do
  case "$1" in
    --platform)        PLATFORM="${2:-}"; shift 2 ;;
    --template)        TEMPLATE_NAME="${2:-}"; shift 2 ;;
    --list-templates)  LIST_ONLY=1; shift ;;
    --dry-run)         DRY_RUN=1; shift ;;
    --keep-running)    KEEP_RUNNING=1; shift ;;
    --ascii)           ASCII_BANNER=1; shift ;;
    -h|--help)         usage; exit 0 ;;
    *)                 err "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

# ----------------------------- cleanup trap ---------------------------------

cleanup() {
  local rc=$?
  if (( INSTANCE_STARTED )) && (( ! KEEP_RUNNING )); then
    log "Stopping EC2 instance ${INSTANCE_ID} (exit code $rc)"
    if (( DRY_RUN )); then
      run_cmd aws ec2 stop-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"
    else
      aws ec2 stop-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" >/dev/null \
        && ok "Stop request sent." \
        || err "Failed to stop instance — stop it manually: $INSTANCE_ID"
    fi
  elif (( INSTANCE_STARTED )) && (( KEEP_RUNNING )); then
    warn "EC2 instance left running ($INSTANCE_ID @ $PUBLIC_IP). Stop manually when done."
  fi

  # Ramp the EBS volume back down to baseline if we raised it for this run.
  # Best-effort: a failure here (e.g. AWS's ~6h modification cooldown) must never
  # abort cleanup or block the EC2 stop above — just warn and let the user
  # ramp it down manually later.
  if (( VOLUME_RAISED )) && (( ! KEEP_RUNNING )); then
    log "Ramping EBS volume back down to baseline"
    local ebs_args=(down)
    (( DRY_RUN )) && ebs_args+=(--dry-run)
    if ! bash "$REPO_ROOT/scripts/aws/ebs-perf.sh" "${ebs_args[@]}"; then
      warn "Could not ramp the volume down automatically — check and ramp it down manually:"
      warn "  bash scripts/aws/ebs-perf.sh status"
      warn "  bash scripts/aws/ebs-perf.sh down"
    fi
  elif (( VOLUME_RAISED )) && (( KEEP_RUNNING )); then
    warn "EBS volume left at benchmark performance. Ramp it down manually when done:"
    warn "  bash scripts/aws/ebs-perf.sh down"
  fi

  if [[ -n "$RESULTS_DIR" && -d "$RESULTS_DIR" ]]; then
    log "Results: $RESULTS_DIR"
  fi
}
trap cleanup EXIT

# ----------------------------- banner + config ------------------------------

print_banner

# List-only mode needs no config; 02-template.sh will handle the exit.
if (( ! LIST_ONLY )); then
  load_or_create_config() {
    if [[ -f "$CONFIG_FILE" ]]; then
      # shellcheck disable=SC1090
      source "$CONFIG_FILE"
      log "Loaded config from $CONFIG_FILE"
      return
    fi
    warn "No $CONFIG_FILE found — let's create one."
    read -r -p "  EC2 INSTANCE_ID         : " INSTANCE_ID
    read -r -p "  AWS_REGION              : " AWS_REGION
    read -r -p "  SSH key path (KEY)      : " KEY
    read -r -p "  REMOTE_USER [ec2-user]  : " REMOTE_USER
    REMOTE_USER="${REMOTE_USER:-ec2-user}"
    local default_base="/home/$REMOTE_USER/holon/flink-experiments"
    read -r -p "  REMOTE_BASE [$default_base] : " REMOTE_BASE
    REMOTE_BASE="${REMOTE_BASE:-$default_base}"
    read -r -p "  LOCAL_RESULTS_DIR [./experiment-results] : " LOCAL_RESULTS_DIR
    LOCAL_RESULTS_DIR="${LOCAL_RESULTS_DIR:-./experiment-results}"

    cat >"$CONFIG_FILE" <<EOF
INSTANCE_ID=$INSTANCE_ID
AWS_REGION=$AWS_REGION
KEY=$KEY
REMOTE_USER=$REMOTE_USER
REMOTE_BASE=$REMOTE_BASE
LOCAL_RESULTS_DIR=$LOCAL_RESULTS_DIR
EOF
    ok "Wrote $CONFIG_FILE"
  }

  load_or_create_config

  : "${INSTANCE_ID:?INSTANCE_ID missing in $CONFIG_FILE}"
  : "${AWS_REGION:?AWS_REGION missing in $CONFIG_FILE}"
  : "${KEY:?KEY missing in $CONFIG_FILE}"
  : "${REMOTE_USER:?REMOTE_USER missing in $CONFIG_FILE}"
  : "${REMOTE_BASE:?REMOTE_BASE missing in $CONFIG_FILE}"
  : "${LOCAL_RESULTS_DIR:?LOCAL_RESULTS_DIR missing in $CONFIG_FILE}"

  if [[ ! -f "$KEY" ]]; then
    err "SSH key not found at: $KEY"
    exit 1
  fi

  # ----------------------------- AWS check ------------------------------------

  if (( ! DRY_RUN )); then
    log "Running AWS prerequisites check..."
    bash "$REPO_ROOT/scripts/setup/check-aws.sh" || exit 1
  fi
fi
