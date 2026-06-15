#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

usage() {
  cat <<'EOF'
Sync HOLON (nexmark-remote-java) remote deployment files to an external instance.

Unlike Flink, the Holon system has no SQL query files or connector JARs — the
workload is selected via the WORKLOAD knob in the assembled JAR baked into the
images. So only the "core" deployment files are synced.

Usage:
  ./scripts/sync/sync-holon-remote.sh [tags...] [options]

Tags (simple selectors):
  core   docker-compose + deploy + pull scripts
  all    everything above (default)

Options:
  --key PATH           SSH private key path (or env: KEY)
  --host USER@HOST     Remote SSH target (or env: HOST)
  --remote-base PATH   Remote base directory (or env: REMOTE_BASE)
  --dry-run            Print actions without transferring files
  -h, --help           Show this help

Examples:
  ./scripts/sync/sync-holon-remote.sh all --key "/c/Users/me/key.pem" --host ec2-user@1.2.3.4 --remote-base "~/holon/holon-experiments"
  KEY=... HOST=... REMOTE_BASE=... ./scripts/sync/sync-holon-remote.sh core
EOF
}

KEY="${KEY:-}"
HOST="${HOST:-}"
REMOTE_BASE="${REMOTE_BASE:-}"
DRY_RUN=0
declare -a TAGS=()

while (($#)); do
  case "$1" in
    core|all)
      TAGS+=("$1")
      shift
      ;;
    --key)
      KEY="${2:-}"
      shift 2
      ;;
    --host)
      HOST="${2:-}"
      shift 2
      ;;
    --remote-base)
      REMOTE_BASE="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ((${#TAGS[@]} == 0)); then
  TAGS=("all")
fi

if [[ -z "$KEY" || -z "$HOST" || -z "$REMOTE_BASE" ]]; then
  echo "Missing required connection options." >&2
  echo "Provide --key, --host, --remote-base (or KEY/HOST/REMOTE_BASE env vars)." >&2
  exit 1
fi

run_cmd() {
  if ((DRY_RUN)); then
    printf '[dry-run] '
    printf '%q ' "$@"
    echo
    return 0
  fi
  "$@"
}

copy_single() {
  local src="$1"
  local dst="$2"
  if [[ -f "$src" ]]; then
    echo "Copying $src -> $dst"
    run_cmd scp -i "$KEY" "$src" "$dst"
  else
    echo "Skipping missing file: $src"
  fi
}

need_core=0
for tag in "${TAGS[@]}"; do
  case "$tag" in
    all)  need_core=1 ;;
    core) need_core=1 ;;
  esac
done

echo "Preparing remote directory: $REMOTE_BASE"
run_cmd ssh -i "$KEY" "$HOST" "mkdir -p \"$REMOTE_BASE\""

if ((need_core)); then
  echo "=== Syncing: core ==="
  copy_single "examples/nexmark-remote-java/docker-compose.holon.yml" "$HOST:$REMOTE_BASE/"
  copy_single "examples/nexmark-remote-java/deploy.sh" "$HOST:$REMOTE_BASE/"
  copy_single "examples/nexmark-remote-java/holon-remote-pull.sh" "$HOST:$REMOTE_BASE/"
fi

if ((DRY_RUN)); then
  echo "[dry-run] Skipping remote file listing."
  exit 0
fi

echo "Verifying remote files..."
ssh -i "$KEY" "$HOST" "ls -lah \"$REMOTE_BASE\""

echo "Sync complete."
