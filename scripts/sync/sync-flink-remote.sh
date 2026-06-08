#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

usage() {
  cat <<'EOF'
Sync Flink remote deployment files to an external instance.

Usage:
  ./scripts/sync/sync-flink-remote.sh [tags...] [options]

Tags (simple selectors):
  core         docker-compose + deploy scripts
  queries      SQL query files
  connectors   Flink connector JARs
  all          everything above (default)

Options:
  --key PATH           SSH private key path (or env: KEY)
  --host USER@HOST     Remote SSH target (or env: HOST)
  --remote-base PATH   Remote base directory (or env: REMOTE_BASE)
  --dry-run            Print actions without transferring files
  -h, --help           Show this help

Examples:
  ./scripts/sync/sync-flink-remote.sh all --key "/c/Users/me/key.pem" --host ec2-user@1.2.3.4 --remote-base "~/holon/flink-experiments"
  KEY=... HOST=... REMOTE_BASE=... ./scripts/sync/sync-flink-remote.sh core queries
EOF
}

KEY="${KEY:-}"
HOST="${HOST:-}"
REMOTE_BASE="${REMOTE_BASE:-}"
DRY_RUN=0
declare -a TAGS=()

while (($#)); do
  case "$1" in
    core|queries|connectors|all)
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

copy_glob() {
  local pattern="$1"
  local dst="$2"
  shopt -s nullglob
  local matches=($pattern)
  shopt -u nullglob

  if ((${#matches[@]} == 0)); then
    echo "No files matched: $pattern"
    return 0
  fi

  for f in "${matches[@]}"; do
    copy_single "$f" "$dst"
  done
}

need_core=0
need_queries=0
need_connectors=0

for tag in "${TAGS[@]}"; do
  case "$tag" in
    all)
      need_core=1
      need_queries=1
      need_connectors=1
      ;;
    core) need_core=1 ;;
    queries) need_queries=1 ;;
    connectors) need_connectors=1 ;;
  esac
done

echo "Preparing remote directories under: $REMOTE_BASE"
mkdir_cmd="mkdir -p \"$REMOTE_BASE\""
if ((need_queries)); then
  mkdir_cmd="$mkdir_cmd \"$REMOTE_BASE/queries\""
fi
if ((need_connectors)); then
  mkdir_cmd="$mkdir_cmd \"$REMOTE_BASE/flink-connectors\""
fi
run_cmd ssh -i "$KEY" "$HOST" "$mkdir_cmd"

if ((need_core)); then
  echo "=== Syncing: core ==="
  copy_single "examples/flink-remote-java/docker-compose.flink.yml" "$HOST:$REMOTE_BASE/"
  copy_single "examples/flink-remote-java/deploy.sh" "$HOST:$REMOTE_BASE/"
  copy_single "examples/flink-remote-java/flink-remote-pull.sh" "$HOST:$REMOTE_BASE/"
fi

if ((need_queries)); then
  echo "=== Syncing: queries ==="
  copy_glob "examples/flink-remote-java/queries/*.sql" "$HOST:$REMOTE_BASE/queries/"
fi

if ((need_connectors)); then
  echo "=== Syncing: connectors ==="
  copy_glob "examples/flink-remote-java/flink-connectors/*.jar" "$HOST:$REMOTE_BASE/flink-connectors/"
fi

if ((DRY_RUN)); then
  echo "[dry-run] Skipping remote file listing."
  exit 0
fi

echo "Verifying remote files..."
verify_cmd="ls -lah \"$REMOTE_BASE\""
if ((need_queries)); then
  verify_cmd="$verify_cmd && ls -lah \"$REMOTE_BASE/queries\""
fi
if ((need_connectors)); then
  verify_cmd="$verify_cmd && ls -lah \"$REMOTE_BASE/flink-connectors\""
fi
ssh -i "$KEY" "$HOST" "$verify_cmd"

echo "Sync complete."
