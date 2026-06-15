#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

usage() {
  cat <<'EOF'
Sync HOLON (nexmark-remote, SBT "on-the-fly") deployment files to a remote instance.

Unlike the assembly flow (sync-holon-remote.sh), this variant compiles on the box,
so the Scala SOURCE TREE is synced too (src/, build.sbt, project/) alongside the
compose file, deploy/pull scripts and the docker/ helper scripts. The containers
bind-mount this source and compile it at run time.

Usage:
  ./scripts/sync/sync-holon-sbt-remote.sh [tags...] [options]

Tags (simple selectors):
  core     docker-compose + deploy + pull + docker/ helper scripts
  source   src/, build.sbt, project/  (the Scala source compiled on the box)
  all      everything above (default)

Options:
  --key PATH           SSH private key path (or env: KEY)
  --host USER@HOST     Remote SSH target (or env: HOST)
  --remote-base PATH   Remote base directory (or env: REMOTE_BASE)
  --dry-run            Print actions without transferring files
  -h, --help           Show this help

Examples:
  ./scripts/sync/sync-holon-sbt-remote.sh all --key "/c/Users/me/key.pem" --host ec2-user@1.2.3.4 --remote-base "~/holon/holon-experiments"
  KEY=... HOST=... REMOTE_BASE=... ./scripts/sync/sync-holon-sbt-remote.sh core
EOF
}

KEY="${KEY:-}"
HOST="${HOST:-}"
REMOTE_BASE="${REMOTE_BASE:-}"
DRY_RUN=0
declare -a TAGS=()

while (($#)); do
  case "$1" in
    core|source|all)
      TAGS+=("$1")
      shift
      ;;
    --key)        KEY="${2:-}"; shift 2 ;;
    --host)       HOST="${2:-}"; shift 2 ;;
    --remote-base) REMOTE_BASE="${2:-}"; shift 2 ;;
    --dry-run)    DRY_RUN=1; shift ;;
    -h|--help)    usage; exit 0 ;;
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
  local src="$1" dst="$2"
  if [[ -f "$src" ]]; then
    echo "Copying $src -> $dst"
    run_cmd scp -i "$KEY" "$src" "$dst"
  else
    echo "Skipping missing file: $src"
  fi
}

# Recursive directory copy that ALWAYS excludes build/IDE caches (target/, .bsp,
# .bloop, .metals, .idea) so we ship sources, not local artifacts — important
# because stale project/target meta-build output would otherwise be compiled into
# the remote build. Prefers rsync; falls back to tar-over-ssh (excludes honoured,
# tar exists in Git Bash and on the box) — NOT bare `scp -r`, which can't exclude.
copy_dir() {
  local src="$1" dst="$2"   # dst is $HOST:$REMOTE_BASE/<name>
  if [[ ! -d "$src" ]]; then
    echo "Skipping missing dir: $src"
    return 0
  fi
  local parent base
  parent="$(dirname "$src")"
  base="$(basename "$src")"

  if command -v rsync >/dev/null 2>&1; then
    echo "Rsyncing $src/ -> $dst/"
    run_cmd rsync -az --delete \
      --exclude 'target' --exclude '.bsp' --exclude '.bloop' \
      --exclude '.metals' --exclude '.idea' --exclude '.git' \
      -e "ssh -i $KEY" "$src/" "$dst/"
    return
  fi

  # tar exclude patterns. GNU tar's `*` does not cross `/`, so list the meta-build
  # depths explicitly; bsdtar (Git Bash) matches all via the `*/` forms anyway.
  local -a ex=(
    --exclude='target' --exclude='*/target' --exclude='*/*/target' --exclude='*/*/*/target'
    --exclude='.bsp' --exclude='*/.bsp' --exclude='.bloop' --exclude='*/.bloop'
    --exclude='.metals' --exclude='*/.metals' --exclude='.idea' --exclude='*/.idea'
    --exclude='.git' --exclude='*/.git'
  )

  if ! command -v tar >/dev/null 2>&1; then
    echo "!! neither rsync nor tar available — falling back to scp -r (no excludes)" >&2
    run_cmd scp -i "$KEY" -r "$src" "$dst"
    return
  fi

  echo "tar-over-ssh $src -> $HOST:$REMOTE_BASE/$base (excluding build/IDE caches)"
  if ((DRY_RUN)); then
    printf '[dry-run] tar -C %q %s -czf - %q | ssh -i %q %q "tar -C %q -xzf -"\n' \
      "$parent" "${ex[*]}" "$base" "$KEY" "$HOST" "$REMOTE_BASE"
    return 0
  fi
  tar -C "$parent" "${ex[@]}" -czf - "$base" \
    | ssh -i "$KEY" "$HOST" "mkdir -p '$REMOTE_BASE' && tar -C '$REMOTE_BASE' -xzf -"
}

need_core=0
need_source=0
for tag in "${TAGS[@]}"; do
  case "$tag" in
    all)    need_core=1; need_source=1 ;;
    core)   need_core=1 ;;
    source) need_source=1 ;;
  esac
done

echo "Preparing remote directory: $REMOTE_BASE"
run_cmd ssh -i "$KEY" "$HOST" "mkdir -p \"$REMOTE_BASE\""

if ((need_core)); then
  echo "=== Syncing: core ==="
  copy_single "examples/nexmark-remote/docker-compose.holon-sbt.yml" "$HOST:$REMOTE_BASE/"
  copy_single "examples/nexmark-remote/deploy.sh"                    "$HOST:$REMOTE_BASE/"
  copy_single "examples/nexmark-remote/holon-sbt-pull.sh"           "$HOST:$REMOTE_BASE/"
  # docker/ helper scripts (init-kafka.sh, build.sh, start-*.sh) — bind-mounted by compose.
  copy_dir   "examples/nexmark-remote/docker" "$HOST:$REMOTE_BASE/docker"
fi

if ((need_source)); then
  echo "=== Syncing: source ==="
  copy_single "build.sbt"   "$HOST:$REMOTE_BASE/"
  copy_dir    "project"     "$HOST:$REMOTE_BASE/project"
  copy_dir    "src"         "$HOST:$REMOTE_BASE/src"
fi

if ((DRY_RUN)); then
  echo "[dry-run] Skipping remote file listing."
  exit 0
fi

echo "Verifying remote files..."
ssh -i "$KEY" "$HOST" "ls -lah \"$REMOTE_BASE\""

echo "Sync complete."
