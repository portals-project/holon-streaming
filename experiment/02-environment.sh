#!/usr/bin/env bash
# Sourced by run-experiment.sh — do not execute directly.
# Phase 2: select the platform (Flink / Holon). Templates are filtered by this
# in the next phase. Keep this minimal — future sub-types/variants slot in here.

TEMPLATE_DIR="$REPO_ROOT/templates"

# Supported platforms. Add new ones here as they come online.
SUPPORTED_PLATFORMS=(flink holon)

is_supported_platform() {
  local p="$1"
  for s in "${SUPPORTED_PLATFORMS[@]}"; do
    [[ "$s" == "$p" ]] && return 0
  done
  return 1
}

# Read the PLATFORM field out of a template .env, same extraction style as 03.
platform_of_template() {
  grep -E '^PLATFORM=' "$1" | head -n1 | sed -E 's/^PLATFORM="?([^"]*)"?/\1/'
}

# List-only mode is handled entirely by the template phase; nothing to select.
if (( LIST_ONLY )); then
  return 0 2>/dev/null || exit 0
fi

# 1) Explicit --platform wins.
if [[ -n "$PLATFORM" ]]; then
  PLATFORM=$(echo "$PLATFORM" | tr '[:upper:]' '[:lower:]')
  if ! is_supported_platform "$PLATFORM"; then
    err "Unknown platform: $PLATFORM (supported: ${SUPPORTED_PLATFORMS[*]})"
    exit 1
  fi
  log "Platform: $PLATFORM (from --platform)"

# 2) --template implies a platform — read it so the non-interactive path needs no prompt.
elif [[ -n "$TEMPLATE_NAME" ]]; then
  tfile="$TEMPLATE_DIR/$TEMPLATE_NAME.env"
  if [[ ! -f "$tfile" ]]; then
    err "Template not found: $tfile"
    exit 1
  fi
  PLATFORM=$(platform_of_template "$tfile")
  PLATFORM="${PLATFORM:-flink}"   # templates predating PLATFORM= are Flink
  log "Platform: $PLATFORM (from template $TEMPLATE_NAME)"

# 3) Interactive selection.
else
  echo
  log "Select platform:"
  i=1
  for p in "${SUPPORTED_PLATFORMS[@]}"; do
    printf "    %2d) %s\n" "$i" "$p"
    i=$((i+1))
  done
  echo
  while true; do
    read -r -p "Select platform [1-${#SUPPORTED_PLATFORMS[@]}]: " choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#SUPPORTED_PLATFORMS[@]} )); then
      PLATFORM="${SUPPORTED_PLATFORMS[$((choice-1))]}"
      break
    fi
    warn "Invalid choice."
  done
  ok "Platform: $PLATFORM"
fi
