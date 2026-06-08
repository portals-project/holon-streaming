#!/usr/bin/env bash
# Sourced by run-experiment.sh — do not execute directly.
# Phase 2: template listing/selection, knob defaults, summary, user confirmation.

TEMPLATE_DIR="$REPO_ROOT/examples/flink-remote-java/templates"

list_templates() {
  if [[ ! -d "$TEMPLATE_DIR" ]]; then
    err "No templates directory: $TEMPLATE_DIR"
    return 1
  fi
  shopt -s nullglob
  local files=("$TEMPLATE_DIR"/*.env)
  shopt -u nullglob
  if (( ${#files[@]} == 0 )); then
    warn "No templates found in $TEMPLATE_DIR"
    return 0
  fi
  printf '%-28s  %s\n' "TEMPLATE" "DESCRIPTION"
  printf '%-28s  %s\n' "--------" "-----------"
  for f in "${files[@]}"; do
    local key name desc
    key=$(basename "$f" .env)
    name=$(grep -E '^NAME=' "$f"        | head -n1 | sed -E 's/^NAME="?([^"]*)"?/\1/')
    desc=$(grep -E '^DESCRIPTION=' "$f" | head -n1 | sed -E 's/^DESCRIPTION="?([^"]*)"?/\1/')
    printf '%-28s  %s — %s\n' "$key" "$name" "$desc"
  done
}

if (( LIST_ONLY )); then
  list_templates
  exit 0
fi

# ------------------------- knob defaults ------------------------------------

declare -A KNOBS=(
  [QUERY]=q7
  [WORKLOAD]=7
  [PARALLELISM]=10
  [TASKMANAGER_COUNT]=5
  [TASKMANAGER_MEMORY]=4g
  [JOBMANAGER_MEMORY]=2g
  [EVENTS_PER_SECOND]=10000
  [N_NODES]=10
  [PARTITIONS_PER_NODE]=2
  [PRODUCER_COUNT]=20
  [PRODUCER_SLEEP_MS]=100
  [PRODUCER_BATCH_SIZE]=1024
  [WINDOW_LENGTH]=10000
  [RUNTIME]=800000
  [ENABLE_RATE_LIMIT]=false
  [PROCESSING_RATE_LIMIT]=10000
  [GARBAGE_COLLECTION_OFFSET]=10
  [FLUSH_THRESHOLD]=500
  [TOPIC_METRICS_INTERVAL]=5000
)
TEMPLATE_NAME_LABEL="Custom"
TEMPLATE_DESC=""
TEMPLATE_FILE=""

source_template() {
  local file="$1"
  TEMPLATE_FILE="$file"
  # shellcheck disable=SC1090
  source "$file"
  TEMPLATE_NAME_LABEL="${NAME:-$(basename "$file" .env)}"
  TEMPLATE_DESC="${DESCRIPTION:-}"
  for k in "${!KNOBS[@]}"; do
    local v="${!k:-}"
    if [[ -n "$v" ]]; then
      KNOBS[$k]="$v"
    fi
  done
}

prompt_custom() {
  echo
  log "Custom configuration — press Enter to accept default."
  for k in QUERY WORKLOAD PARALLELISM TASKMANAGER_COUNT TASKMANAGER_MEMORY JOBMANAGER_MEMORY \
           EVENTS_PER_SECOND N_NODES PARTITIONS_PER_NODE \
           PRODUCER_COUNT PRODUCER_SLEEP_MS PRODUCER_BATCH_SIZE WINDOW_LENGTH RUNTIME \
           ENABLE_RATE_LIMIT PROCESSING_RATE_LIMIT GARBAGE_COLLECTION_OFFSET \
           FLUSH_THRESHOLD TOPIC_METRICS_INTERVAL; do
    local cur="${KNOBS[$k]}"
    local input
    read -r -p "  $k [$cur]: " input
    if [[ -n "$input" ]]; then
      KNOBS[$k]="$input"
    fi
  done
  TEMPLATE_NAME_LABEL="Custom"
  TEMPLATE_DESC="user-customized run"
}

select_template() {
  if [[ -n "$TEMPLATE_NAME" ]]; then
    local f="$TEMPLATE_DIR/$TEMPLATE_NAME.env"
    if [[ ! -f "$f" ]]; then
      err "Template not found: $f"
      exit 1
    fi
    source_template "$f"
    return
  fi

  shopt -s nullglob
  local files=("$TEMPLATE_DIR"/*.env)
  shopt -u nullglob

  echo
  log "Available templates:"
  local i=1
  for f in "${files[@]}"; do
    local n d
    n=$(grep -E '^NAME=' "$f"        | head -n1 | sed -E 's/^NAME="?([^"]*)"?/\1/')
    d=$(grep -E '^DESCRIPTION=' "$f" | head -n1 | sed -E 's/^DESCRIPTION="?([^"]*)"?/\1/')
    printf "    %2d) %-22s %s\n" "$i" "$n" "$d"
    i=$((i+1))
  done
  printf "    %2d) %s\n" "$i" "[Custom — prompt for each value]"
  echo

  local choice
  while true; do
    read -r -p "Select template [1-$i]: " choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= i )); then
      break
    fi
    warn "Invalid choice."
  done

  if (( choice == i )); then
    prompt_custom
  else
    source_template "${files[$((choice-1))]}"
  fi
}

select_template

# Validate WORKLOAD ↔ QUERY family
qfam=$(echo "${KNOBS[QUERY]}" | sed -E 's/^q([0-9]+).*/\1/')
if [[ "$qfam" =~ ^[0-9]+$ ]] && [[ "$qfam" != "${KNOBS[WORKLOAD]}" ]]; then
  warn "WORKLOAD (${KNOBS[WORKLOAD]}) does not match QUERY family (q$qfam)."
  read -r -p "  Continue anyway? [y/N]: " yn
  [[ "$yn" =~ ^[Yy]$ ]] || exit 1
fi

# Experiment summary
echo
log "Experiment summary"
printf '   %-12s : %s\n' "Template" "$TEMPLATE_NAME_LABEL"
[[ -n "$TEMPLATE_DESC" ]] && printf '   %-12s : %s\n' "Description" "$TEMPLATE_DESC"
echo "   ----"
for k in QUERY WORKLOAD PARALLELISM TASKMANAGER_COUNT TASKMANAGER_MEMORY JOBMANAGER_MEMORY \
         EVENTS_PER_SECOND N_NODES PARTITIONS_PER_NODE \
         PRODUCER_COUNT PRODUCER_SLEEP_MS PRODUCER_BATCH_SIZE WINDOW_LENGTH RUNTIME \
         ENABLE_RATE_LIMIT PROCESSING_RATE_LIMIT; do
  printf '   %-22s : %s\n' "$k" "${KNOBS[$k]}"
done
echo

read -r -p "Proceed with this run? [Y/n]: " yn
yn="${yn:-Y}"
[[ "$yn" =~ ^[Yy]$ ]] || { log "Aborted by user."; exit 0; }
