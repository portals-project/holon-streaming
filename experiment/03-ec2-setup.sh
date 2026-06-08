#!/usr/bin/env bash
# Sourced by run-experiment.sh — do not execute directly.
# Phase 3: start EC2, wait for SSH, clean remote, sync files, pre-pull images, verify.

SYNC_SCRIPT="$REPO_ROOT/scripts/sync/sync-flink-remote.sh"

# Start instance
log "Starting EC2 instance: $INSTANCE_ID ($AWS_REGION)"
run_cmd aws ec2 start-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" >/dev/null
INSTANCE_STARTED=1

if (( ! DRY_RUN )); then
  aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"
  PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  if [[ -z "$PUBLIC_IP" || "$PUBLIC_IP" == "None" ]]; then
    err "Could not resolve public IP for $INSTANCE_ID"
    exit 1
  fi
else
  PUBLIC_IP="DRY-RUN-IP"
fi
HOST="$REMOTE_USER@$PUBLIC_IP"
ok "Instance running at $PUBLIC_IP"

# Web UIs — printed early so the user can click while the stack boots
# (links go live once docker compose finishes, ~30-60s after this point).
echo
log "Web UIs (clickable; live once stack is up):"
printf '     %-12s http://%s:8081\n' "Flink:"      "$PUBLIC_IP"
printf '     %-12s http://%s:8080\n' "Kafka:"      "$PUBLIC_IP"
printf '     %-12s http://%s:8090\n' "Start gate:" "$PUBLIC_IP"
echo

SSH_OPTS=(-i "$KEY" -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)

ssh_remote()   { run_cmd ssh "${SSH_OPTS[@]}" "$HOST" "$@"; }
ssh_remote_q() { ssh "${SSH_OPTS[@]}" "$HOST" "$@"; }

# SSH readiness wait
log "Waiting for SSH on $PUBLIC_IP"
if (( ! DRY_RUN )); then
  ssh_err=""
  for attempt in $(seq 1 36); do
    ssh_err=$(ssh "${SSH_OPTS[@]}" "$HOST" 'echo ready' 2>&1) && {
      ok "SSH up after $(( (attempt - 1) * 5 ))s"
      break
    }
    if (( attempt == 36 )); then
      echo
      err "SSH never came up (3 min timeout). Last error:"
      printf '%s\n' "$ssh_err" | sed 's/^/    /'
      exit 1
    fi
    printf '.'
    sleep 5
  done
fi

# Mount local NVMe SSD if present. Instance-store NVMe is ephemeral —
# wiped on every stop/start — so this runs at the top of every experiment.
# No-op on instances without local NVMe (e.g. r6i.*). See SETUP.md §1.
log "Mounting local NVMe SSD (if present)"
nvme_setup=$(cat <<'EOF'
set -e
DEV=$(lsblk -dno NAME,TYPE | awk '$2=="disk" && $1 ~ /^nvme[1-9]/{print "/dev/"$1; exit}')
if [ -z "$DEV" ]; then
  echo "No local NVMe device — using root EBS"
  exit 0
fi
if mountpoint -q /mnt/nvme; then
  echo "/mnt/nvme already mounted ($(findmnt -no SOURCE /mnt/nvme))"
  exit 0
fi
sudo mkfs.xfs -f "$DEV" >/dev/null
sudo mkdir -p /mnt/nvme
sudo mount "$DEV" /mnt/nvme
sudo chown ec2-user:ec2-user /mnt/nvme
echo "Mounted $DEV at /mnt/nvme"
EOF
)
if (( DRY_RUN )); then
  run_cmd ssh "${SSH_OPTS[@]}" "$HOST" "$nvme_setup"
else
  ssh "${SSH_OPTS[@]}" "$HOST" "$nvme_setup" | sed 's/^/   /'
fi

# Warn if NVMe is mounted but REMOTE_BASE doesn't use it.
if (( ! DRY_RUN )); then
  nvme_present=$(ssh "${SSH_OPTS[@]}" "$HOST" 'mountpoint -q /mnt/nvme && echo 1 || echo 0')
  if [[ "$nvme_present" == "1" && "$REMOTE_BASE" != /mnt/nvme* ]]; then
    warn "Local NVMe is mounted at /mnt/nvme but REMOTE_BASE=$REMOTE_BASE is on the root EBS."
    warn "For faster checkpoints, set REMOTE_BASE=/mnt/nvme/holon/flink-experiments in .holon.env."
  fi
fi

# Clean slate
# flink-checkpoints/{checkpoints,savepoints} are pre-created so Flink doesn't
# have to mkdir them inside the container (which can fail under SELinux or
# user-namespace remapping). 777 keeps it writable regardless of which
# UID the container ends up running as.
log "Wiping remote $REMOTE_BASE for a clean run"
ssh_remote "sudo rm -rf '$REMOTE_BASE' && mkdir -p '$REMOTE_BASE/queries' '$REMOTE_BASE/flink-connectors' '$REMOTE_BASE/flink-logs' '$REMOTE_BASE/flink-checkpoints/checkpoints' '$REMOTE_BASE/flink-checkpoints/savepoints' && chmod -R 777 '$REMOTE_BASE/flink-checkpoints' '$REMOTE_BASE/flink-logs'"

# File sync
log "Syncing files to $HOST:$REMOTE_BASE"
sync_args=(all --key "$KEY" --host "$HOST" --remote-base "$REMOTE_BASE")
(( DRY_RUN )) && sync_args+=(--dry-run)
( cd "$REPO_ROOT" && bash "$SYNC_SCRIPT" "${sync_args[@]}" )

# Pre-pull Docker images
log "Pre-pulling Docker images on remote"
if (( DRY_RUN )); then
  run_cmd ssh "${SSH_OPTS[@]}" "$HOST" "cd '$REMOTE_BASE' && bash flink-remote-pull.sh"
else
  ssh "${SSH_OPTS[@]}" "$HOST" "cd '$REMOTE_BASE' && bash flink-remote-pull.sh" | sed 's/^/   /'
fi

# Verify remote files and detect compose command
log "Verifying remote files"
verify_script=$(cat <<EOF
set -e
cd '$REMOTE_BASE'
for f in docker-compose.flink.yml deploy.sh flink-remote-pull.sh; do
  [[ -f "\$f" ]] || { echo "MISSING: \$f"; exit 1; }
done
ls queries/*.sql >/dev/null 2>&1 || { echo "MISSING: any .sql in queries/"; exit 1; }
[[ -f "queries/${KNOBS[QUERY]}.sql" ]] || { echo "MISSING: queries/${KNOBS[QUERY]}.sql"; exit 1; }
chmod +x deploy.sh flink-remote-pull.sh
if docker compose version >/dev/null 2>&1; then
  echo COMPOSE=docker_compose
elif command -v docker-compose >/dev/null 2>&1; then
  echo COMPOSE=docker_dash_compose
else
  echo "MISSING: docker compose"; exit 1
fi
echo OK
EOF
)
if (( DRY_RUN )); then
  run_cmd ssh "${SSH_OPTS[@]}" "$HOST" "<verify script>"
  REMOTE_COMPOSE="docker compose"
else
  verify_out=$(ssh "${SSH_OPTS[@]}" "$HOST" "$verify_script")
  echo "$verify_out" | sed 's/^/   /'
  if [[ "$verify_out" == *docker_dash_compose* ]]; then
    REMOTE_COMPOSE="docker-compose"
  else
    REMOTE_COMPOSE="docker compose"
  fi
fi
ok "Remote verified (compose: $REMOTE_COMPOSE)"
