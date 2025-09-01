#!/usr/bin/env bash
set -euo pipefail

# expects N_NODES & KAFKA_BOOTSTRAP_SERVERS
echo "Launching $N_NODES HolonNode instances…"
for i in $(seq 0 $((N_NODES-1))); do
  java \
      -Dpekko.remote.artery.canonical.port=0 \
      -Dpekko.remote.artery.bind.port=0 \
      -cp /app/holon-assembly.jar \
      holon.example.nexmark.HolonNode \
      "$i" &
done
wait
