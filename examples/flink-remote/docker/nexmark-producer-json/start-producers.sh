#!/usr/bin/env bash
set -euo pipefail

# expects PRODUCER_COUNT & KAFKA_BOOTSTRAP_SERVERS
echo "Launching $PRODUCER_COUNT NexmarkProducer instances…"
for i in $(seq 0 $((PRODUCER_COUNT-1))); do
  java \
     -Dpekko.remote.artery.canonical.port=0 \
     -Dpekko.remote.artery.bind.port=0 \
     -cp /app/holon-assembly.jar \
     holon.example.nexmark.NexmarkProducerPerPartition \
     "$i" &
done
wait