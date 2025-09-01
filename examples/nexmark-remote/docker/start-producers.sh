#!/usr/bin/env bash
set -euo pipefail

echo "Compiling…"
sbt compile

echo "Launching $PRODUCER_COUNT NexmarkProducer instances…"
for i in $(seq 0 $((PRODUCER_COUNT-1))); do
    sbt "runMain holon.example.nexmark.NexmarkProducerPerPartition $i" &
done


wait
