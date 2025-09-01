#!/usr/bin/env bash
set -euo pipefail

echo "Compiling…"
sbt compile

echo "Launching $NUM_NODES HolonNode instances…"
for i in $(seq 0 $((NUM_NODES-1))); do
  sbt "runMain holon.example.nexmark.HolonNode $i" &
done


wait
