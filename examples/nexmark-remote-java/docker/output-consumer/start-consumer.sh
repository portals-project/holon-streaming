#!/usr/bin/env bash
set -euo pipefail

echo "Launching OutputConsumer…"
java \
  -Dpekko.remote.artery.canonical.port=0 \
  -Dpekko.remote.artery.bind.port=0 \
  -cp /app/holon-assembly.jar \
  holon.examples.nexmark.consumers.OutputConsumer