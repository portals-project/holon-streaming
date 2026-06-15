#!/usr/bin/env bash
set -euo pipefail

# Launch the OutputConsumer from the compiled classpath.
# expects a compiled /app/target/classpath.txt (produced by build.sh)

CP="$(cat /app/target/classpath.txt)"

echo "Launching OutputConsumer…"
java \
  -Dpekko.remote.artery.canonical.port=0 \
  -Dpekko.remote.artery.bind.port=0 \
  -cp "$CP" \
  holon.examples.nexmark.consumers.OutputConsumer
