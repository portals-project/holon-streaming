#!/usr/bin/env bash
set -euo pipefail

# Launch PRODUCER_COUNT NexmarkProducerPerPartition instances from the compiled
# classpath. Each producer announces itself to the HTTP start gate (START_GATE_URL)
# and blocks on /ready, so data only flows once every producer is up.
#
# expects PRODUCER_COUNT & a compiled /app/target/classpath.txt (from build.sh)

CP="$(cat /app/target/classpath.txt)"

echo "Launching $PRODUCER_COUNT NexmarkProducer instances…"
for i in $(seq 0 $((PRODUCER_COUNT-1))); do
  java \
     -Dpekko.remote.artery.canonical.port=0 \
     -Dpekko.remote.artery.bind.port=0 \
     -cp "$CP" \
     holon.examples.nexmark.data.NexmarkProducerPerPartition \
     "$i" &
done
wait
