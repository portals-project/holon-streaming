#!/usr/bin/env bash
set -euo pipefail

# Launch N HolonNode instances from the classpath compiled by the holon-build
# service. Mirrors nexmark-remote-java/docker/holon-node/start-nodes.sh but uses
# the on-the-fly compiled classes (target/classpath.txt) instead of a fat JAR.
#
# expects NUM_NODES & a compiled /app/target/classpath.txt (produced by build.sh)

CP="$(cat /app/target/classpath.txt)"

echo "Launching $NUM_NODES HolonNode instances…"
for i in $(seq 0 $((NUM_NODES-1))); do
  java \
      -Dpekko.remote.artery.canonical.port=0 \
      -Dpekko.remote.artery.bind.port=0 \
      -cp "$CP" \
      holon.examples.nexmark.nodes.HolonNode \
      "$i" &
done
wait
