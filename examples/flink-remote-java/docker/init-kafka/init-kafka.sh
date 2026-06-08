#!/bin/bash
# pipefail requires bash; do not run this file with `sh` (POSIX sh rejects `set -o pipefail`).
set -euo pipefail

# 1) Wait for Kafka to be up
until kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
  echo "Waiting for Kafka…"
  sleep 1
done

echo "N_NODES: $N_NODES"
echo "PARTITIONS_PER_NODE: $PARTITIONS_PER_NODE"
TOTAL_PARTITIONS=$((N_NODES * PARTITIONS_PER_NODE))
echo "TOTAL_PARTITIONS: $TOTAL_PARTITIONS"

# 2) Try to ALTER the existing topic to the right partition count.
#    This will succeed if “input” exists (and only increases partitions).
#    If it doesn’t exist yet, the command will error out—but we swallow that.
kafka-topics --bootstrap-server kafka:9092 \
  --alter --topic input \
  --partitions ${TOTAL_PARTITIONS} \
  || echo "Topic 'input' did not exist yet, moving on to create…"

# 3) Create it if it still doesn’t exist
kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic input \
  --replication-factor 1 \
  --partitions ${TOTAL_PARTITIONS} \
  --config message.timestamp.type=LogAppendTime

# Q4 sinks via upsert-kafka, which requires a compacted topic to produce
# correct keyed-upsert semantics. Q0/Q7 use the regular kafka connector
# with null-keyed messages, so a plain append topic is correct for them.
if [ "${WORKLOAD:-}" = "4" ]; then
  echo "Creating compacted 'output' topic (Q4 / upsert-kafka)"
  kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic output \
    --replication-factor 1 \
    --partitions 1 \
    --config cleanup.policy=compact \
    --config message.timestamp.type=LogAppendTime
else
  echo "Creating regular 'output' topic"
  kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic output \
    --replication-factor 1 \
    --partitions 1 \
    --config message.timestamp.type=LogAppendTime
fi

echo "Successfully created topics:"
kafka-topics --bootstrap-server kafka:9092 --list
