#!/bin/sh
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

# FOR Q4
kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic auctionmax \
  --replication-factor 1 \
  --partitions 1 \
  --config cleanup.policy=compact \
  --config message.timestamp.type=LogAppendTime

## Create the output topic **with compaction** so it can be used by upsert-kafka (needed for Q4)
#kafka-topics --bootstrap-server kafka:9092 \
#  --create --if-not-exists \
#  --topic output \
#  --replication-factor 1 \
#  --partitions 1 \
#  --config cleanup.policy=compact \
#  --config message.timestamp.type=LogAppendTime

# Create regular output topic (no compaction needed) uncomment for any other query
kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic output \
  --replication-factor 1 \
  --partitions 1 \
  --config message.timestamp.type=LogAppendTime

echo "Successfully created topics:"
kafka-topics --bootstrap-server kafka:9092 --list
