#!/bin/sh

# wait until Kafka is reachable
until kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
  echo "Waiting for Kafka to be available..."
  sleep 1
done

echo "N_NODES: $N_NODES"

# Create the input topic (no compaction needed)
kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic input \
  --replication-factor 1 \
  --partitions $N_NODES \
  --config message.timestamp.type=LogAppendTime

# FOR Q4
kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic auctionmax \
  --replication-factor 1 \
  --partitions $N_NODES \
  --config cleanup.policy=compact \
  --config message.timestamp.type=LogAppendTime


# Create the output topic **with compaction** so it can be used by upsert-kafka
kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic output \
  --replication-factor 1 \
  --partitions $N_NODES \
  --config cleanup.policy=compact \
  --config message.timestamp.type=LogAppendTime

echo "Successfully created topics:"
kafka-topics --bootstrap-server kafka:9092 --list
