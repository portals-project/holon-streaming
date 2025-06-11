#!/bin/sh

# wait until Kafka is reachable
until kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
  echo "Waiting for Kafka to be available..."
  sleep 1
done

echo "N_NODES: $N_NODES"

kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic input --replication-factor 1 --partitions $N_NODES --config message.timestamp.type=LogAppendTime
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic output --replication-factor 1 --partitions $N_NODES --config message.timestamp.type=LogAppendTime

echo "Successfully created topics:"
kafka-topics --bootstrap-server kafka:9092 --list