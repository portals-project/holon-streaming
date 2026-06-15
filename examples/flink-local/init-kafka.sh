#!/bin/sh

# wait until Kafka is reachable
until kafka-topics --bootstrap-server kafka:9093 --list > /dev/null 2>&1; do
  echo "Waiting for Kafka to be available..."
  sleep 1
done

echo "N_NODES: $N_NODES"
echo "PARTITIONS_PER_NODE: $PARTITIONS_PER_NODE"
TOTAL_PARTITIONS=$((N_NODES * PARTITIONS_PER_NODE))
echo "TOTAL_PARTITIONS: $TOTAL_PARTITIONS"

kafka-topics --bootstrap-server kafka:9093 --create --if-not-exists --topic input --replication-factor 1 --partitions $TOTAL_PARTITIONS --config message.timestamp.type=LogAppendTime
kafka-topics --bootstrap-server kafka:9093 --create --if-not-exists --topic output --replication-factor 1 --partitions 1 --config message.timestamp.type=LogAppendTime

echo "Successfully created topics:"
kafka-topics --bootstrap-server kafka:9093 --list