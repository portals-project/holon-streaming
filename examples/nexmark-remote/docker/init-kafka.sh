#!/bin/sh

# Runs inside the apache/kafka image (bind-mounted), which ships the CLI at
# /opt/kafka/bin. Creates the Holon topics on the in-cluster listener kafka:9093.
KT=/opt/kafka/bin/kafka-topics.sh

# wait until Kafka is reachable
until "$KT" --bootstrap-server kafka:9093 --list > /dev/null 2>&1; do
  echo "Waiting for Kafka to be available..."
  sleep 1
done

echo "N_NODES: $N_NODES"
echo "PARTITIONS_PER_NODE: $PARTITIONS_PER_NODE"
TOTAL_PARTITIONS=$((N_NODES * PARTITIONS_PER_NODE))
echo "TOTAL_PARTITIONS: $TOTAL_PARTITIONS"

"$KT" --bootstrap-server kafka:9093 --create --if-not-exists --topic input --replication-factor 1 --partitions $TOTAL_PARTITIONS --config message.timestamp.type=LogAppendTime
"$KT" --bootstrap-server kafka:9093 --create --if-not-exists --topic broadcast --replication-factor 1 --partitions 1 --config message.timestamp.type=LogAppendTime
"$KT" --bootstrap-server kafka:9093 --create --if-not-exists --topic control --replication-factor 1 --partitions 1 --config message.timestamp.type=LogAppendTime
"$KT" --bootstrap-server kafka:9093 --create --if-not-exists --topic output --replication-factor 1 --partitions 1 --config message.timestamp.type=LogAppendTime

echo "Successfully created topics:"
"$KT" --bootstrap-server kafka:9093 --list
