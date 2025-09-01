#!/bin/sh
set -e

echo "==> STEP 1: Tear down any old containers"
docker compose down

echo "==> STEP 2: Start ONLY Kafka"
docker compose up -d kafka

echo "==> STEP 3: Wait 20s for Kafka to bind 9092"
sleep 20
echo " ✓ Kafka is listening on 9092"

echo "==> STEP 4: Create 'input', 'per_auction_max', and 'output' topics"
docker compose up --build init-kafka

echo "==> STEP 5: Bring up Flink (JobManager + TaskManagers)"
docker compose up -d --build

echo "==> STEP 6: Wait for Flink JobManager to be healthy"
sleep 20
echo " ✓ Flink JobManager is up"

echo "==> STEP 7a: Submit Stage 1 (JOIN + MAX) with parallelism = 5"
#Q0
# docker exec -i flink-jobmanager bash -c "bin/sql-client.sh -Dparallelism.default=10 -f /opt/flink/queries/q0.sql"
# docker exec -i flink-jobmanager bash -c "bin/sql-client.sh -Dparallelism.default=10 -f /opt/flink/queries/q7.sql"

# Q4
docker exec -i flink-jobmanager bash -c "bin/sql-client.sh -Dparallelism.default=5 -f /opt/flink/queries/q4_stage_1.sql"
echo " ✓ Stage 1 submitted."

echo "==> STEP 7b: Submit Stage 2 (AVG + OUTPUT) with parallelism = 5"
docker exec -i flink-jobmanager bash -c "bin/sql-client.sh -Dparallelism.default=5 -f /opt/flink/queries/q4_stage_2.sql"
echo " ✓ Stage 2 submitted."

echo "==> ALL DONE. Flink Q4 (split job) is RUNNING"
