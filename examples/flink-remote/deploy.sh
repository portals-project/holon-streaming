#!/bin/sh
set -e

echo "==> STEP 1: Tear down any old containers"
docker compose down

echo "==> STEP 2: Start ONLY Kafka"
docker compose up -d kafka

echo "==> STEP 3: Wait 20s for Kafka to bind 9092 (used for debugging)"
sleep 20
echo " ✓ Kafka is listening on 9092"

echo "==> STEP 4: Create 'input' & 'output' topics"
docker compose up --build init-kafka

echo "==> STEP 5: Bring up Flink (JobManager + TaskManagers) and other services using build tag (remove if no code changes)"
docker compose up -d --build

echo "==> STEP 6: Wait for Flink JobManager to be healthy"
sleep 20
echo "    ✓ Flink JobManager is up"

echo "==> STEP 7: Submit query job via SQL‐client"
docker exec -i flink-jobmanager bash -c "bin/sql-client.sh -Dparallelism.default=2 -f /opt/flink/queries/q7.sql"
echo "    ✓ SQL job submitted (detached)."
sleep 5
echo "    ✓ Flink job is RUNNING"
echo "==> ALL DONE."

