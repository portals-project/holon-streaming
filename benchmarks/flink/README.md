# Deploy Flink on GKE

These are some basic instructions to follow to deploy and run Flink on Google Kubernetes Engine (GKE).

## Start Deployments
```bash
helm install fink-kafka ./flink-kafka-setup
helm install prod-con ./prod-con-chart
```

## Execute query
```bash
kubectl exec -it <pod_id> -- /bin/bash
chmod -R 777 /flink-checkpoints
./bin/sql-client.sh -Dparallelism.default=15  -f /opt/flink/queries/q7.sql

----
docker exec -it flink-jobmanager ./bin/sql-client.sh -Dparallelism.default=5  -f /opt/flink/queries/q7.sql
```

## Upload custom docker image

```bash
docker build -t gcr.io/<your-project-id>/flink-custom:latest -f custom-flink-docker/Dockerfile .
docker push gcr.io/<your-project-id>/flink-custom:latest
```

**Producer & Consumer**
```bash
docker build -t gcr.io/<your-project-id>/nexmark-producer-json:latest -f examples/flink/docker/nexmark-producer-json/Dockerfile .
docker push gcr.io/<your-project-id>/nexmark-producer-json:latest
docker build -t gcr.io/<your-project-id>/output-consumer-json:latest -f examples/flink/docker/output-consumer-json/Dockerfile .
docker push gcr.io/<your-project-id>/output-consumer-json:latest
docker build -t gcr.io/<your-project-id>/log-append-output-consumer:latest -f examples/flink/docker/log-append-output-consumer/Dockerfile .
docker push gcr.io/<your-project-id>/log-append-output-consumer:latest
docker build -t gcr.io/<your-project-id>/log-append-output-consumer-threaded:latest -f examples/flink/docker/log-append-output-consumer/Dockerfile .
docker push gcr.io/<your-project-id>/log-append-output-consumer-threaded:latest
```

## Install flink chart
```bash
helm install flink-chart oci://registry-1.docker.io/bitnamicharts/flink -f flink-chart-values.yaml
```

## Port forwarding for Flink & Kafka UI 
```bash
kubectl port-forward service/jobmanager 8081:8081
kubectl port-forward service/kafka-ui-service 8080:8080
```


## Checkpoint storage in GCS

1. Create bucket
2. Create service account with storage admin role

```bash
kubectl create secret generic gcs-key \                                    
  --from-file=key.json=../../.gcp/flink-gcs-holon.json

```