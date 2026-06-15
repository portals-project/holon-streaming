# Holon Google Kubernetes Engine Deployment

These are some basic instructions to follow to deploy the Holon application on Google Kubernetes Engine (GKE).

## Config setup
#### Window size vs broadcast offset
Q7: 5k + 1L
Q4: 25k + 1L
Q4: 15k + 12L
Q4: 10K + 13L


## Useful Commands

**Authenticate with Google Cloud**: `gcloud auth login`

**Generating Kubernetes files**
The `combine-kubernetes.yml` file was generated from our existing docker-compose using `kompose convert -f docker-compose.yml --stdout > combined-kubernetes.yaml`.

## Push Docker Image to Google Container Registry (GCR)
```bash
gcloud auth configure-docker
docker build -t gcr.io/<your-project-id>/nexmark-producer:latest -f examples/nexmark/docker/nexmark-producer/Dockerfile .
docker push gcr.io/<your-project-id>/nexmark-producer:latest

docker build -t gcr.io/<your-project-id>/output-consumer:latest -f examples/nexmark/docker/output-consumer/Dockerfile .
docker push gcr.io/<your-project-id>/output-consumer:latest

docker build -t gcr.io/<your-project-id>/holon-node:latest -f examples/nexmark/docker/holon-node/Dockerfile .
docker push gcr.io/<your-project-id>/holon-node:latest
```

## Deploying to Google Kubernetes Engine (GKE)

1. Create a GKE cluster in the Google Cloud Console or using the `gcloud` command line tool.
2. Install the Google Cloud SDK and authenticate with your Google account.
   ```bash
   gcloud auth login
   ```
3. Set the project ID and compute zone for your GKE cluster:
   ```bash
   gcloud config set project YOUR_PROJECT_ID
   gcloud config set compute/zone YOUR_COMPUTE_ZONE
   ```

4. Fetch the credentials for your GKE cluster. This command will configure `kubectl` to use the credentials for the specified cluster.
   ```bash
   gcloud container clusters get-credentials YOUR_CLUSTER_NAME --zone YOUR_COMPUTE_ZONE
   ```
   
5. Deploy the helm charts
    ```bash
    helm install kafka ./kafka-chart
    helm install holon ./holon-chart
  
    # Update the image tags in the deployment files
    helm upgrade holon ./holon-chart
    ```
   


These commands can be used to scale the deployments to the desired number of replicas.
```bash
kubectl scale deployment holon-node-0 holon-node-1 holon-node-2 --replicas=1
kubectl scale deployment nexmark-producer-0 --replicas=1 
```



OR: Apply the Kubernetes manifests to create the necessary resources in your GKE cluster.

   ```bash
    kubectl apply -f combined-kubernetes.yaml 

   ```