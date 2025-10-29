FROM openjdk:11-jre-slim
WORKDIR /app

# 1) fat JAR
COPY "target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar" /app/holon-assembly.jar

# 2) GCP creds
COPY .gcp/gcs-service-account.json /app/.gcp/gcs-service-account.json
COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json

# 3) startup
COPY examples/nexmark-remote-java/docker/holon-node/start-nodes.sh /app/start-nodes.sh
RUN chmod +x /app/start-nodes.sh

ENTRYPOINT ["/app/start-nodes.sh"]
