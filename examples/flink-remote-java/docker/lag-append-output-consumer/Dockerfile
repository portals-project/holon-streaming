FROM openjdk:11-jre-slim
WORKDIR /app

COPY "target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar" /app/holon-assembly.jar
COPY examples/flink-remote-java/docker/lag-append-output-consumer/start-lag-append-consumer.sh /app/start-lag-append-consumer.sh
RUN chmod +x /app/start-lag-append-consumer.sh

# Copy the service account JSON file
COPY .gcp/gcs-service-account.json /app/.gcp/gcs-service-account.json
COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json

# inject the two env vars so FirestoreClient.scala picks up /app/.gcp/… instead of /Users/…
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json

ENTRYPOINT ["/app/start-lag-append-consumer.sh"]