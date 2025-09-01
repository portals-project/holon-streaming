FROM openjdk:11-jre-slim
WORKDIR /app

COPY "target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar" /app/holon-assembly.jar
COPY examples/nexmark-remote-java/docker/output-consumer/start-consumer.sh /app/start-consumer.sh
RUN chmod +x /app/start-consumer.sh

# Copy the service account JSON file
COPY .gcp/gcs-service-account.json /app/.gcp/gcs-service-account.json
COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json

# inject the two env vars so FirestoreClient.scala picks up /app/.gcp/… instead of /Users/…
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json

ENTRYPOINT ["/app/start-consumer.sh"]
