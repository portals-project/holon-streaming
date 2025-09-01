FROM openjdk:11-jre-slim
WORKDIR /app

COPY "target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar" /app/holon-assembly.jar

COPY .gcp/gcs-service-account.json   /app/.gcp/gcs-service-account.json
COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json

COPY examples/nexmark-remote-java/docker/nexmark-producer/start-producers.sh /app/start-producers.sh
RUN chmod +x /app/start-producers.sh

ENTRYPOINT ["/app/start-producers.sh"]
