# eclipse-temurin replaces openjdk (the openjdk Docker Hub images were deprecated
# and the 11-jre-* tags removed). Matches the base used by the Flink images.
FROM eclipse-temurin:11-jre-jammy
WORKDIR /app

COPY "target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar" /app/holon-assembly.jar

# Strip CRLF so the entrypoint runs on Linux regardless of checkout EOL.
COPY examples/nexmark-remote-java/docker/output-consumer/start-consumer.sh /app/start-consumer.sh
RUN chmod +x /app/start-consumer.sh && sed -i 's/\r$//' /app/start-consumer.sh

# No GCP credentials: start coordination uses the HTTP start gate (START_GATE_URL),
# so holon.utils.StartGate never falls back to Firestore.

ENTRYPOINT ["/app/start-consumer.sh"]
