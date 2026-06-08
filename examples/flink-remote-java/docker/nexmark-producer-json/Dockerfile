# # version 1: Build the application without compiled code
# # Use an official Scala SBT image as a parent image
# FROM hseeberger/scala-sbt:11.0.12_1.5.5_2.13.6

# # Set the working directory
# WORKDIR /app

# # Copy the build.sbt and project files
# COPY build.sbt /app/
# COPY project /app/project

# # Download dependencies
# RUN sbt update

# # Copy the source code
# COPY src /app/src

# # Install dependencies and build the application
# RUN sbt compile

# # Run the HolonNode application
# CMD ["sbt", "runMain holon.example.nexmark.NexmarkProducerPerPartition"]

# version 2: Build the application with compiled code
FROM eclipse-temurin:11-jre-jammy
WORKDIR /app

COPY "target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar" /app/holon-assembly.jar

COPY .gcp/gcs-service-account.json   /app/.gcp/gcs-service-account.json
COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json

COPY examples/flink-remote-java/docker/nexmark-producer-json/start-producers.sh /app/start-producers.sh
RUN chmod +x /app/start-producers.sh && sed -i 's/\r$//' /app/start-producers.sh

ENTRYPOINT ["/app/start-producers.sh"]