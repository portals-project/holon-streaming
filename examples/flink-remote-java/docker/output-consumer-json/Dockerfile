## Use an official Scala SBT image as a parent image
#FROM hseeberger/scala-sbt:11.0.12_1.5.5_2.13.6
#
## Set the working directory
#WORKDIR /app
#
## Copy the build.sbt and project files
#COPY build.sbt /app/
#COPY project /app/project
#
## Download dependencies
#RUN sbt update
#
## Copy the source code
#COPY src /app/src
#
## Install dependencies and build the application
#RUN sbt compile
#
## Run the HolonNode application
#CMD ["sbt", "runMain holon.example.nexmark.OutputConsumerJson"]

FROM openjdk:11-jre-slim
WORKDIR /app

COPY "target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar" /app/holon-assembly.jar
COPY examples/flink-remote-java/docker/output-consumer-json/json-consumer.sh /app/json-consumer.sh
RUN chmod +x /app/json-consumer.sh

# Copy the service account JSON file
COPY .gcp/gcs-service-account.json /app/.gcp/gcs-service-account.json
COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json

# inject the two env vars so FirestoreClient.scala picks up /app/.gcp/… instead of /Users/…
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json

ENTRYPOINT ["/app/json-consumer.sh"]