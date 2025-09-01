


#FROM hseeberger/scala-sbt:11.0.12_1.5.5_2.13.6  AS builder
#
## Set the working directory
#WORKDIR /app
#
#COPY build.sbt /app/
#COPY project /app/project
#
#COPY .gcp/gcs-service-account.json   /app/.gcp/gcs-service-account.json
#COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json
#ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
#ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json
#
## Download dependencies
#RUN sbt update
#
## Copy the source code
#COPY src /app/src
#RUN sbt assembly
#
#
## ─── Stage 2: Minimal runtime image ─────────────────────────
#FROM openjdk:11-jre-slim
#WORKDIR /app
#
## 1) Bring in your fat-jar
#COPY --from=builder /app/target/scala-3.3.4/*.jar /app/app.jar
#
## 2) ALSO copy your credentials folder
#COPY --from=builder /app/.gcp          /app/.gcp
#
## default command
##CMD ["java", "-cp", "/app/app.jar", "holon.example.nexmark.NexmarkProducerPerPartition", "0"]
#CMD ["java", "-cp", "/app/app.jar", "holon.example.nexmark.NexmarkProducerJson"]



FROM openjdk:11-jre-slim
WORKDIR /app

COPY "target/scala-3.3.4/execution-service-assembly-0.1.0-SNAPSHOT.jar" /app/holon-assembly.jar

COPY .gcp/gcs-service-account.json   /app/.gcp/gcs-service-account.json
COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json

COPY examples/flink-remote-java/docker/nexmark-producer-json/start-producers.sh /app/start-producers.sh
RUN chmod +x /app/start-producers.sh

ENTRYPOINT ["/app/start-producers.sh"]
