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
## Set the environment variable for the service account JSON file
#ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
#ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json
#
## Copy the service account JSON file
#COPY .gcp/gcs-service-account.json /app/.gcp/gcs-service-account.json
#COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json
#
#
## Copy the source code
#COPY src /app/src
#
## Install dependencies and build the application
#RUN sbt compile
#
## Run the HolonNode application
#CMD ["sbt", "runMain holon.example.nexmark.NexmarkProducerJson"]

FROM hseeberger/scala-sbt:11.0.12_1.5.5_2.13.6  AS builder

# Set the working directory
WORKDIR /app

COPY build.sbt /app/
COPY project /app/project

# Download dependencies
RUN sbt update

# Copy the source code
COPY src /app/src
RUN sbt assembly


# Stage 2: Minimal runtime image
FROM openjdk:11-jre-slim

WORKDIR /app

COPY --from=builder /app/target/scala-3.3.4/*.jar /app/app.jar
COPY .gcp /app/.gcp

ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json

CMD ["java", "-cp", "/app/app.jar", "holon.example.nexmark.NexmarkProducerJson"]