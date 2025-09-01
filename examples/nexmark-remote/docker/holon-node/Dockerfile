# Use an official Scala SBT image as a parent image
FROM hseeberger/scala-sbt:11.0.12_1.5.5_2.13.6

WORKDIR /app

# Copy the build.sbt and project files
COPY build.sbt /app/
COPY project /app/project

# Download dependencies
RUN sbt update

# Copy the source code
COPY src /app/src

# Copy the service account JSON file
COPY .gcp/gcs-service-account.json /app/.gcp/gcs-service-account.json
COPY .gcp/firebase-user-account.json /app/.gcp/firebase-user-account.json

# 2) Copy the startup script from examples/nexmark-remote/docker/
COPY examples/nexmark-remote/docker/start-nodes.sh /app/start-nodes.sh
RUN chmod +x /app/start-nodes.sh

# inject the two env vars so FirestoreClient.scala picks up /app/.gcp/… instead of /Users/…
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/.gcp/gcs-service-account.json
ENV GOOGLE_FIRESTORE_CREDENTIALS=/app/.gcp/firebase-user-account.json

# 4) Create log/snapshot dirs
RUN mkdir -p /app/logs /app/snapshots

ENTRYPOINT ["/app/start-nodes.sh"]
