# Use an official Scala SBT image as a parent image
FROM hseeberger/scala-sbt:11.0.12_1.5.5_2.13.6

# Set the working directory
WORKDIR /app

# Copy the build.sbt and project files
COPY build.sbt /app/
COPY project /app/project

# Copy the source code
COPY src /app/src

# Install dependencies and build the application
RUN sbt compile

# Run the HolonNode application
CMD ["sbt", "runMain holon.example.nexmark.OutputConsumer"]