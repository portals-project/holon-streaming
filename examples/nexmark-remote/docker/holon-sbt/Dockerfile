# Shared base image for the SBT "on-the-fly" Holon flow (rubyies/holon-sbt:latest).
#
# Unlike nexmark-remote-java (which bakes an assembled fat JAR), this image bakes
# only the *dependency cache* — never the source. At runtime the source tree is
# bind-mounted from the synced EC2 directory and compiled in place by the
# `holon-build` one-shot service (see docker-compose.holon-sbt.yml). That lets us
# edit Scala locally and re-run on AWS with just an `sbt compile`, no reassembly
# and no image rebuild/push per change.
#
# Build + push once (and only when build.sbt dependencies change) with
# scripts/build/build-holon-sbt-image.sh.
#
# JDK 17: the sbtscala official images no longer publish JDK 11 tags. The project
# pins `scalaVersion := 3.3.4` in build.sbt, so sbt downloads that compiler
# regardless of the launcher Scala bundled in the image; JDK 17 runs Scala-3.3.4
# bytecode without issue.
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.19_10_1.12.12_3.3.8

WORKDIR /app

# Bake the dependency cache: copy only the build definition (NOT src) and resolve.
# This populates ~/.cache/coursier, ~/.ivy2 and ~/.sbt, which the named cache
# volumes seed from on first run so the remote's first compile doesn't re-download.
COPY build.sbt /app/build.sbt
COPY project /app/project
RUN sbt --batch update

# Create the log/snapshot mount points so the bind mounts land on existing dirs.
RUN mkdir -p /app/logs /app/snapshots /app/target

# No ENTRYPOINT: every service overrides `command:` in the compose file.
