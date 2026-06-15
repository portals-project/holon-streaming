#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

# Build & push the shared base image for the SBT "on-the-fly" Holon flow.
#
# This image bakes only the dependency cache (sbt update over build.sbt/project) —
# NOT the source. Run it ONCE, and again only when build.sbt dependencies change.
# Day-to-day Scala edits never touch this image: the runner syncs source to the
# box and the holon-build service compiles it at run time.

echo "Building rubyies/holon-sbt:latest (deps baked, no source)…"
docker build \
  -f examples/nexmark-remote/docker/holon-sbt/Dockerfile \
  -t rubyies/holon-sbt:latest \
  .

echo "Logging into Docker Hub"
docker login

echo "Pushing rubyies/holon-sbt:latest"
docker push rubyies/holon-sbt:latest

echo "Done! (producer-start-gate / kafka images are unchanged; see build-holon-images.sh for the gate.)"
