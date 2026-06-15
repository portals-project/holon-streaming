#!/usr/bin/env bash
set -euo pipefail

# One-shot compile for the SBT "on-the-fly" Holon flow.
#
# Compiles the bind-mounted source ONCE into the shared `holon-target` volume and
# writes the runtime classpath to /app/target/classpath.txt. The run services
# (holon-nodes / nexmark-producers / output-consumer) depend on this completing
# and then launch cheap `java -cp "$(cat classpath.txt)"` processes — the same
# java fan-out the assembly flow uses, but against freshly compiled classes
# instead of a baked fat JAR. Compiling here (not in each service) avoids N cold
# sbt JVMs.

cd /app

echo "==> Compiling source (sbt compile)…"
sbt --batch -Dsbt.log.noformat=true compile

echo "==> Exporting runtime classpath -> target/classpath.txt"
# `export <task>` prints the machine-readable task result; with --error the
# classpath is the only thing on stdout. Take the last non-empty line to be safe.
mkdir -p /app/target
sbt --batch --error -Dsbt.log.noformat=true 'export Runtime/fullClasspath' \
  | grep -v '^$' \
  | tail -n1 > /app/target/classpath.txt

if [[ ! -s /app/target/classpath.txt ]]; then
  echo "!! classpath export produced no output" >&2
  exit 1
fi

echo "==> classpath.txt written ($(wc -c < /app/target/classpath.txt) bytes)"
echo "==> Build complete."
