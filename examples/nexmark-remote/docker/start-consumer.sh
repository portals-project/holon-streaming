#!/usr/bin/env bash
set -euo pipefail

echo "Compiling…"
sbt compile

echo "Launching OutputConsumer…"
sbt "runMain holon.example.nexmark.OutputConsumer"