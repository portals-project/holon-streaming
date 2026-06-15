# nexmark-remote — Holon SBT "on-the-fly" deployment

This is the **source-compiled** Holon deployment, wired into the unified experiment
runner (`run-experiment.sh`). Unlike `nexmark-remote-java/` (which ships prebuilt
assembly images and takes ~12–20 min to reassemble after any code change), this
variant **syncs the Scala source to the experiment box and compiles it there**, so
you can edit a reducer or core code locally and re-run on AWS with only an
`sbt compile` — no reassembly, no per-change image push.

## How it works

```
local edit ──rsync src/──▶ EC2:$REMOTE_BASE ──compose up──▶ holon-build  (sbt compile → target/classpath.txt)
                                                             └▶ holon-nodes / nexmark-producers / output-consumer
                                                                  java -cp "$(cat classpath.txt)" <MainClass> $i
```

- All Holon services share one base image, `rubyies/holon-sbt:latest`, which bakes
  only the **dependency cache** (`sbt update` over `build.sbt` + `project/`) — never
  the source.
- The source tree (`src/`, `build.sbt`, `project/`) is bind-mounted from the synced
  directory. The one-shot **`holon-build`** service compiles it **once** into a shared
  `holon-target` volume and writes `target/classpath.txt`.
- `holon-nodes`, `nexmark-producers`, `output-consumer` wait for `holon-build` to
  finish, then java-launch from that classpath (cheap fan-out, like the assembly flow).
- Start coordination uses the **HTTP start gate** (`START_GATE_URL`); no GCP/Firestore.

Main classes (current source): `holon.examples.nexmark.nodes.HolonNode`,
`holon.examples.nexmark.data.NexmarkProducerPerPartition`,
`holon.examples.nexmark.consumers.OutputConsumer`.

## Running an experiment

Everything goes through the runner — you do not invoke `docker compose` directly.

```bash
# One-time (and only when build.sbt dependencies change): build + push the base image.
bash scripts/build/build-holon-sbt-image.sh

# Run, selecting the SBT profile via a template (DEPLOYMENT=nexmark-remote).
./run-experiment.sh --template holon-q7-sbt
#   or interactively: ./run-experiment.sh  -> platform: holon -> template: Holon Q7 (SBT on-the-fly)

# Verify routing without touching AWS:
./run-experiment.sh --template holon-q7-sbt --dry-run
```

The runner (`experiment/03-template.sh`) routes `DEPLOYMENT=nexmark-remote` to this
deployment's compose/sync/pull/deploy scripts; `05-ec2-setup.sh` wipes `$REMOTE_BASE`,
syncs source via `scripts/sync/sync-holon-sbt-remote.sh`, pre-pulls images
(`holon-sbt-pull.sh`); `06-run.sh` writes the per-run `.env`, runs `deploy.sh`, waits
for the start gate, then monitors and collects results into `holon-logs/`.

## Files

| File | Purpose |
|------|---------|
| `docker-compose.holon-sbt.yml` | Runner-parameterized stack (kafka, gate, holon-build, nodes/producers/consumer) |
| `docker/holon-sbt/Dockerfile` | `rubyies/holon-sbt:latest` base — deps baked, no source |
| `docker/holon-build/build.sh` | Compile once; write `target/classpath.txt` |
| `docker/holon-node/start-nodes.sh` | Launch `NUM_NODES` HolonNode instances |
| `docker/nexmark-producer/start-producers.sh` | Launch `PRODUCER_COUNT` producers |
| `docker/output-consumer/start-consumer.sh` | Launch the OutputConsumer |
| `docker/init-kafka.sh` | Create topics (runs in the `apache/kafka` image) |
| `deploy.sh` | Bring the stack up (blocks through the one-time compile) |
| `holon-sbt-pull.sh` | Pre-pull base + gate + kafka images |

## Editing code and re-running

1. Edit any file under `src/`.
2. Re-run the same template. The runner re-syncs `src/` and `holon-build` recompiles
   on the box. **No base-image rebuild** unless you changed `build.sbt` dependencies.
3. If deps changed, re-run `scripts/build/build-holon-sbt-image.sh` first.

## Notes

- The one-time `sbt compile` on the box (~minutes) gates start-up; `deploy.sh` uses a
  generous `TIMEOUT`. Set `GATE_WAIT_TIMEOUT` higher if producers are slow to announce.
- Dependency caches (`~/.cache`, `~/.ivy2`, `~/.sbt`) are named volumes seeded from the
  baked image, so the first compile compiles source only — it does not re-download deps.
- JDK 17: the `sbtscala` official images no longer publish JDK 11 tags; `build.sbt` pins
  `scalaVersion := 3.3.4`, so the compiler is fixed regardless of the image's launcher.
