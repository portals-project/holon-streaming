# Setup

This document covers everything that has to be in place **before** running an
experiment. Running an experiment itself is a single command — see
[Running an experiment](#running-an-experiment) at the bottom.

```
holon-streaming-clone/
├── run-experiment.sh         <-- main entry point
├── experiment/               <-- per-run pipeline (sourced by run-experiment.sh)
└── scripts/
    ├── setup/                <-- one-time machine setup
    ├── build/                <-- one-time Docker image builds
    └── sync/                 <-- manual file syncing (rarely needed directly)
```

## 1. EC2 instance

The experiments run on a single EC2 instance. Pick the type based on how
demanding your workload is.

### Recommended types

| Use case                    | Instance         | vCPU / RAM / local NVMe | ~$/hr |
|-----------------------------|------------------|-------------------------|-------|
| Baseline / smoke tests      | `r6id.4xlarge`   | 16 / 128 GB / 950 GB    | $1.21 |
| **Real benchmarking**       | **`r6id.8xlarge`** | **32 / 256 GB / 1.9 TB** | **$2.42** |
| Pushing the limits          | `r6id.16xlarge`  | 64 / 512 GB / 3.8 TB    | $4.84 |

At 5 minutes per run, the cost difference between sizes is pennies. **The
`d` suffix matters** — it gives you a local NVMe SSD that is ~10× faster
than gp3 EBS for the constant checkpoint and state-backend writes a
streaming workload does. If you don't use NVMe, you'll see jittery
checkpoint times and your numbers will be noisy.

Pick the same instance type for both the Flink and HOLON sides of any
comparison so neither side is infra-bottlenecked relative to the other.

### NVMe disk mount

Local NVMe storage is **ephemeral** — it's wiped every time the instance
stops, so it has to be re-formatted and mounted on every boot.

**`run-experiment.sh` does this for you automatically.** Phase 3 of the
pipeline detects any local NVMe device on the instance and mounts it at
`/mnt/nvme` over SSH before syncing files. On a non-`d` instance (no local
NVMe) it's a no-op.

To actually use the NVMe for the experiment's working dir, point
`REMOTE_BASE=/mnt/nvme/holon/flink-experiments` in your `.holon.env` (see
step 3). The script prints a warning if NVMe is available but you've left
`REMOTE_BASE` on the root EBS.

**Optional: use cloud-init instead.** If you'd rather mount the disk
before SSH even comes up (skipping the few seconds the script spends on
it), set this as the instance's user-data (Stop instance → Actions →
Instance settings → Edit user data):

```yaml
#cloud-config
bootcmd:
  - |
    DEV=$(lsblk -dno NAME,TYPE | awk '$2=="disk" && $1 ~ /^nvme[1-9]/{print "/dev/"$1; exit}')
    if [ -n "$DEV" ] && ! mountpoint -q /mnt/nvme; then
      mkfs.xfs -f "$DEV"
      mkdir -p /mnt/nvme
      mount "$DEV" /mnt/nvme
      chown ec2-user:ec2-user /mnt/nvme
    fi
```

Either approach works; pick one. The script is idempotent, so having both
configured is also fine.

### Skipping NVMe (not recommended)

If you don't care about benchmark fidelity, you can use any `r6i.*`
(without `d`) and leave `REMOTE_BASE` on the root EBS. Expect significantly
slower checkpoints once state grows past a few hundred MB.

## 2. Prerequisites

Install and configure once per machine:

- **AWS CLI** — configured with credentials that can start/stop the target EC2
  instance (`aws configure`).
- **Docker** — and a Docker Hub account with `docker login` completed (the
  build scripts push images).
- **SSH key** — the `.pem` file for the EC2 instance, readable by your user.
- **Bash** — Git Bash on Windows works.

## 3. One-time setup

Do this once per checkout (or whenever the target instance / key changes):

1. Copy the env template and fill in your values:

   ```bash
   cp .holon.env.example .holon.env
   ```

   Required fields:

   | Variable            | What it is                                              |
   |---------------------|---------------------------------------------------------|
   | `INSTANCE_ID`       | EC2 instance ID (e.g. `i-0abc123def456`) (ask Ruben for shared EC2 ID)                |
   | `AWS_REGION`        | Region the instance lives in                            |
   | `KEY`               | Path to the SSH private key (Git-Bash style on Windows) |
   | `REMOTE_USER`       | SSH login user (typically `ec2-user`)                   |
   | `REMOTE_BASE`       | Remote working directory (wiped each run). Point at `/mnt/nvme/holon/flink-experiments` if you mounted NVMe in step 1 |
   | `LOCAL_RESULTS_DIR` | Where results are written locally                       |

2. Validate the AWS side is wired up:

   ```bash
   bash scripts/setup/check-aws.sh
   ```

   This verifies AWS CLI auth, that the instance ID resolves, that the SSH key
   is reachable, and that your IAM permissions cover start/stop.

## 4. One-time Docker image builds

The experiment pipeline pulls Docker images from Docker Hub at runtime, so they
need to be built and pushed once (and rebuilt only when their Dockerfiles
change). Pick the build matching your experiment path:

- **Flink experiments**:

  ```bash
  bash scripts/build/build-flink-images.sh
  ```

- **HOLON experiments**:

  ```bash
  bash scripts/build/build-holon-images.sh
  ```

Both push to Docker Hub under your logged-in account.

## Running an experiment

Once setup is done:

```bash
bash run-experiment.sh
```

Useful flags:

- `--list-templates` — show available experiment templates
- `--template <name>` — skip the interactive picker
- `--dry-run` — go through every phase without starting EC2 or transferring files
- `--keep-running` — leave the EC2 instance running after the experiment
- `--help` — full usage

## Manual file sync (advanced)

`run-experiment.sh` syncs files automatically. The standalone sync script is
only needed for ad-hoc transfers between experiment runs:

```bash
KEY=... HOST=ec2-user@... REMOTE_BASE=~/holon/flink-experiments \
  bash scripts/sync/sync-flink-remote.sh core queries
```

Run with `--help` for tags and options.
