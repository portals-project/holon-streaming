# Remote Docker Deployment README

## Most used commands
```bash
ssh jonas_spenger@croaker.eecs.kth.se
    
# Ensure you are in the nexmark-remote directory (local)
docker compose up -d --build
docker compose down

# While on the remote host:
docker ps
# check logs for a specific container
docker logs <container_name> 

# Ensure you are in the flink-remote directory (local)
bash deploy.sh
# if you want to submit a new query to the flink cluster (change dparallelism, query)
docker exec -i flink-jobmanager bash -c "bin/sql-client.sh -Dparallelism.default=2 -f /opt/flink/queries/q7.sql"

# To clean up the logs/snapshots on the remote host
rm -r /home/jonas_spenger/projects/holon/holon-experiments/logs/*
rm -r /home/jonas_spenger/projects/holon/holon-experiments/snapshots/*

# Send files to the remote host
scp /path/to/local/file.txt jonas_spenger@croaker.eecs.kth.se:~/projects/
# Send files to local machine
scp jonas_spenger@croaker.eecs.kth.se:~/projects/ /path/to/local/docker-compose.yml

# Command to send output logs to your local machine
scp jonas_spenger@croaker.eecs.kth.se:/home/jonas_spenger/projects/holon/holon-experiments/logs/output.log C:/Users/rvang/Documents/GitHub/holon-benchmark-analysis/local-logs/output.log
scp jonas_spenger@croaker.eecs.kth.se:/home/jonas_spenger/projects/holon/flink-experiments/flink-logs/output.log C:/Users/rvang/Documents/GitHub/holon-benchmark-analysis/local-logs/output.log
# Command to update queries on the remote host
scp C:/Users/rvang/Documents/GitHub/holon-streaming-clone/examples/flink-remote/queries/q0.sql jonas_spenger@croaker.eecs.kth.se:/home/jonas_spenger/projects/holon/flink-experiments/queries/q0.sql


# Give execute permissions to a file
chmod +x <file-name>
```

## SSH Access & Key Setup (first time only)

### 1. Establish SSH Connectivity
```bash
ssh jonas_spenger@croaker.eecs.kth.se
```

3. If this is your first time, accept the host key fingerprint. You will be prompted for a password.

### 2. Generate SSH Key
** to deploy containers from your local machine without password**

If you do not already have an SSH key pair on your local machine:

```bash
# On Linux/macOS or WSL/Bash:
ssh-keygen -t ed25519 # (or rsa, if preferred)
```

### 3. Copy Public Key to Remote Host

1. On your local machine, run:
   ```bash
   ssh-copy-id jonas_spenger@croaker.eecs.kth.se
   ```
2. Enter your remote password when prompted.
3. After this, you should be able to SSH into the remote host without a password prompt:
   ```bash
   ssh jonas_spenger@croaker.eecs.kth.se
   ```
---

## Docker Context Configuration (first time only)

To run Docker commands on the remote host from your local machine, create a Docker context:

```bash
docker context create remote-docker \
  --description "SSH to remote host for all docker commands" \
  --docker "host=ssh://jonas_spenger@croaker.eecs.kth.se"
```
* You can list available contexts with:
  ```bash
  docker context ls
  ```
* To switch to the remote context:
  ```bash
  docker context use remote-docker
  ```
* To switch back to the default local context:
  ```bash
  docker context use default
  ```
> **Note:** Once you switch to `remote-docker`, any `docker …` command you run locally will execute on the remote host.

---

## Remote Host Folder Structure

Below is an example structure under your home directory on the remote host.

```
~home/jonas_spenger/projects/
                        └── holon/
                            ├── flink-experiments/
                            │   ├── flink-checkpoints/
                            │   ├── flink-connectors/
                            │   ├── flink-logs/
                            │   │   └── output.log
                            │   ├── queries/
                            │   │   └── q7.sql
                            ├── holon-experiments/
                            │   ├── logs/
                            │   │   └── output.log
                            │   ├── snapshots/
```

---

## Running Holon System on remote host
You can simply use the `docker compose` commands with the correct context to manage your deployment as if you were running it locally.

```bash
# Ensure you are in the nexmark-remote directory (local)
docker compose up -d --build

docker compose down
```

To check on the status of your containers, you can run:

```bash
# ssh into the remote host
docker ps

# check logs for a specific container
docker logs <container_name> 
```
* All containers mount snapshots and logs to the remote host, depending on your code version logs are stored in a output.log file, is set using the ``USE_LOG_FILE`` environment variable in the config file.

## Running Flink System on remote host
To run a Flink instance a `deploy.sh` script is set up to start all the necessary services, 
and submit the queries to the Flink cluster. Adjust the number of task managers and job managers 
in the `docker-compose.yml` file as needed, and change the query in the `deploy.sh` file. 
Since Flink is running on the remote host, all supporting (connectors, queries, ...) files must be pushed to the remote host 
using `scp` or `rsync` to copy files from your local machine to the remote host. Remember to change 
the start flag in your google cloud storage to start the nexmark producer :).

```bash
# Ensure you are in the flink-remote directory (local)
bash deploy.sh
```

### Things to consider when debugging
- Check if the remote host has the correct folders and permissions set up.
- Ensure the kafka service is running and accessible before starting the Flink job.