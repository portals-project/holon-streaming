# Using KTH Remote System

# Login
Make sure you're in an terminal that supports unix commands, such as WSL or Git Bash on Windows, or a terminal on Linux or macOS.
```bash
ssh jonas_spenger@croaker.eecs.kth.se
# password: TZfN26n#VW2tDpXv0EoT
```

# Create key to skip login
```bash
# On your laptop, if you don’t already have an SSH key:
ssh-keygen -t ed25519   # (or rsa) 
# Press Enter for all the defaults; this creates ~/.ssh/id_ed25519 (+ id_ed25519.pub)

# Copy your public key to the remote’s authorized_keys:
ssh-copy-id jonas_spenger@croaker.eecs.kth.se
# Enter your remote password one last time; afterward, you should be able to `ssh youruser@remote.host.address` without a password prompt.
```

```bash
docker context create remote-docker \
  --description "SSH to remote host for all docker commands" \
  --docker "host=ssh://jonas_spenger@croaker.eecs.kth.se"
```


```bash
# List all containers (including stopped) whose name starts with “nexmark-local”,
# then pass their IDs to docker rm:
docker rm $(docker ps -a --filter "name=nexmark-local" -q)
```



# Navigate to the nexmark directory
```bash
Basic Linux Navigation & Commands

pwd – print working directory.

ls / ls -l – list files/directories (long listing).

cd <path> – change directory.

mkdir <dir> – make a new directory.

cp / mv / rm – copy/move/delete files.

sudo <command> – run <command> as root (you’ll be prompted for your password).

nano or vim – edit files in the terminal.

htop or top – monitor CPU/memory in real time.
```


