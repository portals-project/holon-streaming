## Command to run docker locally on your machine
```bash
docker compose -f docker-compose.yml up --build
docker compose -f docker-compose.yml down
docker compose -f docker-compose.yml build

# PowerShell command
docker compose down --rmi all; docker compose -f docker-compose.yml up --build

# Unix command
docker compose down --rmi all && docker compose -f docker-compose.yml up --build

```