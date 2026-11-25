# Docker resources

The root `docker-compose.yml` runs a single Kafka broker with ZooKeeper for local producer and Spark development. Start it from the repository root with `docker compose up -d`; stop it with `docker compose down`.

The Spark image definition is in [`spark/Dockerfile`](../spark/Dockerfile), and the separate Airflow stack is in [`airflow/docker-compose.yml`](../airflow/docker-compose.yml). See the root README for run instructions and [troubleshooting](../docs/TROUBLESHOOTING.md) for container networking, mounts, and state cleanup.
