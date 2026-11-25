# Airflow orchestration

This folder contains the Docker Compose environment and `ecommerce_data_pipeline` DAG. The DAG runs three DockerOperator tasks sequentially: `run_silver_batch.py` -> `run_gold.py` -> `validate_gold_metrics.py`. It does not start Kafka or the streaming ingestion job.

## Start

From this directory:

```powershell
docker compose up --build
```

Open `http://localhost:8080`, enable `ecommerce_data_pipeline`, and trigger it. The local Compose configuration initializes the development account `admin` / `admin`; replace these credentials before any broader exposure.

## Prerequisites

1. Start the root Kafka Compose project if you also run the producer/stream locally.
2. Build the Spark image from `../spark` as `ecommerce-spark:latest`.
3. Update `SPARK_PROJECT_PATH` in `docker-compose.yml` to the host path containing the Spark project. Docker Desktop must be able to access it.
4. Produce Bronze Parquet before triggering the DAG, since the first task reads `bronze/*.parquet`.
5. Ensure the Docker socket is mounted and accessible to the Airflow services for DockerOperator.

The DAG mounts Bronze, Silver, Gold, quarantine, and checkpoint folders from the configured Spark path. Its checkpoint folder name is `checkpoint`; Spark's Bronze stream setting is `checkpoint_bronze`. The DAG does not currently use that checkpoint, but align the paths if adding stream orchestration. The DAG runs batch jobs in a Spark container; Spark's configured `localhost:9092` Kafka broker address is not reachable as the host broker from inside a container without network-specific configuration.

## Configuration files

- `docker-compose.yml`: Postgres metadata database, Airflow init, webserver, and scheduler.
- `Dockerfile` and `requirements.txt`: Airflow image and Docker provider dependency.
- `dags/ecommerce_pipeline.py`: orchestration and host data mounts.

Local state is stored in the named Postgres volume and the mounted DAG/log/plugin directories. For operational failures, see [troubleshooting](../docs/TROUBLESHOOTING.md).
