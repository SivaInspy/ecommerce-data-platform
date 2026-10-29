# Troubleshooting Guide

## Real-Time E-Commerce Data Platform

This document records the major technical issues encountered while building and integrating the Real-Time E-Commerce Data Platform.

The purpose is to document:

- Problems encountered during development
- Root causes
- Fixes applied
- Validation performed
- Lessons learned
- Enterprise-level considerations

---

## 1. Windows Hadoop Configuration

### Problem

Running PySpark locally on Windows resulted in Hadoop-related errors such as:

```text
HADOOP_HOME and hadoop.home.dir are unset
```

and errors related to:

```text
winutils.exe
hadoop.dll
```

### Root Cause

Apache Spark uses Hadoop libraries internally. On Windows, Hadoop native support requires the appropriate Hadoop binaries and environment variables.

### Fix

Configured Hadoop locally:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:HADOOP_CONF_DIR = "C:\hadoop\etc\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
$env:JAVA_TOOL_OPTIONS = "-Dhadoop.home.dir=C:\hadoop"
```

The required Hadoop binaries were placed under:

```text
C:\hadoop\bin
```

including:

```text
winutils.exe
hadoop.dll
```

### Validation

PySpark was subsequently able to start successfully and execute Spark jobs locally.

---

## 2. Spark and Kafka Integration

### Problem

The Spark Streaming application initially failed to recognize Kafka as a data source.

The error was similar to:

```text
Failed to find data source: kafka
```

### Root Cause

The Spark Kafka connector was not available on the Spark classpath.

### Fix

Configured the Kafka connector in the Spark session:

```python
.config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
)
```

The project uses:

```text
PySpark 4.0.0
Scala 2.13
```

Therefore, the corresponding Scala 2.13 Kafka connector was used.

### Validation

The Spark application was able to:

1. Connect to Kafka
2. Subscribe to the `orders` topic
3. Read Kafka messages
4. Deserialize JSON records
5. Write records into the Bronze layer

---

## 3. Spark Streaming and Bronze Layer

### Problem

The project required Kafka messages to be persisted reliably so that downstream processing could operate independently of the Kafka producer.

### Solution

Implemented a Bronze layer using Spark Structured Streaming.

The flow is:

```text
Kafka
  |
  v
Spark Structured Streaming
  |
  v
JSON Parsing
  |
  v
Bronze Parquet
```

Bronze output is written using:

```python
.writeStream
.format("parquet")
.outputMode("append")
.option("path", BRONZE_PATH)
.option("checkpointLocation", CHECKPOINT_LOCATION)
.start()
```

### Design Consideration

Spark checkpoints are used to maintain streaming progress.

The project therefore maintains:

```text
checkpoint/
```

for Bronze streaming processing.

---

## 4. Airflow Custom Image Failure

### Problem

The first custom Airflow image attempt resulted in:

```text
/entrypoint: line 20: airflow: command not found
```

### Root Cause

The custom Dockerfile incorrectly installed/configured Airflow dependencies and resulted in an image where the expected Airflow executable/environment was no longer available correctly.

### Fix

The custom image was changed to extend the official Airflow image directly:

```dockerfile
FROM apache/airflow:2.10.5
```

The additional Docker provider was then installed without replacing the Airflow installation.

---

## 5. Airflow Provider Dependency Conflict

### Problem

An attempt was made to install:

```text
apache-airflow-providers-docker==4.4.5
```

This resulted in dependency resolution problems.

The resolver attempted to consider newer Airflow versions, including Airflow 3.x.

### Root Cause

The selected Docker provider version was not aligned with the dependency constraints of the selected Airflow version.

The project uses:

```text
Apache Airflow 2.10.5
```

The Airflow 2.10.5 constraints specify:

```text
apache-airflow-providers-docker==4.0.0
```

### Fix

The Docker provider was changed to:

```dockerfile
RUN pip install --no-cache-dir \
    "apache-airflow-providers-docker==4.0.0" \
    --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
```

### Lesson Learned

When extending an Airflow image, provider versions should be aligned with the Airflow version and its official constraint file.

---

## 6. Airflow DockerOperator `auto_remove` Issue

### Problem

The Airflow DockerOperator initially failed during task execution.

The Docker provider version being used expects `auto_remove` to use a string value.

### Original Configuration

```python
auto_remove=True
```

### Fix

Changed it to:

```python
auto_remove="success"
```

This instructs Docker to automatically remove the temporary container after successful execution.

---

## 7. Airflow Docker Socket Configuration

### Problem

Airflow needed to start the Spark Docker container through DockerOperator.

The Docker Engine was accessible from the Airflow container, but the Docker connection configuration required correction.

### Fix

The Docker socket was mounted:

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

The DockerOperator was configured with:

```python
docker_url="unix:///var/run/docker.sock"
```

### Result

Airflow was able to communicate with Docker and launch the Spark container successfully.

---

## 8. Spark Project Path Configuration

### Problem

Airflow runs inside Docker, while the Spark project and its data directories exist on the Windows host.

Airflow therefore needed the host project location to create the appropriate Docker bind mounts.

### Fix

The Airflow services were configured with:

```yaml
environment:
  SPARK_PROJECT_PATH: "C:/Users/dsiva/OneDrive/Projects/ecommerce-data-platform/spark"
```

The DAG reads the value using:

```python
SPARK_PROJECT_PATH = os.environ.get(
    "SPARK_PROJECT_PATH",
    "C:/Users/dsiva/OneDrive/Projects/ecommerce-data-platform/spark"
)
```

The DAG then creates Docker mounts for:

```text
bronze
silver
gold
quarantine
checkpoint
checkpoint_silver
```

---

## 9. Airflow to Spark Integration

### Architecture

The final orchestration flow is:

```text
Airflow
   |
   | DockerOperator
   v
Spark Docker Container
   |
   +--> Silver Processing
   |
   +--> Gold Processing
   |
   +--> Gold Validation
```

The DAG contains three primary tasks:

```text
silver_processing
        |
        v
gold_processing
        |
        v
gold_validation
```

The dependency is defined as:

```python
silver_processing >> gold_processing >> gold_validation
```

---

## 10. Silver Processing Validation

The Silver processing task was validated through Airflow.

The successful execution produced:

```text
Bronze record count: 34
Valid records: 28
Invalid records: 2
Silver record count: 7
```

The Silver output was written to:

```text
silver/data
```

Invalid records were written to:

```text
quarantine/data
```

---

## 11. Gold Processing Validation

The Gold processing task was successfully executed through Airflow.

The Gold layer generates:

```text
gold/revenue_by_category
gold/revenue_by_city
gold/overall_metrics
gold/sales_trend
```

The Gold processing job reads the Silver layer and creates analytical datasets.

---

## 12. Gold Data Validation

A dedicated validation process compares Gold overall metrics against metrics calculated directly from Silver.

The validation checks:

- Total orders
- Total revenue
- Average order value

The validation uses assertions such as:

```python
assert expected_orders == gold_orders
```

```python
assert expected_revenue == gold_revenue
```

and:

```python
assert abs(expected_aov - gold_aov) < 0.01
```

A successful validation confirms:

```text
✓ Total orders match.
✓ Total revenue matches.
✓ Average order value matches.
✓ Gold metrics match Silver source data.
✓ Validation successful.
```

---

## 13. Full Airflow DAG Validation

The complete Airflow DAG was successfully executed.

The final orchestration sequence was:

```text
Silver Processing
       |
       v
Gold Processing
       |
       v
Gold Validation
```

All three stages completed successfully.

This confirms that the pipeline can be orchestrated through Airflow using DockerOperator and the Spark Docker image.

---

## 14. Key Lessons Learned

### Dependency Management

Airflow providers must be compatible with the selected Airflow version.

Use the official Airflow constraints file when installing additional providers.

### Container Integration

When one Docker container needs to start another Docker container, Docker socket access and the correct Docker URL are important.

### Host and Container Paths

When containers process host data, bind mounts must be carefully designed so that both Airflow and Spark access the expected directories.

### Data Validation

A pipeline should validate its outputs rather than assuming that successful execution means correct data.

### Deduplication

Deduplication must be based on clearly defined business semantics rather than automatically assuming that one column is always a unique identifier.

---

## 15. Final Working Architecture

```text
                    +----------------+
                    |     Kafka      |
                    |    orders      |
                    +-------+--------+
                            |
                            v
                 +---------------------+
                 | Spark Structured    |
                 | Streaming           |
                 +----------+----------+
                            |
                            v
                    +---------------+
                    | Bronze Layer  |
                    |    Parquet    |
                    +-------+-------+
                            |
                            v
                    +---------------+
                    | Data Quality  |
                    |   Validation  |
                    +-------+-------+
                            |
              +-------------+-------------+
              |                           |
              v                           v
       +-------------+             +-------------+
       | Silver      |             | Quarantine  |
       | Valid Data  |             | Invalid Data|
       +------+------+             +-------------+
              |
              v
       +--------------+
       | Gold Layer   |
       +------+-------+
              |
       +------+-------+
       |              |
       v              v
 Revenue Analytics  Metrics
       |
       v
    Validation

Airflow orchestrates:

Silver -> Gold -> Validation
```

---

## 16. Status

The following components have been implemented and validated:

- Kafka
- Spark Structured Streaming
- Bronze layer
- Data-quality validation
- Quarantine layer
- Silver layer
- Gold layer
- Gold metric validation
- Dockerized Spark
- Dockerized Airflow
- Airflow DockerOperator
- Airflow DAG orchestration
- End-to-end DAG execution
- Exact-row deduplication strategy

The next stage of the project is to integrate the Gold datasets with the reporting/visualization layer and continue with CI/CD and project documentation.


# Troubleshooting

Use the component's working directory when running scripts; several input and output paths are relative to the current directory.

## Kafka does not start or producer cannot connect

- From the repository root, check `docker compose ps` and `docker compose logs kafka zookeeper`.
- The producer is configured for the host listener, normally `localhost:9092`. A client in a container should use the internal Compose address `kafka:29092` if it shares the network.
- Verify the broker and topic values in `producer/config/settings.py` and Spark's `spark/config/settings.py` agree.
- Wait for the broker to become healthy before starting the producer. The Compose file declares service ordering, not a Kafka readiness health check.

## Spark reports that Kafka data source is missing

The Structured Streaming source requires the matching Spark SQL Kafka connector. Check `spark/app/session/spark_session.py` and confirm the artifact's Spark and Scala versions match the installed PySpark distribution. The batch jobs do not need the Kafka connector. Connector download may require network access the first time the stream starts.

## Spark cannot find input data or writes output in an unexpected place

Run Spark entry points from `spark/` as documented in the root README. Settings and job paths are relative: Bronze reads from `bronze/`, Silver writes to `silver/data`, Gold writes to `gold/`, and exports go to `bi_export/`. Confirm those directories are present and contain the expected Parquet data. In PowerShell, use `Set-Location spark` before running jobs.

## Stream appears idle

The stream waits continuously for Kafka events. Run `python producer.py` from the repository root to send the five sample records, and check that Spark and producer use the same broker and topic. Inspect Kafka/Spark logs. The consumer does not explicitly set `startingOffsets`; Spark's Kafka source default applies when creating a query without a checkpoint. Bronze progress is tied to the checkpoint in `spark/config/settings.py`.

## Silver quarantines records or outputs fewer rows

Inspect the `error_reason` column in `quarantine/data/`. The validator reports only its first failing condition, in order: missing order ID, customer ID, price, non-positive price, then missing order time. Valid rows are exact-row deduplicated, so identical records collapse, but rows sharing an order ID with different attributes remain. Counts vary with input files. The batch overwrites Silver; quarantine is only overwritten when the current run has invalid rows, so remove or archive stale quarantine output deliberately if you need a clean-run snapshot.

## Gold validation fails

Run the stages in sequence from `spark/`: `python run_silver_batch.py`, `python run_gold.py`, then `python validate_gold_metrics.py`. Confirm Silver and Gold were produced from the same source snapshot. Validation compares order count and revenue exactly and average order value within 0.01. A validation failure exits with an assertion error; inspect its expected and actual values.

## Airflow task fails to launch Spark container

- Build the image expected by the DAG: from `spark/`, run `docker build -t ecommerce-spark:latest .`.
- Check that Docker Desktop is running and the Airflow services can access `/var/run/docker.sock`.
- Set `SPARK_PROJECT_PATH` in `airflow/docker-compose.yml` to the actual host path to `spark/`; bind mount paths are interpreted by the Docker daemon.
- Ensure the host folders used by the mounts exist and contain Bronze data before triggering Silver.
- Review Airflow task logs and `docker compose logs` from `airflow/`.

## Airflow cannot connect to Spark job dependencies

The Airflow DAG currently runs batch jobs only. It mounts storage directories, but its checkpoint mount is named `checkpoint`, while Spark settings name the Bronze streaming checkpoint `checkpoint_bronze`; align this before mounting/running the stream. The Spark config's Kafka broker is `localhost:9092`, which refers to the Spark container itself when run in Docker. Use a reachable broker address if a container job needs Kafka.

## Airflow Compose initialization or dependency issues

Use the Airflow Docker image and provider pins in `airflow/Dockerfile` and `airflow/requirements.txt`. If you change the Airflow version, use its matching official constraints and a compatible Docker provider. Check Postgres health and the `airflow-init` service logs before restarting webserver or scheduler.

## Resetting local data or checkpoints

Bronze files, Silver/Gold outputs, and Spark checkpoints are local state. Do not delete checkpoints while a stream is running. Deleting a checkpoint starts a new query history and can cause replay or gaps depending on Kafka offsets. Back up any data worth keeping before cleanup. For Compose-managed Kafka state, inspect volumes before removing them; `docker compose down -v` removes persisted broker state.
