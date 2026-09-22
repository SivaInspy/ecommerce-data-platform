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
