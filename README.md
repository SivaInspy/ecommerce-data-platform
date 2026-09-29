# Real-Time E-Commerce Data Platform

A production-style data engineering project demonstrating an end-to-end e-commerce data platform using Apache Kafka, Apache Spark Structured Streaming, Docker, Apache Airflow, Parquet, and Power BI.

The platform ingests order events through Kafka, processes them using Spark, applies data-quality validation and deduplication, produces analytical Gold datasets, and exposes the results through a Power BI dashboard.

---

## Architecture

```text
                    REAL-TIME E-COMMERCE DATA PLATFORM

┌──────────────────────┐
│   E-Commerce Events  │
│                      │
│   Order JSON Events  │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Python Producer    │
│                      │
│   kafka-python       │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────┐
│        Apache Kafka          │
│                              │
│        orders topic          │
│                              │
│  Events • Partitions •       │
│  Offsets                     │
└──────────┬───────────────────┘
           │
           │ Structured Streaming
           ▼
┌──────────────────────────────┐
│   Spark Structured Streaming │
│                              │
│   Consume → Parse → Write    │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────┐
│        BRONZE LAYER          │
│                              │
│   Source-oriented Parquet    │
│                              │
│   Kafka → Parquet            │
└──────────┬───────────────────┘
           │
           │ Spark Batch Processing
           ▼
┌──────────────────────────────┐
│         SILVER LAYER         │
│                              │
│  • Schema enforcement        │
│  • Type conversion           │
│  • Data validation           │
│  • Deduplication             │
│  • Data-quality checks       │
└──────────┬───────────────────┘
           │
      ┌────┴──────────────┐
      │                   │
      ▼                   ▼
┌───────────────┐   ┌────────────────┐
│ Silver        │   │  Quarantine    │
│ Parquet       │   │                │
│               │   │ Invalid data   │
│ Valid records │   │ + error reason │
└───────┬───────┘   └────────────────┘
        │
        ▼
┌──────────────────────────────┐
│          GOLD LAYER          │
│                              │
│  • Revenue by Category       │
│  • Revenue by City           │
│  • Overall Metrics           │
│  • Average Order Value       │
│  • Sales Trend               │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────┐
│       BI EXPORT LAYER        │
│                              │
│       Gold → CSV             │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────┐
│          POWER BI            │
│                              │
│  • KPI Cards                 │
│  • Revenue by Category       │
│  • Revenue by City           │
│  • Sales Trend               │
└──────────────────────────────┘


┌─────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION / PLATFORM                 │
│                                                             │
│ Docker • Airflow • Git • Configuration • Logging            │
│ Spark Checkpointing • Validation • Automated Processing     │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze Parquet
  ↓
Silver Validation + Transformation + Deduplication
  ↓
Gold Business Aggregations
  ↓
CSV BI Export
  ↓
Power BI
```

Apache Airflow orchestrates the Silver → Gold → Validation processing flow.

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Event producer and application code |
| Apache Kafka | Event streaming and decoupled ingestion |
| Apache Spark | Distributed data processing |
| Spark Structured Streaming | Kafka event consumption |
| PySpark | Data transformation and analytics |
| Parquet | Columnar data storage |
| Docker | Containerization |
| Apache Airflow | Workflow orchestration |
| Power BI | Business intelligence and visualization |
| Git | Version control |
| GitHub | Source code and portfolio |

---

# Pipeline Components

## 1. Kafka Producer

A Python producer generates e-commerce order events and publishes them to the Kafka `orders` topic.

Example event:

```json
{
  "order_id": 1,
  "customer_id": 101,
  "customer_name": "Arun",
  "city": "Chennai",
  "product_id": "P001",
  "product_name": "iPhone 16",
  "category": "Mobile",
  "price": 79999,
  "payment_method": "UPI",
  "order_time": "2026-09-05 22:55:00"
}
```

---

## 2. Bronze Layer

Spark Structured Streaming consumes events from Kafka and writes the parsed records to Parquet.

Responsibilities:

- Kafka consumption
- JSON parsing
- Schema application
- Streaming checkpointing
- Append-only Bronze storage

Location:

```text
spark/bronze/
```

---

## 3. Silver Layer

The Silver processing layer converts Bronze data into clean, validated, business-ready records.

Processing includes:

- Schema enforcement
- Timestamp conversion
- Required-field validation
- Price validation
- Duplicate removal
- Data-quality classification

Valid records are written to:

```text
spark/silver/
```

Invalid records are written to:

```text
spark/quarantine/
```

---

## 4. Data Quality

Invalid records are not silently discarded.

The pipeline identifies conditions such as:

```text
MISSING_ORDER_ID
MISSING_CUSTOMER_ID
MISSING_PRICE
INVALID_PRICE
MISSING_ORDER_TIME
```

Invalid records are isolated in the quarantine layer for further investigation.

---

## 5. Deduplication Strategy

Deduplication was explicitly tested against the sample data.

A key finding was that deduplicating only by `order_id` was unsafe because the same order ID can appear with different business attributes.

For example, order `101` contained two distinct records:

```text
order_id = 101
customer_id = 201
price = 50000
```

and:

```text
order_id = 101
customer_id = 202
price = 60000
```

Therefore, the Silver transformation uses exact-record deduplication rather than blindly removing records based only on `order_id`.

Detailed troubleshooting and deduplication decisions are documented in:

- `docs/DATA_DEDUPLICATION.md`
- `docs/TROUBLESHOOTING.md`

---

# 6. Gold Layer

The Gold layer produces business-level analytical datasets.

### Revenue by Category

```text
category
total_revenue
total_orders
```

### Revenue by City

```text
city
total_revenue
total_orders
```

### Overall Metrics

```text
total_orders
total_revenue
average_order_value
```

### Sales Trend

```text
order_date
total_orders
total_revenue
```

Gold datasets are stored as Parquet:

```text
spark/gold/
├── overall_metrics/
├── revenue_by_category/
├── revenue_by_city/
└── sales_trend/
```

---

# 7. Gold Validation

The pipeline validates Gold metrics against the Silver source data.

The validation checks:

- Total order count
- Total revenue
- Average order value

The validation process fails if the Gold results do not match the expected Silver-derived metrics.

This provides a validation contract between the Silver and Gold layers.

Expected validation output:

```text
✓ Total orders match.
✓ Total revenue matches.
✓ Average order value matches.

✓ Gold metrics match Silver source data.
✓ Validation successful.
```

---

# 8. BI Export Layer

The Gold Parquet datasets are converted into reporting-friendly CSV files.

```text
spark/bi_export/
├── overall_metrics.csv
├── revenue_by_category.csv
├── revenue_by_city.csv
└── sales_trend.csv
```

Data flow:

```text
Gold Parquet
     ↓
Spark Export Job
     ↓
BI-ready CSV
     ↓
Power BI
```

The BI export files should be regenerated from the Gold layer rather than manually modified.

---

# 9. Power BI Dashboard

The Power BI dashboard provides:

- Total Orders
- Total Revenue
- Average Order Value
- Revenue by Category
- Revenue by City
- Sales Trend

Current sample metrics:

| Metric | Value |
|---|---:|
| Total Orders | 7 |
| Total Revenue | ₹449,998 |
| Average Order Value | ₹64,285.43 |

Dashboard documentation is available under:

```text
powerbi/README.md
```

---

# 10. Airflow Orchestration

Apache Airflow orchestrates the analytical processing workflow.

Current DAG flow:

```text
Silver Processing
       ↓
Gold Processing
       ↓
Gold Validation
```

Airflow uses DockerOperator to execute the Spark processing jobs using the Dockerized Spark image.

This separates:

- Workflow orchestration
- Spark processing
- Data storage
- Validation

---

# 11. Docker

The Spark processing environment is containerized.

The Spark image contains:

- Apache Spark 4.0.0
- PySpark
- Application code
- Configuration
- Processing jobs
- Validation jobs

Docker Compose is used to run the Spark processing environment and Airflow services.

---

# Project Structure

```text
ecommerce-data-platform/
│
├── airflow/
│   ├── dags/
│   ├── logs/
│   ├── plugins/
│   ├── config/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── spark/
│   ├── app/
│   │   ├── consumers/
│   │   ├── parsers/
│   │   ├── schemas/
│   │   ├── services/
│   │   ├── session/
│   │   ├── transformers/
│   │   └── utils/
│   │
│   ├── config/
│   ├── tests/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── quarantine/
│   ├── checkpoint/
│   ├── checkpoint_silver/
│   ├── bi_export/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── requirements.txt
│   ├── run.py
│   ├── run_silver.py
│   ├── run_silver_batch.py
│   ├── run_gold.py
│   ├── export_gold_to_csv.py
│   └── validate_gold_metrics.py
│
├── powerbi/
│   ├── ECommerce_Sales_Dashboard.pbix
│   └── README.md
│
├── docs/
│   ├── DATA_DEDUPLICATION.md
│   └── TROUBLESHOOTING.md
│
├── producer.py
├── docker-compose.yml
├── .gitignore
└── README.md
```

---

# Running the Project

## Start Kafka

From the project root:

```powershell
docker compose up -d
```

Verify the containers:

```powershell
docker ps
```

---

## Start the Kafka Producer

Install the Python dependency:

```powershell
pip install kafka-python
```

Run:

```powershell
python producer.py
```

The producer publishes order events to:

```text
orders
```

---

## Run Spark Bronze Streaming

From the Spark directory:

```powershell
cd spark
python run.py
```

The streaming application consumes Kafka events and writes them to Bronze.

---

## Run Silver Processing

```powershell
python run_silver_batch.py
```

Processing flow:

```text
Bronze
  ↓
Validation
  ↓
Quarantine
  ↓
Transformation
  ↓
Deduplication
  ↓
Silver
```

---

## Run Gold Processing

```powershell
python run_gold.py
```

---

## Validate Gold Metrics

```powershell
python validate_gold_metrics.py
```

---

## Generate BI Exports

```powershell
python export_gold_to_csv.py
```

This generates:

```text
spark/bi_export/
├── overall_metrics.csv
├── revenue_by_category.csv
├── revenue_by_city.csv
└── sales_trend.csv
```

---

# Airflow

The Airflow environment is available under:

```text
airflow/
```

The DAG orchestrates:

```text
Silver Processing
      ↓
Gold Processing
      ↓
Gold Validation
```

---

# Data Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

- Event-driven ingestion
- Kafka producers and consumers
- Kafka topic-based decoupling
- Spark Structured Streaming
- Schema enforcement
- Parquet data lake architecture
- Bronze / Silver / Gold architecture
- Data-quality validation
- Quarantine / dead-letter processing
- Deduplication
- Batch data transformation
- Analytical aggregations
- Data validation contracts
- Spark checkpointing
- Docker containerization
- Airflow orchestration
- BI data preparation
- Power BI dashboards
- Git-based development

---

# Engineering Challenges Solved

## Duplicate Data

The initial implementation used order ID-based deduplication.

Testing revealed that this could incorrectly remove legitimate records when the same order ID contained different business attributes.

The implementation was changed to exact-record deduplication.

See:

```text
docs/DATA_DEDUPLICATION.md
```

---

## Invalid Records

Invalid records are separated from valid records rather than being silently dropped.

This allows the pipeline to preserve problematic records for investigation.

---

## Gold Data Validation

Gold metrics are independently recalculated from Silver and compared with the generated Gold dataset.

This helps detect transformation or aggregation errors.

---

# Current Project Status

The core end-to-end pipeline is implemented and validated.

```text
✓ Kafka ingestion
✓ Spark Structured Streaming
✓ Bronze layer
✓ Silver transformation
✓ Data-quality validation
✓ Quarantine layer
✓ Deduplication
✓ Gold aggregations
✓ Gold validation
✓ Dockerized Spark
✓ Airflow orchestration
✓ BI export
✓ Power BI dashboard
✓ Git/GitHub version control
✓ Troubleshooting documentation
```

---

# Future Enhancements

Potential future improvements include:

- Cloud deployment on AWS
- AWS S3 data lake integration
- AWS MSK
- EMR / EMR Serverless
- Infrastructure as Code
- CI/CD pipeline enhancements
- Automated data-quality testing
- Schema evolution handling
- Monitoring and alerting
- Production-grade metadata management
- Incremental Gold processing
- Streaming Silver processing

---

# Author

**Sivakumar Dayalan**

Senior Data Engineer

Core areas:

```text
Data Engineering
Apache Spark
PySpark
Scala
Kafka
AWS
Airflow
Databricks
SQL
Python
Data Quality
Data Platforms
```