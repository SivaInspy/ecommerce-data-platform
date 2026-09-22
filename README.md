# Real-Time E-Commerce Data Platform

## Overview

This project demonstrates a production-style real-time data engineering pipeline using Kafka, Apache Spark Structured Streaming, Docker, Airflow, and Parquet.

---

## Low-Level Architecture

Producer (Python)
        │
        ▼
Apache Kafka
        │
        ▼
Spark Structured Streaming
        │
        ▼
Bronze Layer (Parquet)
        │
        ▼
Silver Layer
        │
        ▼
Gold Layer
        │
        ▼
Power BI Dashboard

---

## Tech Stack

- Python
- Apache Kafka
- Apache Spark
- Docker
- Apache Airflow
- Parquet
- GitHub Actions

---

## Current Status

🚧 Project is under active development.

---

## Architecture Overview

The platform implements a real-time e-commerce data pipeline using
Apache Kafka and Spark Structured Streaming.

1. **Ingestion** – Python producers generate e-commerce order events
   and publish them to the Kafka `orders` topic.

2. **Bronze Layer** – Spark Structured Streaming consumes Kafka events
   and persists source-oriented data in Parquet format.

3. **Silver Layer** – Data is validated, transformed, typed and
   deduplicated to produce clean, business-ready datasets.

4. **Quarantine Layer** – Invalid records are isolated for data-quality
   investigation instead of being silently discarded.

5. **Gold Layer** – Business-level aggregations are generated for
   revenue, orders, cities, categories and sales trends.

6. **Analytics** – Power BI consumes Gold datasets to provide
   business dashboards and KPIs.

7. **Orchestration & Operations** – Airflow, Docker, GitHub Actions,
   automated testing, logging and Spark checkpointing support
   reliable and maintainable pipeline operations.

---

## 🏗️ High-Level Architecture

```markdown
                         REAL-TIME E-COMMERCE DATA PLATFORM

 ┌──────────────────────┐
 │  E-Commerce Sources  │
 │                      │
 │  Web / Mobile / App  │
 │  Order Events        │
 └──────────┬───────────┘
            │
            │ JSON Events
            ▼
 ┌──────────────────────┐
 │   Kafka Producer     │
 │      Python          │
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────────────────┐
 │          Apache Kafka            │
 │                                  │
 │          orders topic            │
 │                                  │
 │   Partitions • Offsets • Events  │
 └──────────┬───────────────────────┘
            │
            │ Structured Streaming
            ▼
 ┌──────────────────────────────────┐
 │     Spark Structured Streaming   │
 │                                  │
 │  Consume → Parse → Transform     │
 └──────────┬───────────────────────┘
            │
            ▼
 ┌──────────────────────┐
 │     BRONZE LAYER     │
 │                      │
 │   Raw / Source Data  │
 │       Parquet        │
 └──────────┬───────────┘
            │
            │ Streaming
            ▼
 ┌──────────────────────────────────┐
 │          SILVER LAYER            │
 │                                  │
 │  • Data Validation               │
 │  • Type Conversion               │
 │  • Deduplication                 │
 │  • Data Quality                  │
 │  • Business-ready Data           │
 └──────────┬───────────────────────┘
            │
       ┌────┴──────────────┐
       │                   │
       ▼                   ▼
 ┌───────────────┐   ┌────────────────┐
 │ Silver        │   │  Quarantine /  │
 │ Parquet       │   │  Dead-Letter   │
 │               │   │                │
 │ Valid Records │   │ Invalid Records│
 └───────┬───────┘   └────────────────┘
         │
         ▼
 ┌──────────────────────────────────┐
 │            GOLD LAYER            │
 │                                  │
 │  Business Aggregations           │
 │                                  │
 │  • Revenue by Category           │
 │  • Revenue by City               │
 │  • Order Metrics                 │
 │  • Average Order Value           │
 │  • Sales Trends                  │
 └──────────┬───────────────────────┘
            │
            ▼
 ┌──────────────────────┐
 │       Power BI       │
 │                      │
 │  Business Dashboard  │
 │  KPIs • Trends       │
 │  Analytics           │
 └──────────────────────┘


 ┌────────────────────────────────────────────────────────────────┐
 │                    PLATFORM / OPERATIONS                       │
 │                                                                │
 │ Docker • Airflow • Git • GitHub Actions • Testing • Logging    │
 │ Checkpointing • Configuration • Monitoring                     │
 └────────────────────────────────────────────────────────────────┘