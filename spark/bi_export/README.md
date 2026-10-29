# BI Export Layer

This directory contains reporting-ready exports generated from the Spark Gold layer.

## Purpose

The BI Export layer provides simple file-based datasets for reporting tools such as Power BI.

The authoritative analytical data remains in the Spark Gold layer.

## Datasets

- `revenue_by_category.csv`
- `revenue_by_city.csv`
- `overall_metrics.csv`
- `sales_trend.csv`

## Data Flow

Kafka → Bronze → Silver → Gold → BI Export → Power BI

## Important

The BI Export layer should be regenerated from Gold whenever the pipeline is refreshed.

It should not be manually edited.

# BI export

This folder contains CSV snapshots exported from the four Spark Gold Parquet datasets. Regenerate from `spark/` with `python export_gold_to_csv.py`. The job replaces each CSV file and writes a header; export output should not be edited manually. The Gold datasets remain the analytical source of truth. See [Power BI instructions](../../powerbi/README.md).
