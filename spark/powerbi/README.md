# Power BI Dashboard

## Real-Time E-Commerce Data Platform

This directory contains the Power BI reporting documentation and dashboard assets for the Real-Time E-Commerce Data Platform.

## Data Source

Power BI consumes reporting-ready CSV datasets generated from the Spark Gold layer.

### Datasets

- `revenue_by_category.csv`
- `revenue_by_city.csv`
- `overall_metrics.csv`
- `sales_trend.csv`

## Data Flow

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze
  ↓
Silver
  ↓
Gold
  ↓
BI Export
  ↓
Power BI