# Spark jobs

See the [root README](../README.md) for the complete local run sequence and [architecture](../docs/ARCHITECTURE.md) for schemas and paths. Run these scripts with the working directory set to `spark/` because input/output paths are relative.

## Entry points

| Command | Function |
|---|---|
| `python run.py` | Start Kafka Structured Streaming into Bronze (long-running) |
| `python run_silver_batch.py` | Read Bronze Parquet, validate, write Silver and quarantine |
| `python run_silver.py` | Alternate Silver entry point; inspect its current implementation before use |
| `python run_gold.py` | Aggregate Silver to four Gold Parquet datasets |
| `python validate_gold_metrics.py` | Compare overall Gold metrics against Silver |
| `python export_gold_to_csv.py` | Export Gold datasets to one CSV file apiece under `bi_export/` |
| `python inspect_duplicates.py` | Inspect duplicate order IDs in Bronze |
| `python validate_gold.py` | Additional Gold validation script |

## Configuration and dependencies

Install `requirements.txt` in a PySpark-compatible environment. Kafka connection, topic, storage paths, and checkpoint names are in `config/settings.py`. `run.py` needs the Kafka connector configured in `app/session/spark_session.py`; the local batch commands do not consume Kafka. For Docker, build the image from this directory with `docker build -t ecommerce-spark:latest .`.

## Output locations

All paths are relative to the current working directory: `bronze/`, `silver/data/`, `quarantine/data/`, `gold/`, and `bi_export/`. Batch Silver and Gold writes use overwrite mode. The CSV export removes/replaces existing export files and should be regenerated from Gold rather than edited by hand.
