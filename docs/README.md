# Documentation

Start with the [repository README](../README.md) for setup and the end-to-end run sequence.

| Document | Covers |
|---|---|
| [Architecture](ARCHITECTURE.md) | Components, data flow, schemas, layer outputs, quality rules, and entry points |
| [Data deduplication](DATA_DEDUPLICATION.md) | What the current exact-row rule means and its limits |
| [Troubleshooting](TROUBLESHOOTING.md) | Local startup, Spark/Kafka, paths, Airflow, and Docker issues |

Component instructions: [producer](../producer/README.md), [Spark](../spark/README.md), [Airflow](../airflow/README.md), [Docker](../docker/README.md), and [Power BI](../powerbi/README.md).

Runtime paths and sample counts may vary with the data written locally. Treat code and configuration as authoritative when documentation and implementation differ; report a mismatch so both can be corrected.
