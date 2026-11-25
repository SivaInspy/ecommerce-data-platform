"""Silver streaming job that validates and persists each Bronze micro-batch."""

from pyspark.sql import SparkSession

from app.schemas.order_schema import order_schema
from app.transformers.data_quality import validate_orders
from app.transformers.order_transformer import transform_orders

from config.settings import SILVER_CHECKPOINT_LOCATION
from config.settings import SILVER_PATH
from config.settings import QUARANTINE_PATH


# Source path is relative to the Spark process working directory.
BRONZE_PATH = "bronze"


def process_silver_batch(bronze_batch_df, micro_batch_id):
    """Validate and append one streaming micro-batch to Silver/quarantine.

    Args:
        bronze_batch_df: Bronze rows for the current micro-batch.
        micro_batch_id: Spark's monotonically assigned micro-batch identifier.
    """

    print(f"Processing Silver batch: {micro_batch_id}")

    valid_orders_df, invalid_orders_df = validate_orders(bronze_batch_df)

    silver_orders_df = transform_orders(valid_orders_df)

    (
        silver_orders_df
        .write
        .mode("append")
        .format("parquet")
        .save(SILVER_PATH)
    )

    (
        invalid_orders_df
        .write
        .mode("append")
        .format("parquet")
        .save(QUARANTINE_PATH)
    )

    print(f"Completed Silver batch: {micro_batch_id}")


def start_silver_stream():
    """Stream Parquet files from Bronze through ``process_silver_batch`` until stopped.

    The configured checkpoint tracks progress for this streaming query.
    """

    spark = (
        SparkSession.builder
        .appName("ECommerceSilver")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    bronze_orders_df = (
        spark.readStream
        .schema(order_schema)
        .format("parquet")
        .load(BRONZE_PATH)
    )

    silver_streaming_query = (
        bronze_orders_df
        .writeStream
        .foreachBatch(process_silver_batch)
        .option(
            "checkpointLocation",
            SILVER_CHECKPOINT_LOCATION
        )
        .start()
    )

    silver_streaming_query.awaitTermination()
