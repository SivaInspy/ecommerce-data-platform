"""Batch entry point for building Silver and quarantine Parquet outputs."""

import os

from pyspark.sql import SparkSession

from app.schemas.order_schema import order_schema
from app.transformers.data_quality import validate_orders
from app.transformers.order_transformer import transform_orders

from config.settings import SILVER_PATH
from config.settings import QUARANTINE_PATH


# Bronze streaming output contains multiple Parquet part files.
BRONZE_PATH = "bronze/*.parquet"


def start_silver_batch():
    """Read Bronze files, validate and deduplicate orders, then overwrite outputs.

    Valid orders replace ``SILVER_PATH``. Invalid rows replace the quarantine
    output only when the current input contains invalid rows.
    """

    spark = (
        SparkSession.builder
        .appName("ECommerceSilverBatch")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # ---------------------------------------------------------
    # Read Bronze
    # ---------------------------------------------------------

    print("Reading Bronze data...")

    bronze_orders_df = (
        spark.read
        .schema(order_schema)
        .format("parquet")
        .load(BRONZE_PATH)
    )

    bronze_record_count = bronze_orders_df.count()

    print(f"Bronze record count: {bronze_record_count}")

    # ---------------------------------------------------------
    # Data quality validation
    # ---------------------------------------------------------

    print("Running data-quality validation...")

    valid_orders_df, invalid_orders_df = validate_orders(bronze_orders_df)

    valid_order_count = valid_orders_df.count()
    invalid_order_count = invalid_orders_df.count()

    print(f"Valid records: {valid_order_count}")
    print(f"Invalid records: {invalid_order_count}")

    # ---------------------------------------------------------
    # Silver transformation
    # ---------------------------------------------------------

    print("Transforming Silver data...")

    silver_orders_df = transform_orders(valid_orders_df)

    silver_order_count = silver_orders_df.count()

    print(f"Silver record count: {silver_order_count}")

    # ---------------------------------------------------------
    # Silver output
    # ---------------------------------------------------------

    print("Preparing Silver output directory...")

    os.makedirs(SILVER_PATH, exist_ok=True)

    (
        silver_orders_df.write
        .mode("overwrite")
        .format("parquet")
        .save(SILVER_PATH)
    )

    print(f"Silver data written to: {SILVER_PATH}")

    # ---------------------------------------------------------
    # Quarantine output
    # ---------------------------------------------------------

    if invalid_order_count > 0:

        print("Preparing Quarantine output directory...")

        os.makedirs(QUARANTINE_PATH, exist_ok=True)

        (
            invalid_orders_df.write
            .mode("overwrite")
            .format("parquet")
            .save(QUARANTINE_PATH)
        )

        print(f"Quarantine data written to: {QUARANTINE_PATH}")

    else:

        print("No invalid records found. Quarantine output not created.")

    # ---------------------------------------------------------
    # Complete
    # ---------------------------------------------------------

    print("Silver batch completed successfully.")

    spark.stop()


if __name__ == "__main__":
    start_silver_batch()
