import os

from pyspark.sql import SparkSession

from app.schemas.order_schema import order_schema
from app.transformers.data_quality import validate_orders
from app.transformers.order_transformer import transform_orders

from config.settings import SILVER_PATH
from config.settings import QUARANTINE_PATH


# Bronze streaming output contains multiple Parquet files.
BRONZE_PATH = "bronze/*.parquet"


def start_silver_batch():

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

    bronze_df = (
        spark.read
        .schema(order_schema)
        .format("parquet")
        .load(BRONZE_PATH)
    )

    bronze_count = bronze_df.count()

    print(f"Bronze record count: {bronze_count}")

    # ---------------------------------------------------------
    # Data quality validation
    # ---------------------------------------------------------

    print("Running data-quality validation...")

    valid_df, invalid_df = validate_orders(bronze_df)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    print(f"Valid records: {valid_count}")
    print(f"Invalid records: {invalid_count}")

    # ---------------------------------------------------------
    # Silver transformation
    # ---------------------------------------------------------

    print("Transforming Silver data...")

    silver_df = transform_orders(valid_df)

    silver_count = silver_df.count()

    print(f"Silver record count: {silver_count}")

    # ---------------------------------------------------------
    # Silver output
    # ---------------------------------------------------------

    print("Preparing Silver output directory...")

    os.makedirs(SILVER_PATH, exist_ok=True)

    (
        silver_df.write
        .mode("overwrite")
        .format("parquet")
        .save(SILVER_PATH)
    )

    print(f"Silver data written to: {SILVER_PATH}")

    # ---------------------------------------------------------
    # Quarantine output
    # ---------------------------------------------------------

    if invalid_count > 0:

        print("Preparing Quarantine output directory...")

        os.makedirs(QUARANTINE_PATH, exist_ok=True)

        (
            invalid_df.write
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