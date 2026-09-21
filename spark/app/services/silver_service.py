from pyspark.sql import SparkSession

from app.schemas.order_schema import order_schema
from app.transformers.data_quality import validate_orders
from app.transformers.order_transformer import transform_orders

from config.settings import SILVER_CHECKPOINT_LOCATION
from config.settings import SILVER_PATH
from config.settings import QUARANTINE_PATH


BRONZE_PATH = "bronze"


def process_batch(batch_df, batch_id):

    print(f"Processing Silver batch: {batch_id}")

    valid_df, invalid_df = validate_orders(batch_df)

    silver_df = transform_orders(valid_df)

    (
        silver_df
        .write
        .mode("append")
        .format("parquet")
        .save(SILVER_PATH)
    )

    (
        invalid_df
        .write
        .mode("append")
        .format("parquet")
        .save(QUARANTINE_PATH)
    )

    print(f"Completed Silver batch: {batch_id}")


def start_silver_stream():

    spark = (
        SparkSession.builder
        .appName("ECommerceSilver")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    bronze_df = (
        spark.readStream
        .schema(order_schema)
        .format("parquet")
        .load(BRONZE_PATH)
    )

    query = (
        bronze_df
        .writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            SILVER_CHECKPOINT_LOCATION
        )
        .start()
    )

    query.awaitTermination()