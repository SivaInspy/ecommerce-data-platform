"""Transform validated Bronze orders into the Silver representation."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp


def transform_orders(df: DataFrame) -> DataFrame:
    """Convert order timestamps, enforce core fields, and remove exact duplicates.

    Args:
        df: DataFrame containing records accepted by ``validate_orders``.

    Returns:
        DataFrame: Valid orders with timestamp-typed ``order_time``. Records
        sharing an order ID are retained when any field differs.
    """

    # Deduplicate the full row because order_id is not guaranteed to be unique.
    transformed_df = (
        df
        .withColumn(
            "order_time",
            to_timestamp(
                col("order_time"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("price").isNotNull())
        .filter(col("price") > 0)
        .dropDuplicates()
    )

    return transformed_df
