from pyspark.sql import DataFrame

from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp


def transform_orders(df: DataFrame) -> DataFrame:

    transformed_df = (
        df
        .withColumn(
            "order_time",
            to_timestamp(
                col("order_time"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        .filter(
            col("order_id").isNotNull()
        )
        .filter(
            col("customer_id").isNotNull()
        )
        .filter(
            col("price").isNotNull()
        )
        .filter(
            col("price") > 0
        )
        .dropDuplicates(
            ["order_id"]
        )
    )

    return transformed_df