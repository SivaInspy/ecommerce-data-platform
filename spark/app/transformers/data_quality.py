from pyspark.sql import DataFrame

from pyspark.sql.functions import col
from pyspark.sql.functions import when


def validate_orders(df: DataFrame):

    validated_df = (
        df
        .withColumn(
            "error_reason",
            when(
                col("order_id").isNull(),
                "MISSING_ORDER_ID"
            )
            .when(
                col("customer_id").isNull(),
                "MISSING_CUSTOMER_ID"
            )
            .when(
                col("price").isNull(),
                "MISSING_PRICE"
            )
            .when(
                col("price") <= 0,
                "INVALID_PRICE"
            )
            .when(
                col("order_time").isNull(),
                "MISSING_ORDER_TIME"
            )
            .otherwise(None)
        )
    )

    valid_df = (
        validated_df
        .filter(
            col("error_reason").isNull()
        )
        .drop("error_reason")
    )

    invalid_df = (
        validated_df
        .filter(
            col("error_reason").isNotNull()
        )
        .dropDuplicates(
            ["order_id", "error_reason"]
        )
    )

    return valid_df, invalid_df