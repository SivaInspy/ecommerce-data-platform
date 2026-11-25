"""Validation rules that split order records into valid and quarantine rows."""

from pyspark.sql import DataFrame

from pyspark.sql.functions import col
from pyspark.sql.functions import when


def validate_orders(orders_df: DataFrame):
    """Classify orders using the first failing required-field/price rule.

    Args:
        orders_df: DataFrame conforming to the Bronze order schema.

    Returns:
        tuple[DataFrame, DataFrame]: Valid rows without ``error_reason`` and
        invalid rows with the reason they failed. Invalid rows are deduplicated
        by ``(order_id, error_reason)``.
    """

    # Chained when clauses intentionally report only the first failure per row.
    validated_df = (
        orders_df
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

    valid_orders_df = (
        validated_df
        .filter(
            col("error_reason").isNull()
        )
        .drop("error_reason")
    )

    invalid_orders_df = (
        validated_df
        .filter(
            col("error_reason").isNotNull()
        )
        .dropDuplicates(
            ["order_id", "error_reason"]
        )
    )

    return valid_orders_df, invalid_orders_df
