"""Business aggregations that shape Silver orders into Gold datasets."""

from pyspark.sql import DataFrame

from pyspark.sql.functions import avg
from pyspark.sql.functions import count
from pyspark.sql.functions import sum
from pyspark.sql.functions import to_date


def revenue_by_category(orders_df: DataFrame) -> DataFrame:
    """Aggregate order count and revenue by product category.

    Args:
        orders_df: Silver order DataFrame.

    Returns:
        DataFrame: Category, total revenue, and order count, sorted by revenue.
    """

    return (
        orders_df
        .groupBy("category")
        .agg(
            sum("price").alias("total_revenue"),
            count("order_id").alias("total_orders")
        )
        .orderBy("total_revenue", ascending=False)
    )


def revenue_by_city(orders_df: DataFrame) -> DataFrame:
    """Aggregate order count and revenue by city.

    Args:
        orders_df: Silver order DataFrame.

    Returns:
        DataFrame: City, total revenue, and order count, sorted by revenue.
    """

    return (
        orders_df
        .groupBy("city")
        .agg(
            sum("price").alias("total_revenue"),
            count("order_id").alias("total_orders")
        )
        .orderBy("total_revenue", ascending=False)
    )


def overall_metrics(orders_df: DataFrame) -> DataFrame:
    """Calculate total orders, total revenue, and average order value.

    Args:
        orders_df: Silver order DataFrame.

    Returns:
        DataFrame: A single-row DataFrame containing the overall metrics.
    """

    return (
        orders_df
        .agg(
            count("order_id").alias("total_orders"),
            sum("price").alias("total_revenue"),
            avg("price").alias("average_order_value")
        )
    )


def sales_trend(orders_df: DataFrame) -> DataFrame:
    """Aggregate order count and revenue by calendar date.

    Args:
        orders_df: Silver order DataFrame with timestamp-typed ``order_time``.

    Returns:
        DataFrame: Date, total revenue, and order count, sorted by date.
    """

    return (
        orders_df
        .withColumn(
            "order_date",
            to_date("order_time")
        )
        .groupBy("order_date")
        .agg(
            count("order_id").alias("total_orders"),
            sum("price").alias("total_revenue")
        )
        .orderBy("order_date")
    )
