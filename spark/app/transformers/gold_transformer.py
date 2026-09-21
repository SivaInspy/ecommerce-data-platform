from pyspark.sql import DataFrame

from pyspark.sql.functions import avg
from pyspark.sql.functions import count
from pyspark.sql.functions import sum
from pyspark.sql.functions import to_date


def revenue_by_category(df: DataFrame) -> DataFrame:

    return (
        df
        .groupBy("category")
        .agg(
            sum("price").alias("total_revenue"),
            count("order_id").alias("total_orders")
        )
        .orderBy("total_revenue", ascending=False)
    )


def revenue_by_city(df: DataFrame) -> DataFrame:

    return (
        df
        .groupBy("city")
        .agg(
            sum("price").alias("total_revenue"),
            count("order_id").alias("total_orders")
        )
        .orderBy("total_revenue", ascending=False)
    )


def overall_metrics(df: DataFrame) -> DataFrame:

    return (
        df
        .agg(
            count("order_id").alias("total_orders"),
            sum("price").alias("total_revenue"),
            avg("price").alias("average_order_value")
        )
    )


def sales_trend(df: DataFrame) -> DataFrame:

    return (
        df
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