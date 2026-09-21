from pyspark.sql import SparkSession

from app.schemas.order_schema import silver_order_schema

from app.transformers.gold_transformer import revenue_by_category
from app.transformers.gold_transformer import revenue_by_city
from app.transformers.gold_transformer import overall_metrics
from app.transformers.gold_transformer import sales_trend

from config.settings import SILVER_PATH


GOLD_PATH = "gold"


def start_gold_job():

    spark = (
        SparkSession.builder
        .appName("ECommerceGold")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    silver_df = (
        spark.read
        .schema(silver_order_schema)
        .format("parquet")
        .load(SILVER_PATH)
    )

    category_df = revenue_by_category(silver_df)

    city_df = revenue_by_city(silver_df)

    metrics_df = overall_metrics(silver_df)

    trend_df = sales_trend(silver_df)

    category_df.show(truncate=False)

    city_df.show(truncate=False)

    metrics_df.show(truncate=False)

    trend_df.show(truncate=False)

    (
        category_df
        .write
        .mode("overwrite")
        .format("parquet")
        .save(f"{GOLD_PATH}/revenue_by_category")
    )

    (
        city_df
        .write
        .mode("overwrite")
        .format("parquet")
        .save(f"{GOLD_PATH}/revenue_by_city")
    )

    (
        metrics_df
        .write
        .mode("overwrite")
        .format("parquet")
        .save(f"{GOLD_PATH}/overall_metrics")
    )

    (
        trend_df
        .write
        .mode("overwrite")
        .format("parquet")
        .save(f"{GOLD_PATH}/sales_trend")
    )

    spark.stop()