from pyspark.sql import SparkSession

from config.settings import APP_NAME
from config.settings import LOG_LEVEL


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(LOG_LEVEL)

    return spark