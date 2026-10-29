"""Factory for the Spark session used by the streaming application."""

from pyspark.sql import SparkSession

from config.settings import APP_NAME
from config.settings import LOG_LEVEL


def create_spark_session():
    """Create or reuse the local Spark session with the Kafka connector enabled.

    Returns:
        SparkSession: Session configured for the application's name and log level.
    """

    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            # Keep this artifact aligned with the Spark and Scala versions in use.
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(LOG_LEVEL)

    return spark
