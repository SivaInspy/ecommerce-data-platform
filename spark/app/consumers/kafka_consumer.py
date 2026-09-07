from pyspark.sql import DataFrame

from app.session.spark_session import create_spark_session

from config.settings import KAFKA_BOOTSTRAP_SERVERS
from config.settings import KAFKA_TOPIC


def read_stream() -> DataFrame:

    spark = create_spark_session()

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            KAFKA_TOPIC
        )
        .load()
    )

    return kafka_df