"""Bronze streaming query lifecycle."""

from app.consumers.kafka_consumer import read_kafka_orders
from app.parsers.order_parser import parse_orders

from config.settings import BRONZE_CHECKPOINT_LOCATION
from config.settings import BRONZE_PATH


def start_bronze_stream():
    """Read Kafka orders, append parsed records to Bronze Parquet, and await shutdown.

    The configured checkpoint stores query progress so the stream can resume after
    a restart using the same checkpoint directory.
    """

    kafka_orders_df = read_kafka_orders()

    parsed_orders_df = parse_orders(kafka_orders_df)

    bronze_streaming_query = (
        parsed_orders_df
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            BRONZE_PATH
        )
        .option(
            "checkpointLocation",
            BRONZE_CHECKPOINT_LOCATION
        )
        .start()
    )

    bronze_streaming_query.awaitTermination()
