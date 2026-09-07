from app.consumers.kafka_consumer import read_stream
from app.parsers.order_parser import parse_orders

from config.settings import CHECKPOINT_LOCATION
from config.settings import BRONZE_PATH


def start_stream():

    kafka_df = read_stream()

    orders = parse_orders(kafka_df)

    query = (
        orders
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            BRONZE_PATH
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_LOCATION
        )
        .start()
    )

    query.awaitTermination()