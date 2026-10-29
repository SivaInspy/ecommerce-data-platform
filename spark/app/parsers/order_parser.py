"""Convert Kafka message values into typed order columns."""

from pyspark.sql.functions import col
from pyspark.sql.functions import from_json

from app.schemas.order_schema import order_schema


def parse_orders(orders_df):
    """Parse each Kafka ``value`` field as JSON using ``order_schema``.

    Args:
        orders_df: Streaming or batch DataFrame with Kafka's binary ``value`` column.

    Returns:
        DataFrame: One column per order field. Malformed JSON produces null fields.
    """

    # Kafka stores payloads as bytes; Spark's JSON parser expects text.
    kafka_values_df = (
        orders_df.selectExpr(
            "CAST(value AS STRING) AS value"
        )
    )

    parsed_orders_df = (
        kafka_values_df.select(
            from_json(
                col("value"),
                order_schema
            ).alias("parsed_order")
        )
    )

    return parsed_orders_df.select("parsed_order.*")
