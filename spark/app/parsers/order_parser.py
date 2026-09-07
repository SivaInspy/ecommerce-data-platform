from pyspark.sql.functions import col
from pyspark.sql.functions import from_json

from app.schemas.order_schema import order_schema


def parse_orders(df):

    json_df = (
        df.selectExpr(
            "CAST(value AS STRING) AS value"
        )
    )

    parsed_df = (
        json_df.select(
            from_json(
                col("value"),
                order_schema
            ).alias("data")
        )
    )

    return parsed_df.select("data.*")