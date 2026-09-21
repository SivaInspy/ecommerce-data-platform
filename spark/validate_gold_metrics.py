from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import sum
from pyspark.sql.functions import avg


SILVER_PATH = "silver"
GOLD_PATH = "gold"


spark = (
    SparkSession.builder
    .appName("ValidateGoldMetrics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Read Silver
silver_df = spark.read.parquet(SILVER_PATH)


# Calculate expected metrics directly from Silver
expected = (
    silver_df
    .agg(
        count("order_id").alias("total_orders"),
        sum("price").alias("total_revenue"),
        avg("price").alias("average_order_value")
    )
)


# Read Gold
gold = spark.read.parquet(
    f"{GOLD_PATH}/overall_metrics"
)


print("\nExpected metrics from Silver:")
expected.show(truncate=False)


print("Actual metrics from Gold:")
gold.show(truncate=False)


expected_values = expected.collect()[0]
gold_values = gold.collect()[0]


assert expected_values["total_orders"] == gold_values["total_orders"]
assert expected_values["total_revenue"] == gold_values["total_revenue"]

assert abs(
    expected_values["average_order_value"]
    - gold_values["average_order_value"]
) < 0.01


print("\n✓ Gold metrics match Silver source data.")
print("✓ Validation successful.")


spark.stop()