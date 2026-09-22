from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import sum
from pyspark.sql.functions import avg

from app.schemas.order_schema import silver_order_schema

from config.settings import SILVER_PATH


GOLD_PATH = "gold"


spark = (
    SparkSession.builder
    .appName("ValidateGoldMetrics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ---------------------------------------------------------
# Read Silver
# ---------------------------------------------------------

print("\nReading Silver data...")

silver_df = (
    spark.read
    .schema(silver_order_schema)
    .format("parquet")
    .load(SILVER_PATH)
)

print(f"Silver record count: {silver_df.count()}")


# ---------------------------------------------------------
# Calculate expected metrics directly from Silver
# ---------------------------------------------------------

print("\nCalculating expected metrics from Silver...")

expected = (
    silver_df
    .agg(
        count("order_id").alias("total_orders"),
        sum("price").alias("total_revenue"),
        avg("price").alias("average_order_value")
    )
)


# ---------------------------------------------------------
# Read Gold metrics
# ---------------------------------------------------------

print("Reading Gold overall metrics...")

gold = (
    spark.read
    .format("parquet")
    .load(f"{GOLD_PATH}/overall_metrics")
)


# ---------------------------------------------------------
# Display results
# ---------------------------------------------------------

print("\nExpected metrics from Silver:")
expected.show(truncate=False)

print("Actual metrics from Gold:")
gold.show(truncate=False)


# ---------------------------------------------------------
# Collect metrics
# ---------------------------------------------------------

expected_values = expected.collect()[0]
gold_values = gold.collect()[0]


expected_orders = expected_values["total_orders"]
gold_orders = gold_values["total_orders"]

expected_revenue = expected_values["total_revenue"]
gold_revenue = gold_values["total_revenue"]

expected_aov = expected_values["average_order_value"]
gold_aov = gold_values["average_order_value"]


# ---------------------------------------------------------
# Validate metrics
# ---------------------------------------------------------

print("\nValidating total orders...")

assert expected_orders == gold_orders, (
    f"Total orders mismatch: "
    f"expected={expected_orders}, actual={gold_orders}"
)

print("✓ Total orders match.")


print("Validating total revenue...")

assert expected_revenue == gold_revenue, (
    f"Total revenue mismatch: "
    f"expected={expected_revenue}, actual={gold_revenue}"
)

print("✓ Total revenue matches.")


print("Validating average order value...")

assert abs(expected_aov - gold_aov) < 0.01, (
    f"Average order value mismatch: "
    f"expected={expected_aov}, actual={gold_aov}"
)

print("✓ Average order value matches.")


# ---------------------------------------------------------
# Final result
# ---------------------------------------------------------

print("\n✓ Gold metrics match Silver source data.")
print("✓ Validation successful.")


spark.stop()