"""Inspect Bronze quality results and repeated order IDs for analysis."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import sum

from app.schemas.order_schema import order_schema
from app.transformers.data_quality import validate_orders


# Glob all streaming Parquet part files in Bronze.
BRONZE_PATH = "bronze/*.parquet"


spark = (
    SparkSession.builder
    .appName("InspectBronzeDuplicates")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


print("\n===== READING BRONZE DATA =====")

bronze_df = (
    spark.read
    .schema(order_schema)
    .format("parquet")
    .load(BRONZE_PATH)
)

bronze_record_count = bronze_df.count()

print(f"Bronze records: {bronze_record_count}")


print("\n===== BRONZE DATA =====")

bronze_df.orderBy("order_id").show(
    100,
    truncate=False
)


print("\n===== DATA QUALITY VALIDATION =====")

valid_orders_df, invalid_orders_df = validate_orders(bronze_df)

valid_order_count = valid_orders_df.count()
invalid_order_count = invalid_orders_df.count()

print(f"Valid records: {valid_order_count}")
print(f"Invalid records: {invalid_order_count}")


print("\n===== INVALID RECORDS =====")

invalid_orders_df.orderBy("order_id").show(
    100,
    truncate=False
)


print("\n===== DUPLICATE ORDER IDS =====")

# Identify IDs with multiple valid input rows; this is diagnostic, not the
# exact-row deduplication operation used by the Silver transformer.
duplicate_ids = (
    valid_orders_df
    .groupBy("order_id")
    .agg(
        count("*").alias("record_count")
    )
    .filter(col("record_count") > 1)
    .orderBy(col("record_count").desc(), col("order_id"))
)

duplicate_ids.show(
    100,
    truncate=False
)


duplicate_id_count = duplicate_ids.count()

print(
    f"Number of order IDs appearing more than once: "
    f"{duplicate_id_count}"
)


print("\n===== RECORDS REMOVED BY DEDUPLICATION =====")

total_valid_records = valid_orders_df.count()

distinct_order_ids = (
    valid_orders_df
    .select("order_id")
    .distinct()
    .count()
)

records_removed = total_valid_records - distinct_order_ids

print(f"Valid records before deduplication: {total_valid_records}")
print(f"Distinct order IDs: {distinct_order_ids}")
print(f"Records removed by deduplication: {records_removed}")


print("\n===== DUPLICATE RECORD DETAILS =====")

duplicate_order_id_list = (
    duplicate_ids
    .select("order_id")
)

duplicate_order_records_df = (
    valid_orders_df
    .join(
        duplicate_order_id_list,
        on="order_id",
        how="inner"
    )
    .orderBy("order_id", "order_time")
)

duplicate_order_records_df.show(
    100,
    truncate=False
)


print("\n===== SUMMARY =====")

print(f"Bronze records                : {bronze_record_count}")
print(f"Valid records                 : {valid_order_count}")
print(f"Invalid records               : {invalid_order_count}")
print(f"Distinct valid order IDs      : {distinct_order_ids}")
print(f"Records removed by dedup      : {records_removed}")

print("\nDuplicate analysis completed.")

spark.stop()
