"""Print schemas, sample rows, and counts for each persisted Gold dataset."""

from pyspark.sql import SparkSession

# Gold dataset directories are relative to the current working directory.
GOLD_PATH = "gold"

spark = (
    SparkSession.builder
    .appName("ValidateGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def validate_dataset(dataset_name):
    """Display one Gold dataset's schema, rows, and record count.

    Args:
        dataset_name: Dataset directory under ``GOLD_PATH``.
    """
    dataset_path = f"{GOLD_PATH}/{dataset_name}"

    print("\n" + "=" * 60)
    print(f"DATASET: {dataset_name}")
    print("=" * 60)

    gold_dataset_df = spark.read.parquet(dataset_path)

    print("\nSchema:")
    gold_dataset_df.printSchema()

    print("\nData:")
    gold_dataset_df.show(truncate=False)

    print(f"Record count: {gold_dataset_df.count()}")


datasets = [
    "revenue_by_category",
    "revenue_by_city",
    "overall_metrics",
    "sales_trend"
]


for dataset in datasets:
    validate_dataset(dataset)


spark.stop()

print("\nGold validation completed successfully.")
