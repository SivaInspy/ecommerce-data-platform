from pyspark.sql import SparkSession

GOLD_PATH = "gold"

spark = (
    SparkSession.builder
    .appName("ValidateGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def validate_dataset(name):
    path = f"{GOLD_PATH}/{name}"

    print("\n" + "=" * 60)
    print(f"DATASET: {name}")
    print("=" * 60)

    df = spark.read.parquet(path)

    print("\nSchema:")
    df.printSchema()

    print("\nData:")
    df.show(truncate=False)

    print(f"Record count: {df.count()}")


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