import os
import shutil

from pyspark.sql import SparkSession


GOLD_PATH = "gold"
EXPORT_PATH = "bi_export"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GoldToBIExport")
        .master("local[*]")
        .getOrCreate()
    )


def export_dataset(spark, dataset_name, output_filename):
    input_path = f"{GOLD_PATH}/{dataset_name}"
    temp_path = f"{EXPORT_PATH}/_{dataset_name}_temp"
    final_path = f"{EXPORT_PATH}/{output_filename}"

    print(f"\nReading: {input_path}")

    df = (
        spark.read
        .format("parquet")
        .load(input_path)
    )

    record_count = df.count()

    print(f"Records: {record_count}")

    # Remove previous temporary output if it exists
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)

    # Remove previous final CSV if it exists
    if os.path.exists(final_path):
        os.remove(final_path)

    # Spark writes CSV output as a directory.
    # coalesce(1) ensures that only one CSV part file is created.
    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(temp_path)
    )

    # Find the generated CSV file.
    generated_csv = None

    for filename in os.listdir(temp_path):
        if filename.endswith(".csv"):
            generated_csv = os.path.join(temp_path, filename)
            break

    if generated_csv is None:
        raise FileNotFoundError(
            f"No CSV file generated for dataset: {dataset_name}"
        )

    # Move the generated CSV to the final BI export location.
    shutil.move(generated_csv, final_path)

    # Remove Spark temporary output directory.
    shutil.rmtree(temp_path)

    print(f"Exported to: {final_path}")


def export_gold_datasets():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("Starting Gold to BI export...")

    os.makedirs(EXPORT_PATH, exist_ok=True)

    datasets = {
        "revenue_by_category": "revenue_by_category.csv",
        "revenue_by_city": "revenue_by_city.csv",
        "overall_metrics": "overall_metrics.csv",
        "sales_trend": "sales_trend.csv",
    }

    for dataset_name, output_filename in datasets.items():
        export_dataset(
            spark,
            dataset_name,
            output_filename
        )

    spark.stop()

    print("\nGold to BI export completed successfully.")


if __name__ == "__main__":
    export_gold_datasets()