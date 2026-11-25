"""Export Gold Parquet datasets as single-file CSVs for BI consumers."""

import os
import shutil

from pyspark.sql import SparkSession


# Paths are relative to the current working directory (normally ``spark/``).
GOLD_PATH = "gold"
EXPORT_PATH = "bi_export"


def create_spark_session():
    """Create the local Spark session used to read Gold datasets."""
    return (
        SparkSession.builder
        .appName("GoldToBIExport")
        .master("local[*]")
        .getOrCreate()
    )


def export_gold_dataset_to_csv(spark_session, dataset_name, output_filename):
    """Write one Gold dataset as a single headered CSV file.

    Args:
        spark_session: Active SparkSession used to read and write the dataset.
        dataset_name: Directory name under ``GOLD_PATH``.
        output_filename: Destination filename under ``EXPORT_PATH``.

    Raises:
        FileNotFoundError: If Spark did not produce a CSV part file.
    """
    gold_dataset_path = f"{GOLD_PATH}/{dataset_name}"
    temporary_csv_directory = f"{EXPORT_PATH}/_{dataset_name}_temp"
    csv_output_path = f"{EXPORT_PATH}/{output_filename}"

    print(f"\nReading: {gold_dataset_path}")

    gold_dataset_df = (
        spark_session.read
        .format("parquet")
        .load(gold_dataset_path)
    )

    record_count = gold_dataset_df.count()

    print(f"Records: {record_count}")

    # Remove previous temporary output if it exists
    if os.path.exists(temporary_csv_directory):
        shutil.rmtree(temporary_csv_directory)

    # Remove previous final CSV if it exists
    if os.path.exists(csv_output_path):
        os.remove(csv_output_path)

    # Spark writes CSV output as a directory.
    # coalesce(1) ensures that only one CSV part file is created.
    (
        gold_dataset_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(temporary_csv_directory)
    )

    # Find the generated CSV file.
    generated_csv_path = None

    for filename in os.listdir(temporary_csv_directory):
        if filename.endswith(".csv"):
            generated_csv_path = os.path.join(temporary_csv_directory, filename)
            break

    if generated_csv_path is None:
        raise FileNotFoundError(
            f"No CSV file generated for dataset: {dataset_name}"
        )

    # Move the generated CSV to the final BI export location.
    shutil.move(generated_csv_path, csv_output_path)

    # Remove Spark temporary output directory.
    shutil.rmtree(temporary_csv_directory)

    print(f"Exported to: {csv_output_path}")


def export_gold_datasets():
    """Export all supported Gold datasets and stop the Spark session."""
    spark_session = create_spark_session()
    spark_session.sparkContext.setLogLevel("ERROR")

    print("Starting Gold to BI export...")

    os.makedirs(EXPORT_PATH, exist_ok=True)

    datasets = {
        "revenue_by_category": "revenue_by_category.csv",
        "revenue_by_city": "revenue_by_city.csv",
        "overall_metrics": "overall_metrics.csv",
        "sales_trend": "sales_trend.csv",
    }

    for dataset_name, output_filename in datasets.items():
        export_gold_dataset_to_csv(
            spark_session,
            dataset_name,
            output_filename
        )

    spark_session.stop()

    print("\nGold to BI export completed successfully.")


if __name__ == "__main__":
    export_gold_datasets()
