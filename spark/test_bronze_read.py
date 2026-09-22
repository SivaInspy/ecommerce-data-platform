from pyspark.sql import SparkSession


PARQUET_FILE = "/opt/spark-app/bronze/part-00000-0322a499-297c-4101-a090-090e8c38443c-c000.snappy.parquet"


spark = (
    SparkSession.builder
    .appName("TestBronzeRead")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print(f"Reading file: {PARQUET_FILE}")

df = spark.read.parquet(PARQUET_FILE)

print(f"Record count: {df.count()}")

df.printSchema()

df.show(10, truncate=False)

spark.stop()