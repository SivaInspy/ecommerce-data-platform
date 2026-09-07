from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType


order_schema = StructType([

    StructField("order_id", IntegerType()),

    StructField("customer_id", IntegerType()),

    StructField("customer_name", StringType()),

    StructField("city", StringType()),

    StructField("product_id", StringType()),

    StructField("product_name", StringType()),

    StructField("category", StringType()),

    StructField("price", IntegerType()),

    StructField("payment_method", StringType()),

    StructField("order_time", StringType())

])