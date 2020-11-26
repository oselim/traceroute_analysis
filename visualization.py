from ctypes import Array

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("traceroute_analysis") \
    .getOrCreate()

spark_context = spark.sparkContext


from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("ip_addr", StringType()),
    StructField("asn", LongType())
])

spark.read.csv()