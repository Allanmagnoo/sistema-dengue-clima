from pyspark.sql import SparkSession
import sys

print(f"Python Version: {sys.version}")

spark = SparkSession.builder \
    .appName("MinimalTest") \
    .master("local[*]") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")

# Test 1: Simple DataFrame
print("Test 1: Range")
df = spark.range(10)
df.show()

# Test 2: Simple UDF (tests Python worker)
print("Test 2: UDF")
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(returnType=IntegerType())
def double_val(x):
    return x * 2

df.withColumn("doubled", double_val("id")).show()
