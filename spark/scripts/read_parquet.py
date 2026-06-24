from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquet") \
    .master("spark://spark:7077") \
    .getOrCreate()

df = spark.read.parquet(
    "/opt/spark/processed/clickstream_cleaned"
)

print("Number of rows:")
print(df.count())

print("Schema:")
df.printSchema()

print("Sample data:")
df.show(10, truncate=False)

spark.stop()