from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("EcommercePipeline") \
    .master("spark://spark:7077") \
    .getOrCreate()

print("Reading dataset...")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/opt/spark/raw/2019-Oct.csv.gz")

print("Dataset loaded!")

print("Schema:")
df.printSchema()

# Small sample for development
sample_df = df.limit(10000)

clean_df = (
    sample_df
    .dropDuplicates()
    .filter(col("event_time").isNotNull())
    .filter(col("event_type").isNotNull())
    .filter(col("product_id").isNotNull())
)

print("Sample rows:")
clean_df.show(5, truncate=False)

print("Writing parquet...")

clean_df.write \
    .mode("overwrite") \
    .parquet("/opt/spark/processed/clickstream_cleaned")

print("Finished!")

spark.stop()