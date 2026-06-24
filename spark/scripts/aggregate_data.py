from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Aggregation")
    .master("spark://spark:7077")
    .getOrCreate()
)

df = spark.read.parquet(
    "/opt/spark/processed/clickstream_cleaned"
)

brand_counts = (
    df.groupBy("brand")
      .count()
      .orderBy("count", ascending=False)
)

brand_counts.show(20)

spark.stop()