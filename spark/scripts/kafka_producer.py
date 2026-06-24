from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaProducer") \
    .master("spark://spark:7077") \
    .getOrCreate()

print("Reading Parquet files...")

df = spark.read.parquet(
    "/opt/spark/processed/clickstream_cleaned"
)

print(f"Rows in parquet: {df.count()}")

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Send only first 100 rows for testing
rows = df.limit(100).toJSON().collect()

for row in rows:
    producer.send(
        "customer-events",
        value=json.loads(row)
    )

producer.flush()
producer.close()

print(f"Sent {len(rows)} messages to Kafka.")

spark.stop()