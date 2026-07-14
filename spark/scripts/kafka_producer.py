"""
kafka_producer.py

Reads the cleaned Parquet dataset and publishes
records to an Apache Kafka topic.
"""

import json
import logging
import os

from kafka import KafkaProducer
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "spark://spark:7077"
)

PARQUET_PATH = os.getenv(
    "PARQUET_PATH",
    "/opt/spark/processed/clickstream_cleaned"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "customer-events"
)

MESSAGE_LIMIT = 100


def main():
    """Read Parquet data and publish records to Kafka."""

    spark = None
    producer = None

    try:
        logger.info("Starting Spark session...")

        spark = (
            SparkSession.builder
            .appName("KafkaProducer")
            .master(SPARK_MASTER)
            .getOrCreate()
        )

        logger.info("Reading Parquet dataset from %s", PARQUET_PATH)

        df = spark.read.parquet(PARQUET_PATH)

        logger.info("Rows available: %d", df.count())

        logger.info(
            "Connecting to Kafka broker: %s",
            KAFKA_BOOTSTRAP_SERVERS
        )

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        logger.info(
            "Publishing first %d records to topic '%s'...",
            MESSAGE_LIMIT,
            KAFKA_TOPIC
        )

        rows = df.limit(MESSAGE_LIMIT).toJSON().collect()

        for row in rows:
            producer.send(
                KAFKA_TOPIC,
                value=json.loads(row)
            )

        producer.flush()

        logger.info(
            "Successfully published %d messages to Kafka.",
            len(rows)
        )

    except Exception as e:
        logger.exception("Kafka producer failed: %s", e)
        raise

    finally:
        if producer:
            logger.info("Closing Kafka producer.")
            producer.close()

        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    main()