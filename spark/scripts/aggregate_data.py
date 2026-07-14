"""
aggregate_data.py

Reads the cleaned Parquet dataset and performs
a simple aggregation by counting the number of
records for each product brand.
"""

import logging
import os

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


def main():
    """Aggregate records by brand and display the results."""

    spark = None

    try:
        logger.info("Starting Spark session...")

        spark = (
            SparkSession.builder
            .appName("Aggregation")
            .master(SPARK_MASTER)
            .getOrCreate()
        )

        logger.info("Reading Parquet dataset from %s", PARQUET_PATH)

        df = spark.read.parquet(PARQUET_PATH)

        logger.info("Calculating brand aggregation...")

        brand_counts = (
            df.groupBy("brand")
              .count()
              .orderBy("count", ascending=False)
        )

        logger.info("Displaying top 20 brands.")

        brand_counts.show(20, truncate=False)

        logger.info("Aggregation completed successfully.")

    except Exception as e:
        logger.exception("Aggregation failed: %s", e)
        raise

    finally:
        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    main()