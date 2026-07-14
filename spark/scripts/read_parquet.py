"""
read_parquet.py

Reads the cleaned Parquet dataset and displays
basic information for verification.
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
    """Read and inspect the cleaned Parquet dataset."""

    spark = None

    try:
        logger.info("Starting Spark session...")

        spark = (
            SparkSession.builder
            .appName("ReadParquet")
            .master(SPARK_MASTER)
            .getOrCreate()
        )

        logger.info("Reading Parquet dataset from %s", PARQUET_PATH)

        df = spark.read.parquet(PARQUET_PATH)

        row_count = df.count()

        logger.info("Dataset loaded successfully.")
        logger.info("Number of rows: %d", row_count)

        logger.info("Dataset schema:")
        df.printSchema()

        logger.info("Displaying sample records:")
        df.show(10, truncate=False)

        logger.info("Parquet verification completed successfully.")

    except Exception as e:
        logger.exception("Failed to read Parquet dataset: %s", e)
        raise

    finally:
        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    main()