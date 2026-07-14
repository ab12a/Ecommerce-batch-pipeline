"""
read_data.py

Reads the raw e-commerce dataset, performs basic preprocessing,
and writes the cleaned dataset to Parquet format for downstream
processing.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

RAW_DATA_PATH = os.getenv(
    "RAW_DATA_PATH",
    "/opt/spark/raw/2019-Oct.csv.gz"
)

PARQUET_PATH = os.getenv(
    "PARQUET_PATH",
    "/opt/spark/processed/clickstream_cleaned"
)


def main():
    """Run the Spark ETL pipeline."""

    spark = None

    try:
        logger.info("Starting Spark session...")

        spark = (
            SparkSession.builder
            .appName("EcommercePipeline")
            .master(SPARK_MASTER)
            .getOrCreate()
        )

        logger.info("Reading dataset from %s", RAW_DATA_PATH)

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(RAW_DATA_PATH)
        )

        logger.info("Dataset loaded successfully.")

        logger.info("Dataset schema:")
        df.printSchema()

        logger.info("Limiting dataset to 10,000 records for development.")

        sample_df = df.limit(10000)

        logger.info("Cleaning dataset...")

        clean_df = (
            sample_df
            .dropDuplicates()
            .filter(col("event_time").isNotNull())
            .filter(col("event_type").isNotNull())
            .filter(col("product_id").isNotNull())
        )

        logger.info("Displaying sample records:")
        clean_df.show(5, truncate=False)

        logger.info("Writing cleaned dataset to Parquet...")

        (
            clean_df.write
            .mode("overwrite")
            .parquet(PARQUET_PATH)
        )

        logger.info("ETL pipeline completed successfully.")

    except Exception as e:
        logger.exception("ETL pipeline failed: %s", e)
        raise

    finally:
        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    main()