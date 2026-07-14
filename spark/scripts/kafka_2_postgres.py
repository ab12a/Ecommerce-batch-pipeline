"""
kafka_2_postgres.py

Consumes messages from an Apache Kafka topic and stores
them in a PostgreSQL database.
"""

import json
import logging
import os

import psycopg2
from kafka import KafkaConsumer

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
KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "customer-events"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "postgres"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432"
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "ecommerce"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "admin"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "admin123"
)


def main():
    """Consume Kafka messages and insert them into PostgreSQL."""

    consumer = None
    conn = None
    cursor = None

    try:
        logger.info(
            "Connecting to Kafka broker: %s",
            KAFKA_BOOTSTRAP_SERVERS
        )

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=10000,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

        logger.info("Connecting to PostgreSQL database...")

        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

        cursor = conn.cursor()

        logger.info("Listening to Kafka topic '%s'...", KAFKA_TOPIC)

        count = 0

        for message in consumer:
            event = message.value

            cursor.execute(
                """
                INSERT INTO customer_events (
                    event_time,
                    event_type,
                    product_id,
                    category_id,
                    category_code,
                    brand,
                    price,
                    user_id,
                    user_session
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event.get("event_time"),
                    event.get("event_type"),
                    event.get("product_id"),
                    event.get("category_id"),
                    event.get("category_code"),
                    event.get("brand"),
                    event.get("price"),
                    event.get("user_id"),
                    event.get("user_session"),
                ),
            )

            count += 1

        conn.commit()

        logger.info(
            "Successfully inserted %d rows into PostgreSQL.",
            count
        )

    except Exception as e:
        if conn:
            conn.rollback()

        logger.exception("Failed to insert data into PostgreSQL: %s", e)
        raise

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()

        if consumer:
            consumer.close()

        logger.info("Resources closed successfully.")


if __name__ == "__main__":
    main()