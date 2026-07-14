"""
kafka_consumer.py

Consumes messages from an Apache Kafka topic and writes
them to a JSON file for downstream processing.
"""

import json
import logging
import os

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

KAFKA_OUTPUT_PATH = os.getenv(
    "KAFKA_OUTPUT_PATH",
    "/opt/spark/data/kafka_output/events.json"
)

MESSAGE_LIMIT = 100


def main():
    """Consume Kafka messages and save them to a JSON file."""

    consumer = None

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
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        os.makedirs(
            os.path.dirname(KAFKA_OUTPUT_PATH),
            exist_ok=True
        )

        logger.info("Writing messages to %s", KAFKA_OUTPUT_PATH)

        count = 0

        with open(KAFKA_OUTPUT_PATH, "w", encoding="utf-8") as file:

            logger.info("Listening to Kafka topic '%s'...", KAFKA_TOPIC)

            for message in consumer:
                file.write(json.dumps(message.value))
                file.write("\n")

                count += 1

                logger.info(
                    "Consumed %d/%d messages",
                    count,
                    MESSAGE_LIMIT
                )

                if count >= MESSAGE_LIMIT:
                    logger.info("Message limit reached.")
                    break

        logger.info(
            "Successfully saved %d messages to %s",
            count,
            KAFKA_OUTPUT_PATH
        )

    except Exception as e:
        logger.exception("Kafka consumer failed: %s", e)
        raise

    finally:
        if consumer:
            logger.info("Closing Kafka consumer.")
            consumer.close()


if __name__ == "__main__":
    main()