from kafka import KafkaConsumer
import psycopg2
import json

consumer = KafkaConsumer(
    "customer-events",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    consumer_timeout_ms=10000,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

conn = psycopg2.connect(
    host="postgres",
    database="ecommerce",
    user="admin",
    password="admin123"
)

cursor = conn.cursor()

count = 0

print("Listening to Kafka...")

for message in consumer:
    event = message.value

    cursor.execute("""
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
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        event.get("event_time"),
        event.get("event_type"),
        event.get("product_id"),
        event.get("category_id"),
        event.get("category_code"),
        event.get("brand"),
        event.get("price"),
        event.get("user_id"),
        event.get("user_session")
    ))

    count += 1

conn.commit()
cursor.close()
conn.close()

print(f"Inserted {count} rows into PostgreSQL.")