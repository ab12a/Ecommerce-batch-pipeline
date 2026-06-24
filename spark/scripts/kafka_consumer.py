from kafka import KafkaConsumer
import json
import os

# Create consumer
consumer = KafkaConsumer(
    "customer-events",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

output_dir = "/opt/spark/data/kafka_output"
os.makedirs(output_dir, exist_ok=True)

output_file = f"{output_dir}/events.json"

count = 0

print("Listening to Kafka...")

with open(output_file, "w") as f:
    for message in consumer:
        f.write(json.dumps(message.value))
        f.write("\n")

        count += 1
        print(f"Consumed {count} messages")

        # Stop after first 100 messages
        if count >= 100:
            break

consumer.close()

print(f"Saved {count} messages to {output_file}")