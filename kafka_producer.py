from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "real-time-stream"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Function to Generate Simulated IoT Data
def generate_data():
    return {
        "device_id": random.randint(1000, 9999),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

# Continuous Data Streaming
if __name__ == "__main__":
    print(f"Starting Kafka Producer. Sending data to topic: {KAFKA_TOPIC}")
    
    try:
        while True:
            data = generate_data()
            producer.send(KAFKA_TOPIC, data)
            print(f"Sent: {data}")
            time.sleep(2)  # Simulating real-time data every 2 seconds

    except KeyboardInterrupt:
        print("\nKafka Producer Stopped.")
    finally:
        producer.close()
