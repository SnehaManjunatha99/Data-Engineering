from flask import Flask, request, jsonify
from kafka import KafkaConsumer
import threading
import json

app = Flask(__name__)

# Kafka Consumer Function
def consume_kafka():
    consumer = KafkaConsumer(
        "real-time-stream",  # Kafka topic
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="flask_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    for message in consumer:
        print(f"Processing Event: {message.value}")  # Simulate AWS Lambda Processing

# Run Kafka Consumer in a Background Thread
thread = threading.Thread(target=consume_kafka)
thread.daemon = True
thread.start()

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "running"}), 200

if __name__ == "__main__":
    app.run(port=5000, debug=True)
