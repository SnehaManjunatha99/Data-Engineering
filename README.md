# Real-Time Data Pipeline

This project sets up a real-time data pipeline using Kafka, Flask, PostgreSQL, and Python.

## Prerequisites

- **Docker** (for running Kafka and Zookeeper)
- **Python 3+**
- **PostgreSQL** (for storing processed data)

## Installation

1. **Clone the repository** (if applicable)
2. **Install dependencies**:

   ```sh
   pip install -r requirements.txt
   ```

## Setup and Execution

### 1. Start Kafka and Zookeeper
If running locally, use Docker:

```sh
docker-compose up -d
```

Or start manually:

```sh
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
```

### 2. Create Kafka Topic
```sh
kafka-topics.sh --create --topic real-time-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Start the Flask Event Processor
```sh
python event_processor.py
```

### 4. Start the Kafka Producer
```sh
python kafka_producer.py
```

### 5. Start the Database Connector
```sh
python database_connector.py
```

## PostgreSQL Setup

1. Ensure PostgreSQL is running.
2. Create the `processed_data` table:

```sql
CREATE TABLE processed_data (
    id SERIAL PRIMARY KEY,
    device_id INT,
    temperature FLOAT,
    humidity FLOAT,
    timestamp TIMESTAMP
);
```

## Testing

- **Check Kafka messages:** 
  ```sh
  kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic real-time-stream --from-beginning
  ```

- **Check Flask API:**
  ```sh
  curl http://localhost:5000/health
  ```

## Troubleshooting

- If Kafka is not running, check logs:
  ```sh
  docker logs kafka
  ```
- If the database is not reachable, verify connection details in `database_connector.py`.

## Author
Developed for real-time data streaming and processing.

---

