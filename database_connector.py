from kafka import KafkaConsumer
import psycopg2
import json

# Database Configuration
DB_HOST = "localhost"  # Change this to your database host
DB_PORT = "5432"
DB_NAME = "your_database"
DB_USER = "your_username"
DB_PASSWORD = "your_password"

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "real-time-stream"

# Connect to PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Insert Data into PostgreSQL
def insert_data(cursor, data):
    query = """INSERT INTO processed_data (device_id, temperature, humidity, timestamp)
                VALUES (%s, %s, %s, %s)"""
    values = (data["device_id"], data["temperature"], data["humidity"], data["timestamp"])
    cursor.execute(query, values)

# Kafka Consumer to Database
def consume_and_store():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="database_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    conn = connect_db()
    if conn is None:
        return

    cursor = conn.cursor()

    print("Listening for processed data...")
    try:
        for message in consumer:
            data = message.value
            print(f"Inserting data into database: {data}")
            insert_data(cursor, data)
            conn.commit()
    except Exception as e:
        print(f"Error processing message: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    consume_and_store()
