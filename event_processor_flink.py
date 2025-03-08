from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.table import StreamTableEnvironment
from pyflink.table.descriptors import Schema, Json, Kafka, FileSystem
from pyflink.table.udf import udf

import json

# Initialize Flink Environment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# Define Kafka Source
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("real-time-stream") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

# Define Schema for Incoming Data
schema = Schema() \
    .field("device_id", "INT") \
    .field("temperature", "FLOAT") \
    .field("humidity", "FLOAT") \
    .field("timestamp", "TIMESTAMP(3)")

# Register Kafka Source as Table
table_env.connect(Kafka()
    .version("universal")
    .topic("real-time-stream")
    .start_from_earliest()
    .property("bootstrap.servers", "localhost:9092")
) \
    .with_format(Json().fail_on_missing_field(True)) \
    .with_schema(schema) \
    .create_temporary_table("KafkaTable")

# Query to Perform Aggregation and Anomaly Detection (10-minute window)
table_env.execute_sql("""
    CREATE TEMPORARY VIEW ProcessedData AS
    SELECT 
        device_id,
        TUMBLE_START(timestamp, INTERVAL '10' MINUTE) AS window_start,
        TUMBLE_END(timestamp, INTERVAL '10' MINUTE) AS window_end,
        AVG(temperature) AS avg_temperature,
        AVG(humidity) AS avg_humidity,
        MAX(temperature) - MIN(temperature) AS temp_fluctuation,
        CASE 
            WHEN MAX(temperature) - MIN(temperature) > 10 THEN 'Rapid Temp Fluctuation'
            WHEN MAX(temperature) > 80 THEN 'Overheating'
            WHEN MIN(humidity) < 30 THEN 'Dry Environment'
            ELSE 'Normal'
        END AS anomaly_flag
    FROM KafkaTable
    GROUP BY device_id, TUMBLE(timestamp, INTERVAL '10' MINUTE)
""")

# Detect Missing Data Points
table_env.execute_sql("""
    CREATE TEMPORARY VIEW MissingData AS
    SELECT 
        device_id,
        COUNT(*) AS data_count,
        CASE 
            WHEN COUNT(*) = 0 THEN 'Device Inactive (>10 min)'
            ELSE 'Active'
        END AS status
    FROM KafkaTable
    GROUP BY device_id, TUMBLE(timestamp, INTERVAL '10' MINUTE)
""")

# PostgreSQL Sink for Processed Data
table_env.execute_sql("""
    CREATE TEMPORARY TABLE PostgresSink (
        device_id INT,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        avg_temperature FLOAT,
        avg_humidity FLOAT,
        temp_fluctuation FLOAT,
        anomaly_flag STRING
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/your_database',
        'table-name' = 'processed_data',
        'username' = 'your_username',
        'password' = 'your_password',
        'driver' = 'org.postgresql.Driver'
    )
""")

# PostgreSQL Sink for Missing Data
table_env.execute_sql("""
    CREATE TEMPORARY TABLE PostgresMissingDataSink (
        device_id INT,
        data_count INT,
        status STRING
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/your_database',
        'table-name' = 'missing_data',
        'username' = 'your_username',
        'password' = 'your_password',
        'driver' = 'org.postgresql.Driver'
    )
""")

# Insert Processed Data into PostgreSQL
table_env.execute_sql("""
    INSERT INTO PostgresSink
    SELECT * FROM ProcessedData
""")

# Insert Missing Data Alerts into PostgreSQL
table_env.execute_sql("""
    INSERT INTO PostgresMissingDataSink
    SELECT * FROM MissingData
""")

# Execute Job
env.execute("Kafka Flink Streaming Job with Anomaly Detection")
