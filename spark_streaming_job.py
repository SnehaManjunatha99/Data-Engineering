from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder     .appName("KafkaSparkStreaming")     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.2.18")     .getOrCreate()

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "real-time-stream"

# Define Schema for Incoming Data
schema = StructType([
    StructField("device_id", IntegerType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# Read Stream from Kafka
raw_stream = spark.readStream     .format("kafka")     .option("kafka.bootstrap.servers", KAFKA_BROKER)     .option("subscribe", KAFKA_TOPIC)     .option("startingOffsets", "latest")     .load()

# Parse JSON Data
data_stream = raw_stream.selectExpr("CAST(value AS STRING)")     .select(from_json(col("value"), schema).alias("data"))     .select("data.*")

# Convert timestamp column to proper format
data_stream = data_stream.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Perform Aggregation (Example: Average Temperature & Humidity per 10-minute Window)
aggregated_stream = data_stream     .groupBy(window(col("timestamp"), "10 minutes"), col("device_id"))     .agg(avg("temperature").alias("avg_temperature"), avg("humidity").alias("avg_humidity"))     .select(
        col("device_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("avg_humidity")
    )

# PostgreSQL Configuration
DB_URL = "jdbc:postgresql://localhost:5432/your_database"
DB_PROPERTIES = {
    "user": "your_username",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

# Write Stream to PostgreSQL
query = aggregated_stream.writeStream     .outputMode("append")     .foreachBatch(lambda df, epoch_id: df.write.jdbc(url=DB_URL, table="processed_data", mode="append", properties=DB_PROPERTIES))     .start()

query.awaitTermination()
