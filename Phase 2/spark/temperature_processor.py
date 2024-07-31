from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, expr
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("TemperatureProcessor") \
    .getOrCreate()

# Read from Kafka
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "temperature") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Define schema
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("temperature", FloatType(), True)
])

# Parse JSON
df = df.select(F.from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp to datetime
df = df.withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Calculate the metrics
monthly_df = df.withColumn("month", F.date_format(col("timestamp"), "yyyy-MM")) \
    .groupBy("month", "station_id") \
    .agg(
        avg("temperature").alias("avg_temperature"),
        (max("temperature") - min("temperature")).alias("temperature_range"),
        expr("percentile_approx(temperature, 0.5)").alias("median_temperature"),
        F.mode("temperature").alias("mode_temperature")
    )

# Write the result to HDFS
query = monthly_df \
    .writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("path", "hdfs://hdfs:9000/temperature_metrics") \
    .option("checkpointLocation", "hdfs://hdfs:9000/checkpoints/temperature_metrics") \
    .start()

query.awaitTermination()