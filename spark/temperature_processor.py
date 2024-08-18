from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, percentile_approx, date_format, count, from_json, year as year_func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import logging
import time
import os
import json


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataProcessingService") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

logger.info("Spark session started.")

logger.info("Spark startup delay.")
time.sleep(60)

# Define schema for JSON data
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("station_id", IntegerType(), True),
    StructField("temperature", FloatType(), True)
])

# Offset tracking functions
OFFSET_FILE_PATH = "/output/offsets.json"

def read_offsets():
    if os.path.exists(OFFSET_FILE_PATH):
        with open(OFFSET_FILE_PATH, 'r') as f:
            offsets = json.load(f)
    else:
        offsets = {"temperature": {"0": 0}}  # Default offset if no file exists
    return offsets

def save_offsets(offsets):
    with open(OFFSET_FILE_PATH, 'w') as f:
        json.dump(offsets, f)

# Define the mode function using window functions
def get_mode(df, group_by_cols, value_col):
    mode_df = df.groupBy(group_by_cols + [value_col]).agg(count('*').alias('count'))
    window = Window.partitionBy(group_by_cols).orderBy(F.desc('count'), F.desc(value_col))
    mode_df = mode_df.withColumn('rank', F.rank().over(window)).filter(col('rank') == 1).drop('rank')
    return mode_df.select(group_by_cols + [col(value_col).alias('mode_temperature')])

# Read from Kafka with offset tracking
def read_from_kafka_with_retry(max_retries=5, retry_interval=10):
    retries = 0
    while retries < max_retries:
        try:
            offsets = read_offsets()
            df = spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka-broker:9092") \
                .option("subscribe", "temperature") \
                .option("startingOffsets", json.dumps(offsets)) \
                .load()
            
            df = df.selectExpr("CAST(value AS STRING) as json", "partition", "offset") \
                .select(from_json(col("json"), schema).alias("data"), "partition", "offset") \
                .select("data.*", "partition", "offset")

            latest_offsets = df.groupBy("partition").agg(F.max("offset").alias("max_offset")).collect()
            latest_offsets = {str(row["partition"]): row["max_offset"] + 1 for row in latest_offsets}
            return df, latest_offsets

        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            retries += 1
            logger.info(f"Retrying in {retry_interval} seconds... ({retries}/{max_retries})")
            time.sleep(retry_interval)

    raise RuntimeError("Failed to read from Kafka after multiple retries.")

# Check for data and process in batches
def process_data():
    try:
        while True:
            df, latest_offsets = read_from_kafka_with_retry()
            saved_offsets = read_offsets().get("temperature", {})

            if any(latest_offsets[partition] > saved_offsets.get(partition, 0) for partition in latest_offsets):
                logger.info("Parsing data batch from Kafka.")

                df = df.withColumn("temperature", col("temperature").cast(FloatType()))

                # Add year and month columns
                df = df.withColumn("year_month", date_format(col("timestamp"), "yyyy-MM"))
                df = df.withColumn("year", year_func(col("timestamp")))

                # Perform aggregations per month
                monthly_aggregates = df.groupBy("station_id", "year", "year_month").agg(
                    avg("temperature").alias("average_temperature"),
                    (max("temperature") - min("temperature")).alias("temperature_range"),
                    percentile_approx("temperature", 0.5).alias("median_temperature")
                )

                mode_df = get_mode(df, ["station_id", "year", "year_month"], "temperature")
                monthly_aggregates = monthly_aggregates.join(mode_df, on=["station_id", "year", "year_month"], how="left")

                # Save to HDFS as CSV
                monthly_aggregates.write.mode('append').option("header", "true").csv('hdfs://namenode:8020/tmp/hadoop-root/dfs/data/processed_data.csv')

                logger.info("Aggregated data saved to HDFS as CSV.")

                save_offsets({"temperature": latest_offsets})
                logger.info("Offsets updated.")

                counter = 0
                logger.info("Reset no new data counter.")
            else:
                counter += 1
                logger.info(f"No new data counter increased. Current: {counter}")

            logger.info("Sleeping to wait for new data.")
 
            time.sleep(10)  # Sleep to prevent continuous querying

    except Exception as e:
        logger.error(f"Error in processing data: {e}")

# Execute processing pipeline
process_data()