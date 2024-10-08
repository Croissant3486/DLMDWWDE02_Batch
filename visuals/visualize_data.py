import logging
import time
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
import pyspark.sql.functions as F

# Offset file path
OFFSET_FILE_PATH = "/output/offsets.json"
Process_Finished_Once = False

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataVisualizationService") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

logger.info("Spark session started.")

logger.info("Visualization startup delay.")
time.sleep(60)

# Function to read offsets from a file
def read_offsets():
    if os.path.exists(OFFSET_FILE_PATH):
        with open(OFFSET_FILE_PATH, 'r') as f:
            offsets = json.load(f)
    else:
        offsets = {"temperature": {"0": 0}}  # Default offset if no file exists
    return offsets

# Define schema for JSON data
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("station_id", IntegerType(), True),
    StructField("temperature", FloatType(), True)
])

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
            return latest_offsets

        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            retries += 1
            logger.info(f"Retrying in {retry_interval} seconds... ({retries}/{max_retries})")
            time.sleep(retry_interval)

    raise RuntimeError("Failed to read from Kafka after multiple retries.")

# Function to monitor Kafka for new data and ensure inactivity for a specified duration
def monitor_kafka_for_data_and_inactivity():
    counter = 0
    try: 
        while counter < 12:
            latest_offsets = read_from_kafka_with_retry()
            saved_offsets = read_offsets().get("temperature", {})

            if any(latest_offsets[partition] > saved_offsets.get(partition, 0) for partition in latest_offsets):
                counter = 0
                logger.info("new data found.")
            else:    
                if any(latest_offsets[partition] <= saved_offsets.get(partition, 0) for partition in latest_offsets) and Process_Finished_Once == True:
                    logger.info(f"No new data found and has not changed.")    
                    counter = 0
                else:
                    counter += 1
                    logger.info(f"No new data found. Current: {counter}")

            logger.info("Sleeping to wait for new data.")
    
            time.sleep(10)  # Sleep to prevent continuous querying

        return True
    except Exception as e:
        logger.error(f"Error in visualizing data: {e}")


# Visualization function with explicit color and style for each metric
def plot_aggregations(df: pd.DataFrame, year: str, station_id: str):
    metrics = {
        'average_temperature': {'label': 'Average Temperature', 'color': 'b', 'linestyle': '-'},
        'mode_temperature': {'label': 'Mode Temperature', 'color': 'r', 'linestyle': '--'},
        'median_temperature': {'label': 'Median Temperature', 'color': 'g', 'linestyle': '-.'}
    }

    # Ensure sorting by year_month before plotting
    df = df.sort_values('year_month')

    # Round the values to one decimal place
    df['average_temperature'] = df['average_temperature'].astype(float).round(1)
    df['mode_temperature'] = df['mode_temperature'].astype(float).round(1)
    df['median_temperature'] = df['median_temperature'].astype(float).round(1)

    plt.figure(figsize=(12, 8))

    for metric, properties in metrics.items():
        sns.lineplot(
            data=df,
            x='year_month',
            y=metric,
            marker='o',
            label=properties['label'],
            color=properties['color'],
            linestyle=properties['linestyle']
        )

    plt.title(f'Temperature Metrics in {year} (Station: {station_id})')
    plt.xlabel('Month')
    plt.ylabel('Temperature')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()

    output_dir = f'/output/{station_id}/{year}'
    os.makedirs(output_dir, exist_ok=True)
    file_path = f'{output_dir}/temperature_metrics.png'
    plt.savefig(file_path)
    logger.info(f"Saved visualization to {file_path}")
    plt.clf()  # Clear the figure for the next plot

# Function to create visualizations after inactivity
def create_visualizations():
    aggregated_temperature_dataframe = spark.read.format("csv").option("header", "true").load("hdfs://namenode:8020/tmp/hadoop-root/dfs/data/processed_data.csv")
    logger.info("Loaded aggregated data for visualization.")

    unique_years = aggregated_temperature_dataframe.select("year").distinct().collect()
    unique_years = [row["year"] for row in unique_years]

    for year in unique_years:
        unique_stations = aggregated_temperature_dataframe.filter(col("year") == year).select("station_id").distinct().collect()
        unique_stations = [row["station_id"] for row in unique_stations]

        for station_id in unique_stations:
            yearly_data = aggregated_temperature_dataframe.filter((col("year") == year) & (col("station_id") == station_id)).toPandas()
            plot_aggregations(yearly_data, str(year), station_id)

    logger.info("Visualizations created.")

# Main execution loop
if __name__ == "__main__":
    while True:
        logger.info("Monitoring Kafka for new data and inactivity.")
        
        # Monitor Kafka for new data followed by 120 seconds of inactivity
        if monitor_kafka_for_data_and_inactivity():
            create_visualizations()
    
        Process_Finished_Once = True
        logger.info(f'"Process_Finished_Once: " {Process_Finished_Once}')
        time.sleep(1200)
        logger.info("Restarting monitoring process.")
