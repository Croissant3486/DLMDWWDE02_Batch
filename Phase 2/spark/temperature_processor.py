from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, percentile_approx, date_format, count, from_json, year as year_func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import logging
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TemperatureAggregation") \
    .getOrCreate()

logger.info("Spark session started.")

logger.info("Spark forced delay.")
time.sleep(30)

# Define schema for JSON data
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("station_id", IntegerType(), True),
    StructField("temperature", FloatType(), True)
])

# Define the mode function using window functions
def get_mode(df, group_by_cols, value_col):
    mode_df = df.groupBy(group_by_cols + [value_col]).agg(count('*').alias('count'))
    window = Window.partitionBy(group_by_cols).orderBy(F.desc('count'), F.desc(value_col))
    mode_df = mode_df.withColumn('rank', F.rank().over(window)).filter(col('rank') == 1).drop('rank')
    return mode_df.select(group_by_cols + [col(value_col).alias('mode_temperature')])

# Offset tracking functions
OFFSET_FILE_PATH = "/tmp/offsets.json"

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

# Read from Kafka with offset tracking
def read_from_kafka():
    offsets = read_offsets()
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "temperature") \
        .option("startingOffsets", json.dumps(offsets)) \
        .load()
    
    df = df.selectExpr("CAST(value AS STRING) as json", "partition", "offset") \
        .select(from_json(col("json"), schema).alias("data"), "partition", "offset") \
        .select("data.*", "partition", "offset")
    
    latest_offsets = df.groupBy("partition").agg(F.max("offset").alias("max_offset")).collect()
    latest_offsets = {str(row["partition"]): row["max_offset"] + 1 for row in latest_offsets}

    return df, latest_offsets

# Check for data and process in batches
def process_data():
    try:
        counter = 0
        while counter < 6:
            df, latest_offsets = read_from_kafka()
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

                logger.info("Monthly aggregates:")

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
            time.sleep(10)  # Sleep to prevent continuous querying, adjust as needed
            
        create_visualizations()
    except Exception as e:
        logger.error(f"Error in processing data: {e}")

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
    plt.ylabel('Value')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()

    #Lösung zum Speichern von Daten in HDFS
    #file_path = f'/tmp/average_temperature_per_{time_unit}.png'
    #plt.savefig(file_path)
    #img_df = spark.read.format("image").load(file_path)
    #proc_df = img_df.select(base64(col("image.data")).alias('encoded'))
    #proc_df.coalesce(1).write.mode('overwrite').format("text").save('hdfs://namenode:8020/tmp/hadoop-root/dfs/data/visuals'

    #Da impraktikabel wird das Image zurück auf die local disk, in das output verzeichnis, geschrieben.
    output_dir = f'/output/{station_id}/{year}'
    os.makedirs(output_dir, exist_ok=True)
    file_path = f'{output_dir}/temperature_metrics.png'
    plt.savefig(file_path)
    logger.info(f"Saved visualization to {file_path}")
    plt.clf()  # Clear the figure for the next plot

# Convert Spark DataFrame to Pandas DataFrame for visualization
def create_visualizations():
    aggregated_temperature_dataframe = spark.read.format("csv").option("header", "true").load("hdfs://namenode:8020/tmp/hadoop-root/dfs/data/processed_data.csv")
    unique_years = aggregated_temperature_dataframe.select("year").distinct().collect()
    unique_years = [row["year"] for row in unique_years]

    for year in unique_years:
        unique_stations = aggregated_temperature_dataframe.filter(col("year") == year).select("station_id").distinct().collect()
        unique_stations = [row["station_id"] for row in unique_stations]
        
        for station_id in unique_stations:
            yearly_data = aggregated_temperature_dataframe.filter((col("year") == year) & (col("station_id") == station_id)).toPandas()
            plot_aggregations(yearly_data, str(year), station_id)

    logger.info("Visualizations created.")
    spark.stop()

process_data()