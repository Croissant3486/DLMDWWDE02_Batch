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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TemperatureAggregation") \
    .getOrCreate()

logger.info("Spark session started.")

logger.info("Spark forced delay.")
time.sleep(20)

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


def read_from_kafka():
    # Read data from Kafka
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "temperature") \
        .load()

    # Parse the JSON data and filter out NaN values
    df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("temperature").isNotNull())

    return df

# # Function to calculate mode
# def calculate_mode(df, column):
#     mode_df = df.groupBy(column).agg(count(column).alias('count')).orderBy('count', ascending=False)
#     mode_value = mode_df.first()[0]
#     return mode_value

# Visualization function
def plot_aggregations(df: pd.DataFrame, year: str, station_id: str):
    metrics = ['average_temperature', 'temperature_range', 'median_temperature', 'mode_temperature']

    for metric in metrics:
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=df, x='year_month', y=metric, marker='o')
        plt.title(f'{metric.replace("_", " ").capitalize()} in {year} (Station: {station_id})')
        plt.xlabel('Month')
        plt.ylabel(metric.replace("_", " ").capitalize())
        plt.xticks(rotation=45)
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
        file_path = f'{output_dir}/{metric}.png'
        plt.savefig(file_path)
        logger.info(f"Saved visualization to {file_path}")
        plt.clf()  # Clear the figure for the next plot

# Check for data and process in batches
def process_data():
    try:
        while True:
            df = read_from_kafka()
            if df.count() > 0:
                logger.info("Parsing data batch from Kafka.")

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
                monthly_aggregates.show()

                # Save to HDFS as CSV
                monthly_aggregates.write.mode('append').option("header", "true").csv('hdfs://namenode:8020/tmp/hadoop-root/dfs/data/monthly')

                logger.info("Aggregated data saved to HDFS as CSV.")

                # Convert Spark DataFrame to Pandas DataFrame for visualization
                pandas_monthly_df = monthly_aggregates.toPandas()

                # Create visualizations for each year
                unique_years = pandas_monthly_df['year'].unique()
                for yr in unique_years:
                    unique_stations = pandas_monthly_df['station_id'].unique()
                    for station_id in unique_stations:
                        yearly_data = pandas_monthly_df[(pandas_monthly_df['year'] == yr) & (pandas_monthly_df['station_id'] == station_id)]
                        plot_aggregations(yearly_data, str(yr), station_id)

                logger.info("Visualizations created.")

            logger.info("Sleeping to wait for new data.")
            time.sleep(60)  # Sleep to prevent continuous querying, adjust as needed

    except Exception as e:
        logger.error(f"Error in processing data: {e}")

# Start processing data
process_data()