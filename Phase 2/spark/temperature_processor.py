import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min, expr, from_json, percentile_approx, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

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
    StructField("timestamp", StringType(), True),
    StructField("station_id", IntegerType(), True),
    StructField("temperature", FloatType(), True)
])

def read_from_kafka():

    # Read data from Kafka
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "temperature") \
        .load()

    logger.info("Data read from Kafka.")

    # Parse the JSON data
    df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    logger.info("JSON data parsed.")

    # Convert timestamp to month and year
    df = df.withColumn("year_month", expr("substring(timestamp, 1, 7)"))
    df = df.withColumn("year", expr("substring(timestamp, 1, 4)"))

    return df
        

# Function to calculate mode
def calculate_mode(df, column):
    mode_df = df.groupBy(column).agg(count(column).alias('count')).orderBy('count', ascending=False)
    mode_value = mode_df.first()[0]
    return mode_value

# Visualization function
def plot_aggregations(df: pd.DataFrame, time_unit: str):
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x=time_unit, y='average_temperature', hue='station_id', marker='o')
    plt.title(f'Average Temperature Per {time_unit.capitalize()}')
    plt.xlabel(time_unit.capitalize())
    plt.ylabel('Average Temperature')
    plt.xticks(rotation=45)
    plt.tight_layout()
    file_path = f'/hadoop/dfs/temperatureData/average_temperature_per_{time_unit}.png'
    plt.savefig(file_path)
    logger.info(f"Saved visualization to {file_path}")

# Check for data and process in batches
def process_data():
    try:
        while True:
            df = read_from_kafka()
            if df.count() > 0:
                # Log the schema and initial data
                df.printSchema()
                df.show(5)
                logger.info("Schema and sample data logged.")

                # Perform aggregations per month
                monthly_aggregates = df.groupBy("station_id", "year_month").agg(
                    avg("temperature").alias("average_temperature"),
                    (max("temperature") - min("temperature")).alias("temperature_range"),
                    percentile_approx("temperature", 0.5).alias("median_temperature")
                )

                logger.info(monthly_aggregates)

                # # Calculate mode per month
                # mode_per_month = df.groupBy("station_id", "year_month").agg(
                #     expr('first(temperature) as temperature')  # Placeholder for the mode
                # ).select("station_id", "year_month", "temperature")

                # # Apply mode calculation
                # mode_per_month = mode_per_month.rdd.map(lambda row: (row.station_id, row.year_month, calculate_mode(df.filter((col("station_id") == row.station_id) & (col("year_month") == row.year_month)), "temperature"))).toDF(["station_id", "year_month", "mode_temperature"])

                # logger.info("Monthly aggregations performed.")

                # # Perform aggregations per year
                # yearly_aggregates = df.groupBy("station_id", "year").agg(
                #     avg("temperature").alias("average_temperature"),
                #     (max("temperature") - min("temperature")).alias("temperature_range"),
                #     percentile_approx("temperature", 0.5).alias("median_temperature")
                # )

                # # Calculate mode per year
                # mode_per_year = df.groupBy("station_id", "year").agg(
                #     expr('first(temperature) as temperature')  # Placeholder for the mode
                # ).select("station_id", "year", "temperature")

                # # Apply mode calculation
                # mode_per_year = mode_per_year.rdd.map(lambda row: (row.station_id, row.year, calculate_mode(df.filter((col("station_id") == row.station_id) & (col("year") == row.year)), "temperature"))).toDF(["station_id", "year", "mode_temperature"])

                # logger.info("Yearly aggregations performed.")

                # Save to HDFS
                monthly_aggregates.write.mode('append').parquet('/hadoop/dfs/temperatureData/monthly')
                # mode_per_month.write.mode('append').parquet('/hadoop/dfs/temperatureData/monthly_mode')
                # yearly_aggregates.write.mode('append').parquet('/hadoop/dfs/temperatureData/yearly')
                # mode_per_year.write.mode('append').parquet('/hadoop/dfs/temperatureData/yearly_mode')

                logger.info("Aggregated data saved to HDFS.")

                # Convert Spark DataFrame to Pandas DataFrame for visualization
                pandas_monthly_df = monthly_aggregates.toPandas()
                # pandas_yearly_df = yearly_aggregates.toPandas()



                # Create visualizations
                logger.info("Creating visualizations.")
                plot_aggregations(pandas_monthly_df, 'year_month')
                # plot_aggregations(pandas_yearly_df, 'year')

                logger.info("Visualizations created.")

            logger.info("Sleep to wait for new data.")
            time.sleep(60)  # Sleep to prevent continuous querying, adjust as needed

    except Exception as e:
        logger.error(f"Error in processing data: {e}")

# Start processing data
process_data()
