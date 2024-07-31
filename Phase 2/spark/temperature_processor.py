from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min, expr, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TemperatureAggregation") \
    .getOrCreate()

logger.info("Spark session started.")

# Define schema for JSON data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("station_id", IntegerType(), True),
    StructField("temperature", FloatType(), True)
])

# Read data from Kafka
dataframe = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "temperature") \
    .load()

logger.info("Data read from Kafka.")

# Parse the JSON data
dataframe = dataframe.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

logger.info("JSON data parsed.")

# Convert timestamp to month and year
dataframe = dataframe.withColumn("year_month", expr("substring(timestamp, 1, 7)"))

# Log the schema and initial data
dataframe.printSchema()
dataframe.show(5)
logger.info("Schema and sample data logged.")

# Perform aggregations
monthly_aggregates = dataframe.groupBy("station_id", "year_month").agg(
    avg("temperature").alias("average_temperature"),
    (max("temperature") - min("temperature")).alias("temperature_range"),
    expr("percentile_approx(temperature, 0.5)").alias("median_temperature")
)

logger.info("Aggregations performed.")

# Log the schema and aggregated data
monthly_aggregates.printSchema()
monthly_aggregates.show(5)
logger.info("Schema and sample aggregated data logged.")

# Convert Spark DataFrame to Pandas DataFrame for visualization
def spark_to_pandas(spark_dataframe: DataFrame) -> pd.DataFrame:
    return spark_dataframe.toPandas()

pandas_dataframe = spark_to_pandas(monthly_aggregates)

# Visualization function
def plot_aggregations(dataframe: pd.DataFrame):
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=dataframe, x='year_month', y='average_temperature', hue='station_id', marker='o')
    plt.title('Average Temperature Per Month')
    plt.xlabel('Year-Month')
    plt.ylabel('Average Temperature')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/path/to/hdataframes/average_temperature_per_month.png')
    plt.close()

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=dataframe, x='year_month', y='temperature_range', hue='station_id', marker='o')
    plt.title('Temperature Range Per Month')
    plt.xlabel('Year-Month')
    plt.ylabel('Temperature Range')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/path/to/hdataframes/temperature_range_per_month.png')
    plt.close()

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=dataframe, x='year_month', y='median_temperature', hue='station_id', marker='o')
    plt.title('Median Temperature Per Month')
    plt.xlabel('Year-Month')
    plt.ylabel('Median Temperature')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/path/to/hdataframes/median_temperature_per_month.png')
    plt.close()

logger.info("Creating visualizations.")
plot_aggregations(pandas_dataframe)
logger.info("Visualizations created and saved to HDFS.")

# Save aggregated data to HDFS
monthly_aggregates.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("/path/to/hdataframes/temperature_aggregates")

logger.info("Data saved to HDFS.")

# Stop the Spark session
spark.stop()
logger.info("Spark session stopped.")