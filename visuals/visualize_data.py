import logging
import time
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from confluent_kafka import Consumer, KafkaError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_TOPIC = "temperature"
KAFKA_CONF = {
    'bootstrap.servers': 'kafka-broker:9092',  
    'group.id': 'temperature-consumer-group',
    'auto.offset.reset': 'latest'
}
TIMEOUT_DURATION = 120  # 120 seconds

logger.info("Visualization Service startup delay.")
time.sleep(60)


# Function to monitor Kafka for new data and ensure inactivity for 120 seconds
def monitor_kafka_for_data_and_inactivity():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([KAFKA_TOPIC])
    
    new_data_detected = False
    inactivity_timer_started = False
    inactivity_start_time = None
    
    try:
        while True:
            msg = consumer.poll(timeout=5000)  # Poll for messages with a 5-second timeout
            
            if msg is None:  # No message received in this poll
                if new_data_detected:
                    # If new data was detected earlier, start checking for inactivity
                    if not inactivity_timer_started:
                        inactivity_timer_started = True
                        inactivity_start_time = time.time()
                        logger.info("No new data. Starting inactivity timer.")
                    elif time.time() - inactivity_start_time >= TIMEOUT_DURATION:
                        # 120 seconds of inactivity after new data was detected
                        logger.info("120 seconds of inactivity detected. Triggering visualization.")
                        return True  # Trigger visualization process
                continue
            
            if not msg.error():
                logger.info("New data received on Kafka topic.")
                new_data_detected = True
                inactivity_timer_started = False  # Reset inactivity check
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                continue  # Reached end of partition; no new messages
            else:
                logger.error(f"Error: {msg.error()}")
                break  # Break on error
    finally:
        consumer.close()
    
    return False

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
    # Load the data from CSV directly to Pandas
    aggregated_temperature_dataframe = pd.read_csv("hdfs://namenode:8020/tmp/hadoop-root/dfs/data/processed_data.csv")
    logger.info("Loaded aggregated data for visualization.")

    unique_years = aggregated_temperature_dataframe['year'].unique()

    for year in unique_years:
        yearly_data = aggregated_temperature_dataframe[aggregated_temperature_dataframe['year'] == year]
        unique_stations = yearly_data['station_id'].unique()

        for station_id in unique_stations:
            station_data = yearly_data[yearly_data['station_id'] == station_id]
            plot_aggregations(station_data, str(year), station_id)

    logger.info("Visualizations created.")

# Main execution loop
if __name__ == "__main__":
    while True:
        logger.info("Monitoring Kafka for new data and inactivity.")
        
        # Monitor Kafka for new data followed by 120 seconds of inactivity
        if monitor_kafka_for_data_and_inactivity():
            create_visualizations()
        
        logger.info("Restarting monitoring process.")
