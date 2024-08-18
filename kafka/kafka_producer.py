from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import pandas as pd
import logging
import time
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_kafka_producer():
    max_retries = 5
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka-broker:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Ensure that all replicas acknowledge the message
                retries=5,  # the number of retries in case of failure
                batch_size=1048576,  # Maximum batch size in bytes (1MB)
                linger_ms=5000,  # Wait up to 5s for more messages to accumulate in the batch
            )
            return producer
        except Exception as e:
            logging.error(f"Kafka producer creation failed: {e}")
            if i < max_retries - 1:
                logging.info(f"Retrying... ({i + 1}/{max_retries})")
                time.sleep(10)  # Wait 10 seconds before retrying
            else:
                logging.error("Max retries reached. Exiting.")
                raise

def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    max_retries = 5
    for i in range(max_retries):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers="kafka-broker:9092", client_id='kafkaAdminClient')
            topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logging.info(f"Kafka topic '{topic_name}' created.")
            return

        except Exception as e:
            error_message = str(e)
            if "exist" in error_message:
                logging.info(f"Kafka topic '{topic_name}' already exists. Continuing.")
                return
            logging.error(f"Failed to create Kafka topic: {e}")
            if i < max_retries - 1:
                logging.info(f"Retrying... ({i + 1}/{max_retries})")
                time.sleep(10)  # Wait 10 seconds before retrying
            else:
                logging.error("Max retries reached. Exiting.")
                raise

def send_batch(producer, data):
    try:
        producer.send('temperature', data)
    except Exception as e:
        logging.error(f"Error sending data: {e}")

def load_and_send_data(producer, filepath):
    if not os.path.exists(filepath):
        logging.error(f"File {filepath} does not exist.")
        return False

    dataframe = pd.read_csv(filepath, delimiter=",")
    dataframe_melted = dataframe.melt(id_vars=['MESS_DATUM'], var_name='station_id', value_name='temperature').dropna()
    dataframe_melted['station_id'] = dataframe_melted['station_id'].astype(int)

    new_data_sent = False
    for _, row in dataframe_melted.iterrows():
        timestamp = row['MESS_DATUM']
        station_id = row['station_id']
        data = {
            'timestamp': timestamp,
            'station_id': station_id,
            'temperature': row['temperature']
        }

        producer.send('temperature', data)
        new_data_sent = True

    if new_data_sent:
        send_batch(producer, data)

    return new_data_sent

# Main loop to check for new data and send to Kafka
def main_loop(producer, filepath, check_interval=10):
    last_mtime = None

    while True:
        try:
            current_mtime = os.path.getmtime(filepath)
            if last_mtime is None or current_mtime > last_mtime:
                logging.info("New data detected. Sending to Kafka.")
                if load_and_send_data(producer, filepath):
                    last_mtime = current_mtime
                    logging.info("Data sent successfully.")
                else:
                    logging.info("No new data to send.")
            else:
                logging.info("No new data detected. Waiting for the next check.")
            
            time.sleep(check_interval)
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            time.sleep(check_interval)  # Wait before retrying in case of error

if __name__ == "__main__":
    # Check if the topic exists and create it if not
    create_kafka_topic('temperature')

    # Create Kafka producer
    producer = create_kafka_producer()

    # Define the path to the CSV file
    filepath = '/input/german_temperature_data_1996_2021_from_selected_weather_stations.csv'

    # Start the main loop to send data to Kafka
    main_loop(producer, filepath)