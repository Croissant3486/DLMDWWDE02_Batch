from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import pandas as pd
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_kafka_producer():
    max_retries = 5
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all', # Ensure that all replicas acknowledge the message
                retries=5,  # the number of retries in case of failure
                batch_size= 1048576, # Maximum batch size in bytes (1mb)
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
            admin_client = KafkaAdminClient(bootstrap_servers="kafka:9092", client_id='kafkaAdminClient')
            topic_list = []
            topic_list.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))
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

# Check if the topic exists and create if not
create_kafka_topic('temperature')

# Create Kafka producer
producer = create_kafka_producer()

dataframe = pd.read_csv('/input/german_temperature_data_1996_2021_from_selected_weather_stations.csv', delimiter=",")

dataframe_melted = dataframe.melt(id_vars=['MESS_DATUM'], var_name='station_id', value_name='temperature').dropna()

dataframe_melted['station_id'] = dataframe_melted['station_id'].astype(int)

for index, row in dataframe_melted.iterrows():
    timestamp = row['MESS_DATUM']
    station_id = row['station_id']
    data = {
        'timestamp': timestamp,
        'station_id': station_id,
        'temperature': row['temperature']
    }

    producer.send('temperature', data)

if data:
     send_batch(producer, data)

# Close the producer
producer.close()

# for chunk in pd.read_csv('/input/german_temperature_data_1996_2021_from_selected_weather_stations.csv', delimiter=",", chunksize=batch_size):

#     # Melt the DataFrame to have station_id and temperature as separate columns also drop NaN values
#     chunk_melted = chunk.melt(id_vars=['MESS_DATUM'], var_name='station_id', value_name='temperature')

#     chunk_melted = chunk_melted.dropna()

#     # Convert station_id to integer
#     chunk_melted['station_id'] = chunk_melted['station_id'].astype(int)

#     # Sort DataFrame by timestamp (no need to sort by station_id now)
#     #dataframe_melted.sort_values(by=['MESS_DATUM'], inplace=True)

#     # Initialize variables to manage batching
#     batch = []

#     # Iterate over the DataFrame rows
#     for index, row in chunk_melted.iterrows():
#         timestamp = row['MESS_DATUM']
#         station_id = row['station_id']

#         data = {
#             'timestamp': timestamp,
#             'station_id': station_id,
#             'temperature': row['temperature']
#         }
#         batch.append(data)

#         if len(batch) >= batch_size:
#             send_batch(producer, batch)
#             batch = []

# Send any remaining messages in the batch
# if batch:
    # send_batch(producer, batch)
# 
#executor.shutdown(wait=True)
