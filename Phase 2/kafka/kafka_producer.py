from kafka import KafkaProducer
import json
import pandas as pd
import logging
import threading
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
                linger_ms=0,  # Set to 0 to ensure immediate sending
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

def send_batch(producer, messages):
    try:
        logging.info(f"Sending batch: {messages}")  # Log the contents of the batch
        for message in messages:
            producer.send('temperature', message)
        producer.flush()
        logging.info("Batch sent successfully")
    except Exception as e:
        logging.error(f"Error sending batch: {e}")

def delayed_send(producer, batch, wait_time):
    logging.info(f"Waiting for {wait_time} seconds before sending the next batch...")
    time.sleep(wait_time)
    send_batch(producer, batch)

# Read the CSV file
dataframe = pd.read_csv('/input/german_temperature_data_1996_2021_from_selected_weather_stations.csv')

# Melt the DataFrame to have station_id and temperature as separate columns
dataframe_melted = dataframe.melt(id_vars=['MESS_DATUM'], var_name='station_id', value_name='temperature')

# Convert station_id to integer
dataframe_melted['station_id'] = dataframe_melted['station_id'].astype(int)

# Sort DataFrame by station_id and timestamp
dataframe_melted.sort_values(by=['station_id', 'MESS_DATUM'], inplace=True)

# Initialize variables to manage batching
current_station = None
batch = []
wait_time = 600  # Time to wait (in seconds) before sending the next batch. 10 minutes should be good with this amount of data

# Create Kafka producer
producer = create_kafka_producer()

# Iterate over the DataFrame rows
for index, row in dataframe_melted.iterrows():
    timestamp = row['MESS_DATUM']
    station_id = row['station_id']
    
    if station_id != current_station:
        if batch:
            # Send the current batch in a separate thread and start a new batch immediately
            threading.Thread(target=delayed_send, args=(producer, batch, wait_time)).start()
            batch = []
        current_station = station_id
    
    data = {
        'timestamp': timestamp,
        'station_id': station_id,
        'temperature': row['temperature']
    }
    batch.append(data)
    

# Send any remaining messages in the batch
if batch:
    send_batch(producer, batch)

# Close the producer
producer.close()