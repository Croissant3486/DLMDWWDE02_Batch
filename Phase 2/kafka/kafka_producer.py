from kafka import KafkaProducer
import json
import pandas as pd
import logging

# Set up logging
#logging.basicConfig(level=logging.DEBUG)

# Initialize Kafka producer with batch configuration
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    #batch_size=26214400,  # Batch size in bytes (25 MB)
    linger_ms=60000,     # Wait time before sending the batch
    #buffer_memory=33554432  # Buffer memory in bytes (32 MB)
)

def send_batch(messages):
    try:
        for message in messages:
            producer.send('temperature', message)
        producer.flush()
        logging.info("Batch sent successfully")
    except Exception as e:
        logging.error(f"Error sending batch: {e}")

# Read the CSV file
df = pd.read_csv('/input/german_temperature_data_1996_2021_from_selected_weather_stations.csv')

# Accumulate messages in a batch
batch = []
batch_size_limit = 25000  # Adjust batch size limit based on requirements

for index, row in df.iterrows():
    timestamp = row[0]
    for station_id, temperature in row[1:].items():
        data = {
            'timestamp': timestamp,
            'station_id': station_id,
            'temperature': temperature
        }
        batch.append(data)
        
        if len(batch) >= batch_size_limit:
            send_batch(batch)
            batch = []

# Send any remaining messages in the batch
if batch:
    send_batch(batch)

# Close the producer
producer.close()