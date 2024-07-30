from kafka import KafkaProducer
import pandas as pd
import json
from datetime import datetime

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read the CSV file
df = pd.read_csv('/input/german_temperature_data_1996_2021_from_selected_weather_stations.csv')

# Transform the dataframe to a suitable format
for index, row in df.iterrows():
    timestamp = row[0]
    for station_id, temperature in row[1:].items():
        data = {
            'timestamp': timestamp,
            'station_id': station_id,
            'temperature': temperature
        }
        print(data)
        producer.send('temperature', data)


# Ensure all messages are sent
producer.flush()