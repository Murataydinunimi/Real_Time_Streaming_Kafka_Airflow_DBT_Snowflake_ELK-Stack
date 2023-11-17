from time import sleep
import json
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
import time
import os
from dotenv import load_dotenv
from load_to_azure import load_to_azure
load_dotenv()
from datetime import datetime


topic = 'taxi_app'


consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',value_deserializer=lambda x: json.loads(x.decode('utf-8')))
                         
                         
trip_ids = []
message_dicts = []
threshold = 60  # Threshold for sending a batch
container_name = os.getenv("container_name")

for message in consumer:
    message_data = message.value
    
    #filter duplicated messages
    parsed_message = json.loads(message_data)
    parsed_message_unique_ids = [message['trip_id'] for message in parsed_message]
    duplicated_messages = [value for value in parsed_message_unique_ids if value in trip_ids]
    filtered_parsed_messages = [d for d in parsed_message if d['trip_id'] not in duplicated_messages]
    trip_ids.extend([ message['trip_id'] for message in filtered_parsed_messages ])
    
    message_dicts.append(filtered_parsed_messages)
    flattened_messages = [message for sublist in message_dicts for message in sublist]

    if len(flattened_messages) >= threshold:
        
        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        blob_name = "transactions.json"
        load_to_azure(flattened_messages,container_name,blob_name)
        print(f'message written with size {len(flattened_messages)} in {current_timestamp}')
        time.sleep(5)
        message_dicts = []

    