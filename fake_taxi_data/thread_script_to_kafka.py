from time import sleep
import json
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
import threading
import time
import requests


#channel
topic='taxi_app'

# producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def get_data():

    data_response = requests.get('http://localhost:8081/fake_taxi_data')
    return data_response.text

#worker --> fetching app data and sends to kafka
def fetch_data_and_send_to_kafka():
    while True:
        # Make a GET request to localhost8081 and fetch data
        data = get_data()
        time.sleep(6)  # Wait for 6 second before the next iteration

        producer.send('taxi_app',value=data)
    
    
fetch_thread = threading.Thread(target=fetch_data_and_send_to_kafka)
fetch_thread.start()

if __name__ == '__main__':
    fetch_data_and_send_to_kafka()