import csv
import pandas as pd
from kafka import KafkaProducer
import json
from time import sleep


def read_csv(file_path):
    df = pd.read_csv(file_path)
    return df.to_dict(orient='records')


# Function to send data to Kafka topic
def send_to_kafka(data, topic_name, kafka_server):
    producer = KafkaProducer(bootstrap_servers=kafka_server,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for record in data:
        producer.send(topic_name, record)
        producer.flush()  # Ensures each message is sent
        sleep(0.1)
    producer.close()

if __name__ == '__main__':
    csv_file_path = r"D:\data\final_iphone14_tweets_30ksamples.csv"
    kafka_server = 'localhost:9092'
    topic_name = 'tweets-iphone14'
    csv_data = read_csv(csv_file_path)
    send_to_kafka(csv_data, topic_name, kafka_server)
