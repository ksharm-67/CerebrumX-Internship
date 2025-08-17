import json
from kafka import KafkaConsumer
from pymongo import MongoClient

#This consumer receives messages from Kafka while listening on port 9092
consumer = KafkaConsumer(
    'logs',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='mongo-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

#Defines our MongoDB Client
client = MongoClient('mongodb://localhost:27017/')
db = client['test']
collection = db['users']

#Listens to messages from Kafka and stores them in MongoDB
if __name__ == "__main__":
    try:
        for message in consumer:
            log_document = message.value
            collection.insert_one(log_document)
            print(f"Inserted into MongoDB: {log_document}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        client.close()
