import json
from kafka import KafkaConsumer

#Creates a consumer to receive the produced messages
consumer = KafkaConsumer(
    'logs',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='log-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

#Reads the json files until stopped
if __name__ == "__main__":
    try:
        print("Listening for messages on topic 'logs'...")
        for message in consumer:
            log = message.value
            print(f"Received: {log}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

