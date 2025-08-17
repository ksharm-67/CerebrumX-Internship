import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

#Kafka Producer 
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

levels = ["INFO", "DEBUG", "WARN", "ERROR"]
services = ["auth-service", "payment-service", "order-service", "user-service"]

#Generates a log with a random level, service, message to simulate actual kafka records
def generate_log():
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "level": random.choice(levels),
        "service": random.choice(services),
        "message": f"Simulated log event {random.randint(1000, 9999)}"
    }

#Defines the topic
topic = "logs"

#Generates logs until stopped
if __name__ == "__main__":
    try:
        while True:
            log = generate_log()
            producer.send(topic, value=log)
            print(f"Sent: {log}")
            time.sleep(2)  
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.flush()

