from faker import Faker
import json
from datetime import datetime
import time
from kafka import KafkaProducer

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')    
    )

minLat = 33.31982
minLong = -111.97182

maxLat = 33.46618
maxLong = -111.8897

def generateDriver(i: int):
    speed = fake.random_int(min=0, max=100)
    return {
        "vehicle_id": "V0" + str(i) if i < 10 else "V" + str(i),
        "timestamp": str(datetime.now()),
        "latitude": fake.pyfloat(min_value = minLat, max_value = maxLat, right_digits=6),
        "longitude": fake.pyfloat(min_value = minLong, max_value = maxLong, right_digits=6),
        "speed": speed,
        "engine": 0 if speed == 0 else 1
    }
    
try:
   while True:
       for i in range(1, 16):  
           log = generateDriver(i)
           print(log)
           producer.send(topic='vehicle_telemetry_raw', value=log)
       time.sleep(5) 
       
except KeyboardInterrupt:
   print("Producer stopped manually")
except Exception as e:
   print(f"Error: {e}")
finally:
   producer.flush()
