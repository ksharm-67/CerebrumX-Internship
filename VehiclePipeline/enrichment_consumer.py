from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pprint import pprint
import json

consumer = KafkaConsumer('vehicle_telemetry_raw',
                         group_id = 'enrichment_consumer',
                         bootstrap_servers = ['localhost:9092'])

client = MongoClient("mongodb://localhost:27017/")

db = client["vehicle_telemetry"]
driv = db["driver_data"]

for message in consumer:
    telemetry_data = json.loads(message.value.decode('utf-8'))
    vehicle_id = telemetry_data['vehicle_id']

    pprint(driv.find_one({"vehicle_id": vehicle_id},
                         {"driver_name": 1,
                          "_id": 0}
                         ))