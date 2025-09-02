from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pprint import pprint
import json

consumer = KafkaConsumer('vehicle_telemetry_raw',
                         group_id = 'enrichment_consumer',
                         bootstrap_servers = ['localhost:9092'])

enriched_producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

client = MongoClient("mongodb://localhost:27017/")

db = client["vehicle_telemetry"]
driv = db["driver_data"]
fences = db["geofences"]

for message in consumer:
    telemetry_data = json.loads(message.value.decode('utf-8'))
    
    vehicle_id = telemetry_data['vehicle_id']
    long = telemetry_data['longitude']
    lat = telemetry_data['latitude']

    dname = (driv.find_one({"vehicle_id": vehicle_id},
                         ))['driver_name']
    
    geodata = list(fences.find({"polygon": {
                                    "$geoIntersects": {
                                                     "$geometry": {
                                                         "type": "Point",
                                                         "coordinates": [long, lat]
                                                         }
                                                     }
                                    }
                            },
                            {"zone_name": 1,
                             "description": 1
                             }
                             ))
    
    try:
        dat = {**telemetry_data, "driver_name" : dname, 'geo_data': geodata if geodata else None}
                                                                      
        enriched_producer.send(topic='vehicle_events_enriched', value=dat)

        pprint(dat)
    except Exception as e:
        print(f"Error: {e}")
                                    