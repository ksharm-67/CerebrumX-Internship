from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pprint import pprint
import json

#Create a consumer to get data from the raw data topic
consumer = KafkaConsumer('vehicle_telemetry_raw',
                         group_id = 'enrichment_consumer',
                         bootstrap_servers = ['localhost:9092'])

#Create another producer to send the enriched data to vehicle_events_enriched topic
enriched_producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

#Create a client to connect to Mongo at port 27017
client = MongoClient("mongodb://localhost:27017/")

#Use the database vehicle_telemetry in Mongo
db = client["vehicle_telemetry"]

#Use the collections driver_data and geofences
driv = db["driver_data"]
fences = db["geofences"]

for message in consumer:
    #Load a single json object into telemetry_data
    telemetry_data = json.loads(message.value.decode('utf-8'))
    
    #Get the ID, longitude, and latitude from the json object
    vehicle_id = telemetry_data['vehicle_id']
    long = telemetry_data['longitude']
    lat = telemetry_data['latitude']
    
    #Find the corresponding driver name in Mongo from vehicle_id
    dname = (driv.find_one({"vehicle_id": vehicle_id},
                         ))['driver_name']
    
    #Find which zone the driver is in, and if the zone is something specific (Warehouse, Restricted, etc.)
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
        #Create a new dictionary with the previous data, but add the driver_name and zone and description
        dat = {**telemetry_data, "driver_name" : dname, 'geo_data': geodata if geodata else None}
                                                                      
        #Send it to the enriched producer                                                                      
        enriched_producer.send(topic='vehicle_events_enriched', value=dat)

        pprint(dat)
        
    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        enriched_producer.flush()
                                    