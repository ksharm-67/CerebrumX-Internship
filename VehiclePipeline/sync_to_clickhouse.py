from kafka import KafkaConsumer
import clickhouse_connect
import json

client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')

r = client.query("""create table if not exists telemetry.enriched_telemetry_history(vehicle_id String,
                                                                          timestamp DateTime,
                                                                          latitude Float64, 
                                                                          longitude Float64,
                                                                          speed UInt8,
                                                                          driver_name String,
                                                                          zone String 
                                                                          )
                    ENGINE = MergeTree() PARTITION BY toYYYYMM(timestamp) ORDER BY (vehicle_id, timestamp);""")

print((client.query('show tables')).result_set)

consumer = KafkaConsumer('vehicle_events_enriched',
                         bootstrap_servers = ['localhost:9092'])

for message in consumer:
    enriched_data = json.loads(message.value.decode('utf-8'))
    
    vehicle_id = enriched_data['vehicle_id']
    timestamp = enriched_data['timestamp']
    long = enriched_data['longitude']
    lat = enriched_data['latitude']
    speed = enriched_data['speed']
    driver_name = enriched_data['driver_name']
    zone_name = enriched_data['geo_data'][0]['zone_name'] if enriched_data['geo_data'] else None

    
    client.insert(
        table="telemetry.enriched_telemetry_history",
        columns=["vehicle_id", "timestamp", "latitude", "longitude", "speed", "driver_name", "zone"],
        values=[(vehicle_id, timestamp, lat, long, speed, driver_name, zone_name)]
    )
    
    
    
    
    

