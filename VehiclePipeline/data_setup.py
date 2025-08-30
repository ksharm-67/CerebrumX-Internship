from pymongo import MongoClient
import json

client = MongoClient("mongodb://localhost:27017/")

db = client["vehicle_telemetry"]
driv = db["driver_data"]

try:
    with open("driverData.json", "r") as f:
        data = json.load(f)
        print(data)
        db.driv.insert_many(data)

except Exception as e:
    print(f"Error: {e}")
    

geo = db["geofences"]
try:
    with open("geofences.json", "r") as g:
        reg = json.load(g)
        print(ref)
        db.geo.insert_many(reg)
        
except Exception as ex:
    print(f"Error: {ex}")
    
