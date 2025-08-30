from pymongo import MongoClient
import json

#Connect to your MongoDB Database
client = MongoClient("mongodb://localhost:27017/")

#Define the database and driver data collection
db = client["vehicle_telemetry"]
driv = db["driver_data"]

#From a json file, send driver data to your Mongo database
try:
    with open("driverData.json", "r") as f:
        data = json.load(f)
        print(data)
        db["driver_data"].insert_many(data)

except Exception as e:
    print(f"Error: {e}")
    
    
#Define the geofence collection
geo = db["geofences"]

#From a json file, define the geofence boundaries
try:
    with open("geofences.json", "r") as g:
        reg = json.load(g)
        print(reg)
        db.geofences.insert_many(reg)
        
except Exception as ex:
    print(f"Error: {ex}")
    
#Create a 2dsphere index for efficiency
db.geofences.create_index([("polygon", "2dsphere")]) 


