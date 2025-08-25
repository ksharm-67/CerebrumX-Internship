from pymongo import MongoClient
from clickhouse_connect import Client

#MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["test"]
mongo_collection = mongo_db["users"]

#ClickHouse connection
clickhouse_client = Client(
    host='localhost',  
    port=8123,        
    username='default',
    password=''
)

# ClickHouse table name
clickhouse_table = 'test'

#Fetch data from MongoDB
mongo_data = list(mongo_collection.find({}))

if not mongo_data:
    print("No data found!")
    exit()

#Prepare data for ClickHouse
#Convert MongoDB documents to a list of tuples
rows_to_insert = []
for doc in mongo_data:
    rows_to_insert.append((
        doc.get('Name'),
        doc.get('Net-worth'),
        doc.get('DOB')
    ))

#Insert into ClickHouse
clickhouse_client.insert(
    clickhouse_table,
    rows_to_insert,
    column_names=['Name', 'Net-Worth', 'DOB']
)

print(f"Synced {len(rows_to_insert)} documents from MongoDB to ClickHouse.")
