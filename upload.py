import pymongo
from pymongo import MongoClient
import certifi
import time
import random
import datetime
import csv

# Initialize MongoDB client
client = MongoClient("mongodb+srv://aarushibawejaji:test@cluster0.imgm1l7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", tlsCAFile=certifi.where())
db = client["Main_tractor"]
collection = db["demo"]

# List to store documents
document_list = []

# Open the CSV file
with open('c://Users//AARUSHI//Downloads//test_data.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)

    start_time = time.time()
    c = datetime.datetime.now()
    def get_lat_direction(lat):
        return "N" if lat >= 0 else "S"
    
    def get_lng_direction(lng):
        return "E" if lng >= 0 else "W"

    for row in reader:
        try:
            latitude = float(row['LAT'])
            longitude = float(row['LON'])
            timestamp = row['timestamp']
            dt = datetime.datetime.now()
            spd = (random.randint(100, 999)) / 100
            head = random.randint(0, 360)
            current_time = c.strftime('%H:%M:%S')
            dtt = dt.strftime('%y-%m-%d')
            tract = 6060
            data = {
                "tractor": tract,
                "timestamp": timestamp,
                "date": dtt,
                "time": current_time,
                "position_data": {
                    "latitude": latitude,
                    "longitude": longitude,
                    "speed": spd,
                    "heading": head,
                    "latitude_direction": get_lat_direction(latitude),
                    "longitude_direction": get_lng_direction(longitude),
                }
            }
            print(dt)
            # Append the combined data to the list
            document_list.append(data)
            print(data)
            # Uncomment the following line to insert into MongoDB
            # collection.insert_one(data)
        except Exception as e:
            print(f"Error processing row {row}: {e}")
        collection.insert_one(data)
        time.sleep(0.02)

