'''
this code makes list of list based on the distance difference
odd numbred list contains data till significant distance difference occurs
and even numbred list contains data of the 2 points between which difference was detected
'''
from flask import Flask, render_template, request, jsonify, session, Response
from pymongo import MongoClient
from bson.json_util import dumps
from datetime import datetime, timedelta
import threading
import json
import time
import certifi
import os
import pandas as pd
from math import radians, sin, cos, sqrt, atan2

app = Flask(__name__)

app.secret_key = os.urandom(24)

try:
    client = MongoClient("mongodb+srv://aarushibawejaji:test@cluster0.imgm1l7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", tlsCAFile=certifi.where())
    db = client["Main_tractor"]
    collection = db["demo"]
    print("Connected to MongoDB successfully.")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")

stop_event = threading.Event()

def haversine(lon1, lat1, lon2, lat2):
    R = 6371e3  # Earth's radius in meters
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)

    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c  # Distance in meters
    return distance / 1000  # Convert to kilometers

def watch_changes():
    global selected_duration
    global selected_tractor
    pipeline = [{'$match': {'fullDocument.tractor': int(selected_tractor)}}]
    try:
        with collection.watch(pipeline) as stream:
            for change in stream:
                # If duration is 1, fetch data for the whole day and stop streaming
                if selected_duration == '1':
                    break
                curr_data = {
                    'latitude': change["fullDocument"]["position_data"]["latitude"],
                    'longitude': change["fullDocument"]["position_data"]["longitude"],
                    'speed': change["fullDocument"]["position_data"]["speed"],
                    'heading': change["fullDocument"]["position_data"]["heading"],
                    'timestamp': str(change["fullDocument"]["timestamp"])
                }
                print(f"Streaming data: {curr_data}")  
                yield f"data:{json.dumps(curr_data)}\n\n"
                time.sleep(1)
    except Exception as e:
        print(f"Error in change stream: {e}")

def check_missing_data():
    x = collection.find()
    all_lists = []
    current_list = []
    previous_point = None
    odd_list = True

    for data in x:
        try:
            timestamp = pd.to_datetime(data.get('timestamp'))
            position_data = data.get('position_data', {})
            latitude = position_data.get('latitude')
            longitude = position_data.get('longitude')
            tractor = data.get('tractor')
            speed = position_data.get('speed')
            heading = position_data.get('heading')

            if latitude is None or longitude is None:
                continue

            current_data = {
                "latitude": float(latitude),
                "longitude": float(longitude),
                "tractor": tractor,
                "timestamp": timestamp,
                "speed": speed,
                "heading": heading,
                "distance_difference": None if previous_point is None else haversine(previous_point['longitude'], previous_point['latitude'], float(longitude), float(latitude))
            }

            if previous_point is not None:
                distance_difference = current_data["distance_difference"]
                if distance_difference is not None and distance_difference > 1:  # Assuming 1 km as the threshold for significant distance
                    print("Significant distance difference detected")
                    
                    if odd_list:
                        if current_list:
                            all_lists.append(current_list)
                        current_list = [previous_point, current_data]
                        odd_list = False
                    else:
                        current_list = [previous_point, current_data]
                        all_lists.append(current_list)
                        current_list = []
                        odd_list = True
                else:
                    current_list.append(current_data)
            else:
                current_list.append(current_data)
            
            previous_point = current_data
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Data not appended due to error: {e}")

    if current_list:
        all_lists.append(current_list)

    modified_lists = []
    for i, lst in enumerate(all_lists):
        if i > 0:
            lst.insert(0, modified_lists[-1][-1])
        if (i + 1) % 2 == 0:
            lst = lst[:2]
        modified_lists.append(lst)

    for idx, lst in enumerate(modified_lists):
        print(f"List {idx + 1}:")
        print(len(lst))
        # Uncomment the next lines to print the actual data
        # for item in lst:
        #     print(item)
    return modified_lists

@app.route('/')
def index():
    return render_template('home_page.html')

@app.route('/submit-data', methods=['POST'])
def submit_data():
    data = request.json
    global selected_tractor
    global selected_duration
    selected_tractor = data.get('tractor')
    selected_duration = data.get('duration')

    # Save the data in session
    session['selected_date'] = data.get('date')
    session['selected_tractor'] = data.get('tractor')
    session['selected_duration'] = data.get('duration')
    print(f"Data submitted: {data}")  # Debug statement
    return jsonify(data)

upload_thread = threading.Thread(target=watch_changes)
upload_thread.daemon = True
upload_thread.start()

@app.route('/process-data', methods=['GET'])
def process():
    # Get the data from session
    global selected_duration
    global selected_tractor

    selected_date = session.get('selected_date')
    selected_tractor = session.get('selected_tractor')
    selected_duration = session.get('selected_duration')

    print(f"Session data - Date: {selected_date}, Tractor: {selected_tractor}, Duration: {selected_duration}") 

    if not selected_date or not selected_tractor or not selected_duration:
        return jsonify({'error': 'Missing data in session'}), 400

    # Calculate the end date for the query
    start_date = datetime.strptime(selected_date, '%Y-%m-%d')
    end_date = start_date + timedelta(days=1)

    print(f"Querying data from {start_date} to {end_date} for tractor {selected_tractor}")  

    if selected_duration == '1':
        retrieved_data = check_missing_data()

        response_data = []
        for sublist in retrieved_data:
            sublist_data = []
            for data_point in sublist:
                sublist_data.append({
                    'latitude': data_point["latitude"],
                    'longitude': data_point["longitude"],
                    'speed': data_point["speed"],
                    'heading': data_point["heading"],
                    'timestamp': data_point["timestamp"],
                })
            response_data.append(sublist_data)

        print(f"Retrieved data length: {len(response_data)}")
        json_response = jsonify(response_data)
        return json_response

@app.route('/process', methods=['GET'])
def curr_val():
    global selected_duration
    global selected_tractor
    selected_tractor = session.get('selected_tractor')
    selected_duration = session.get('selected_duration')
    if not selected_tractor or not selected_duration:
        return jsonify({'error': 'Missing data in session'}), 400
    print(f"Processing data with duration {selected_duration}")  
    # Stream the current data
    if selected_duration == '0':
        print("Fetching current data") 
        return Response(watch_changes(), mimetype="text/event-stream")
    # Fetch data for the whole day
    else:
        if selected_duration == '1':
            print("Fetching whole day data") 
            return process()

# Run the Flask application
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3000, threaded=True)
