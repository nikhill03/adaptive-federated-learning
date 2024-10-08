import pandas as pd
import requests
import time
import os
from datetime import datetime
import argparse
from flask import Flask, request, jsonify
from threading import Thread
import logging
from influxdb_client import Point
from utils import logger, write_api
import utils
import random


# Define URLs for Service 2 instances
SERVICE_2_URLS = {
    'bs-0': 'http://bs0-svc.taisp-demo.svc:5001/data',  # URL for Service 2 instance 1 (bs1)
    'bs-1': 'http://bs1-svc.taisp-demo.svc:5001/data',  # URL for Service 2 instance 2 (bs2)
    # Add more Service 2 URLs if needed
}

IOT_CSV_FILE = 'iot.csv'
VIDEO_CSV_FILE = 'video.csv'

app = Flask(__name__)

current_bs_id = None  # To be set via arguments
data_source_type = None  # To be set via arguments


def read_data_from_csv(file_name):
    try:
        df = pd.read_csv(file_name)
        return df.sample(random.randint(10, 20))  # Randomly select one row
    except Exception as e:
        logger.exception(f"Error reading {file_name}: {e}")
        return None

def send_data():
    global current_bs_id, data_source_type

    instance_id = os.getenv('HOSTNAME')

    while True:
        # Decide which file to use based on data_source_type
        current_file = IOT_CSV_FILE if data_source_type == 'Ping' else VIDEO_CSV_FILE

        data_row = read_data_from_csv(current_file)

        data_row = data_row[['time', 'ul_brate', 'dl_brate']]

        
        if data_row is not None:

            # Convert the selected row to a dictionary
            data_dict = data_row.to_dict(orient='records')

            service_url = SERVICE_2_URLS.get(current_bs_id)
            if service_url:
                try:
                    logger.info(f"[Instance {instance_id}] is sending data to service {current_bs_id} at {service_url}: {len(data_dict)} records")
                    requests.post(service_url, json={'uid': instance_id, 'data': data_dict})
                except Exception as e:
                    logger.error(f"Unable to send data to BS due to error: {e}")

        time.sleep(5)  # Wait for 3 minutes

@app.route('/change_bs', methods=['POST'])
def change_bs():
    global current_bs_id
    global data_source_type

    new_bs_id = request.json.get('bs_id')
    new_data_source = request.json.get('data_source')

    if new_data_source not in ['Ping', 'Video']:
        return jsonify({'error': 'Invalid Data Source'}), 400
    
    data_source_type = new_data_source
    old_bs_id = current_bs_id

    if new_bs_id in SERVICE_2_URLS.keys():
        if current_bs_id != new_bs_id:
            
            current_bs_id = new_bs_id
            msg = f'UE {request.json.get('uid')} changed from BS {old_bs_id} to BS {current_bs_id}'

            logger.info(msg)

            return jsonify({'message': msg}), 200
        
        return jsonify({'message': f'Already connecting to {current_bs_id}. No need further change.'}), 200
    
    else:
        return jsonify({'error': 'Invalid BS ID'}), 400


@app.route('/change_data_source', methods=['POST'])
def change_data_source():
    global data_source_type
    new_data_source = request.json.get('data_source')
    if new_data_source in ['Ping', 'Video']:
        data_source_type = new_data_source
        return jsonify({'message': f'Data source changed to {data_source_type}'}), 200
    else:
        return jsonify({'error': f'Invalid Data Source {new_data_source}. Must be Ping or Video.'}), 400

if __name__ == "__main__":
    
    # Set global variables based on arguments
    
    instance_id = os.getenv('HOSTNAME')

    if instance_id in ['ue-0', 'ue-1', 'ue-2']:
        data_source_type = 'Ping'
        current_bs_id = 'bs-0'

    if instance_id in ['ue-3', 'ue-4']:
        data_source_type = 'Video'
        current_bs_id = 'bs-1'

    logger.info(f'Instance {instance_id} connecting to BS {current_bs_id} and sending {data_source_type} data')


    # Start the Flask app in a separate thread
    Thread(target=app.run, kwargs={'host':'0.0.0.0', 'port': 5001}).start()
    
    # Start sending data
    send_data()
