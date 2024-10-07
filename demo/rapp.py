from flask import Flask, request, jsonify
from datetime import datetime
import requests
from influxdb_client import Point
import utils
from utils import logger, write_api

app = Flask(__name__)

# Configure logging

connection_mapping = {'ue-0': {'service': 'Ping', 'bs': 'bs-0'}, 
                      'ue-1': {'service': 'Ping', 'bs': 'bs-0'},
                      'ue-2': {'service': 'Ping', 'bs': 'bs-0'}, 
                      'ue-3': {'service': 'Video', 'bs': 'bs-1'}, 
                      'ue-4': {'service': 'Video', 'bs': 'bs-1'}}

@app.route('/submit_connection_map', methods=['POST'])
def submit_connection_map():
    global connection_mapping
    try:
        # Get data from xApp
        data = request.json
        logger.info(f"Received data from xApp: {data}")
        
        updated_mapping = {}

        for uid, service in data.items():
            if service == connection_mapping[uid]['service']:
                continue
            
            updated_mapping[uid] = {'service': service, 'bs': 'bs-0' if service == 'Ping' else 'bs-1'}
            connection_mapping[uid] = updated_mapping[uid]
    


        return jsonify({'updated_mapping': updated_mapping}), 200
            
    except Exception as e:
        logger.exception(f"Error in receive_prediction: {e}")
        return jsonify({'error': 'Internal server error'}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)  # Run on all interfaces
