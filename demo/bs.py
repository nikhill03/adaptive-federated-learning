import pandas as pd
import requests
from flask import Flask, request, jsonify
import os
import logging
from datetime import datetime
from utils import logger


app = Flask(__name__)

# Store received data
received_data = None

def reset_received_data():
    global received_data
    received_data = {'ue-0': [], 'ue-1': [], 'ue-2': [], 'ue-3': [], 'ue-4': []}

@app.route('/data', methods=['POST'])
def receive_data():
    global received_data
    try:
        logger.info(f"Received data from {request.json['uid']}")
        for rec in request.json['data']:
            rec['time'] = datetime.now()
        received_data[request.json['uid']].extend(request.json['data']) # Store received data

        logger.info(f"Number of received records for {request.json['uid']}: {len(received_data[request.json['uid']])} records")
        return jsonify({'message': 'Data received and processed'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_data', methods=['GET'])
def get_data():
    global received_data
    try:
        if received_data is None:
            return jsonify({'message': 'No data available'}), 404
        
        res = jsonify(received_data), 200  # Return the last received data
        reset_received_data()

        return res
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    reset_received_data()
    app.run(host='0.0.0.0', port=5001)
