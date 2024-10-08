import requests
import time
import os
import threading
from datetime import datetime, timedelta
from utils import logger

# Define URLs for Service 2 and Service 4
SERVICE_2_URLS = {
    'bs-0': 'http://bs0-svc.taisp-demo.svc:5001/get_data',  # URL for Service 2 instance 1 (bs1)
    'bs-1': 'http://bs1-svc.taisp-demo.svc:5001/get_data',  # URL for Service 2 instance 2 (bs2)
    # Add more Service 2 URLs if needed
}

SERVICE_4_URL = 'http://model-serving.taisp-demo.svc:5001/inference'    # URL for Service 4

SERVICE_5_URL = 'http://rapp-svc.taisp-demo.svc:5001/submit_connection_map'  # URL for Service 5

def get_data_from_ran():
    hostname = os.getenv('HOSTNAME')

    bs_id = 'bs-0' if hostname == 'xapp-0' else 'bs-1'

    while True:
        try:
            # Get the latest data from BS
            response = requests.get(SERVICE_2_URLS[bs_id])
            connection_map = {}
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Received data from BS {bs_id}: {len(data)} records")
                
                # Call model serving with the retrieved data
                for uid, udata in data.items():
                    
                    if len(udata) == 0:
                        continue

                    latest_item = max(udata, key=lambda x: datetime.strptime(x['time'], '%a, %d %b %Y %H:%M:%S %Z'))

                    # Get the time of the latest item
                    latest_time = datetime.strptime(latest_item['time'], '%a, %d %b %Y %H:%M:%S %Z')

                    logger.info(f'UE {uid}: {len(udata)} records, latest record at {latest_time}')

                    # Get the current time
                    current_time = datetime.now()

                    # Check if the latest item's time is within the last 10 seconds
                    if current_time - latest_time <= timedelta(seconds=10):
                        connection_map[uid] = get_prediction(udata)
                
                logger.info(f'Current connection_map: {connection_map}')
                if connection_map:
                    send_connection_map(connection_map)

            else:
                logger.warning(f"Failed to get data from Service {SERVICE_2_URLS[bs_id]}: {response.status_code}")
        except Exception as e:
            logger.exception(f"Error while getting data from Service {SERVICE_2_URLS[bs_id]}: {e}")

        # Wait for a specified interval before the next request
        time.sleep(60)  # Adjust the interval as needed (e.g., 60 seconds)

def get_prediction(data):
    try:
        # Send the data to Service 4 to get prediction results
        response = requests.post(SERVICE_4_URL, json=data)
        if response.status_code == 200:
            prediction = response.json()
            logger.info(f"Received prediction from model serving: {prediction}")
            return prediction['result']
        else:
            logger.warning(f"Failed to get prediction from model serving: {response.status_code}")
    except Exception as e:
        logger.exception(f"Error while getting prediction from model serving: {e}")


def send_connection_map(connection_map):
    try:
        response = requests.post(SERVICE_5_URL, json=connection_map)
        if response.status_code == 200:
            logger.info(f"Successfully get updated connection from rApp. {response.json()}")
            updated_con_map = response.json()['updated_mapping']
            for uid, uconfig in updated_con_map.items():
                data = {'bs_id': uconfig['bs'], 'data_source': uconfig['service']}
                ue_url = f'http://{uid}-svc.taisp-demo.svc:5001/change_bs'
                logger.info(f'Data sent to UE {uid}: {data}')
                resp = requests.post(ue_url, json=data)
                if resp.status_code == 200:
                    logger.info(f"Successfully update UE {uid}. {resp.json()}")
        else:
            logger.warning(f"Failed to send connection_map to rApp: {response.status_code}")
    except Exception as e:
        logger.exception(f"Error while sending connection_map to rApp: {e}")



if __name__ == "__main__":
    # Start the data fetching thread
    threading.Thread(target=get_data_from_ran).start()
