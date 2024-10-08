import logging
import os
import sys
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS



logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(filename)s - %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


url = "http://10.180.113.115:32086"
token = "R8I5zKqjKqoFcPMG4LGweBeVfYKjLE7Ei-l1I_hfDpFD5VpabaAOyx95jrdIegrMItfUJ2mVgcyD7EANZjtELg=="
org = "taisp"
bucket = "TAISP-DEMO"


client = InfluxDBClient(url=url, token=token, org=org)

# Create a write client
write_api = client.write_api(write_options=SYNCHRONOUS)