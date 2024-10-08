import logging
import os
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

UE_METRICS_FILE = os.getenv('UE_METRICS_FILE', 'ue_metrics.csv')
PING_UE_METRICS_FILE = os.getenv('PING_UE_METRICS_FILE', 'ping_ue_metrics.csv')