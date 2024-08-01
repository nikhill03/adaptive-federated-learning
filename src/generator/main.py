import time
import pandas as pd
from datetime import datetime, timedelta
import signal
import sys


def signal_handler(sig, frame):
    print('Ctrl+C pressed. Exiting gracefully...')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

def main():
    df = pd.read_csv('raw.csv', sep=',')
    df = df.drop(["nrCellIdentity", "nbCellIdentity_0", "nbCellIdentity_1", "nbCellIdentity_2", "nbCellIdentity_3", "nbCellIdentity_4", "ue-id"], axis=1, errors='ignore')
    cells = [1001, 1002, 1003, 1004]
    
    while True:
        try:
            sub_df = df[df['CELL_GLOBAL_ID'].isin(cells)]
            start_datetime = datetime.now()
            dt = start_datetime - timedelta(minutes=5)
            sub_df.loc[:, 'DATETIME'] = dt
                
            sub_df.to_csv('pmhistory.csv', index=False)
            print('Sleep 300s and create another dataset')
            time.sleep(300)
        except KeyboardInterrupt:
            print("KeyboardInterrupt detected. Exiting gracefully...")
            sys.exit(0)

        
if __name__ == "__main__":
    main()