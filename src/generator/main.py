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
    df = pd.read_csv('pmhistory.csv', sep=',')
    df = df.drop(["nrCellIdentity", "nbCellIdentity_0", "nbCellIdentity_1", "nbCellIdentity_2", "nbCellIdentity_3", "nbCellIdentity_4", "ue-id"], axis=1, errors='ignore')
    cells = df['CELL_GLOBAL_ID'].unique().tolist()
    try:
        start_datetime = datetime.now()
        increment = timedelta(seconds=5)

        for cell in cells:
            n = df[df['CELL_GLOBAL_ID']==cell].shape[0]
            datetime_list = [start_datetime + i * increment for i in range(n)]
            cell_indices = df.index[df['CELL_GLOBAL_ID']==cell].tolist()
            df.loc[cell_indices, 'DATETIME'] = datetime_list

        df.to_csv('pmhistory.csv', index=False)
    except KeyboardInterrupt:
        print("KeyboardInterrupt detected. Exiting gracefully...")
        sys.exit(0)
        
if __name__ == "__main__":
    main()