import pandas as pd
import numpy as np
import os
from utils.load_configuration import load_cfg_file

CFG_PATH = './utils/config.yaml'

if __name__ == '__main__':
    # Range of timestamp to generate
    start_ts = '26-09-2022'
    end_ts = '25-09-2023'

    # Features to generate
    features = ['pressure', 'velocity', 'speed']

    # Create the timestamp column
    ts = pd.date_range(start=start_ts, end=end_ts, freq='H')
    df = pd.DataFrame(ts, columns=['event_timestamp'])
    # df = df.set_index('event_timestamp')
    
    # Random floats in the half-open interval [0.0, 1.0)
    # to add other columns in the dataframe
    for feature in features:
        df[feature] = np.random.random_sample((len(ts),))
    
    # Load our pre-defined config to find where the 
    # fake data path will reside in
    cfg = load_cfg_file(CFG_PATH)
    fake_data_cfg = cfg["fake_data"]
    num_files = fake_data_cfg["num_files"]

    # Split data frame by num_files
    df_splits = np.array_split(df, num_files)

    for i in range(num_files):
        df_splits[i].reset_index()\
            .to_parquet(
                os.path.join(
                    fake_data_cfg["folder_path"],
                    f"part_{i}.parquet"
                )
            )