import requests
import psycopg2
import os
import time

time.sleep(5)



# Function to parse CSV files and write to the TimescaleDB container
def parse_csv_and_write_to_db():
    # Define a function to create dummy data

    df = _create_dummy_data()

    # Connect to the database
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    
    # Create a cursor object
    cur = conn.cursor()
    
    # Iterate through the DataFrame and insert the data into the database
    for tup in df.itertuples():
        cur.execute(
            "INSERT INTO cml_data (time, cml_id, RSL, TSL) VALUES (%s, %s, %s, %s)",
            (tup.time, tup.cml_id, tup.rsl, tup.tsl)
        )
    conn.commit()
    
    
    # Commit the changes to the database
    conn.commit()
    
    # Close the cursor and connection
    cur.close()
    conn.close()


def _create_dummy_data():
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta

    # Create dummy data
    cml_ids = [f"cml_{i:03d}" for i in range(1, 11)]
    timestamps = pd.date_range(start=datetime.now() - timedelta(hours=1), periods=60, freq='min')

    # Create a list to hold the DataFrames for each sensor_id
    dfs = []

    # Loop through each sensor_id and create a DataFrame for it
    for i, cml_id in enumerate(cml_ids):
        df = pd.DataFrame(index=timestamps)
        df['rsl'] = np.random.randn(len(df.index)) + i
        df['tsl'] = np.random.randn(len(df.index)) + i
        df['cml_id'] = cml_id
        dfs.append(df)

    # Concatenate the DataFrames into one long DataFrame
    df = pd.concat(dfs)

    df = df.reset_index(names='time')
    
    return df



if __name__ == "__main__":
    parse_csv_and_write_to_db()