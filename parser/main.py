import requests
import psycopg2
import os
import time


def get_dataframe_from_cml_dataset(ds):
    """Return data as DataFrame from a CML xarray.Dataset
    
    Parameters
    ----------
    ds : CMLDataset
        The CML dataset to convert.
    
    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the 'tsl' and 'rsl' columns.
    
    Notes
    -----
        This function assumes that the CML dataset has a 'time' index and columns 'cml_id' and 'sublink_id'.
        The 'time' index is reordered to 'time', 'cml_id', and 'sublink_id', and the DataFrame is sorted
        by these columns. The 'tsl' and 'rsl' columns are extracted from the DataFrame.
    """
    df = ds.to_dataframe() 
    df = df.reorder_levels(order=['time', 'cml_id', 'sublink_id'])
    df = df.sort_values(by=['time', 'cml_id'])
    return df.loc[:, ['tsl', 'rsl']]


def get_metadata_dataframe_from_cml_dataset(ds):
    """Return a DataFrame containing metadata from a CML xarray.Dataset

    Parameters
    ----------
    ds : xr.Dataset
        The CML dataset to retrieve metadata from, assuming that the
        OpenSense naming conventions and structure are used.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the metadata from the CML dataset.
    """
    return ds.drop_vars(ds.data_vars).drop_dims('time').to_dataframe()


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
    # Currently required so that the DB container is ready before we start parsing
    time.sleep(5)
    parse_csv_and_write_to_db()