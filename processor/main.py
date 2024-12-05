import os
import psycopg2
import pandas as pd

# Get the DATABASE_URL environment variable
database_url = os.getenv('DATABASE_URL')

import sys


def read_timescaledb_data():
    print(database_url, file=sys.stdout)
    conn = psycopg2.connect(database_url)
    query = 'SELECT * FROM cml_data'
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def process_data():
    # Your implementation here
    pass

if __name__ == "__main__":
    import time
    time.sleep(10)

    df = read_timescaledb_data()
    print(df)
    process_data()