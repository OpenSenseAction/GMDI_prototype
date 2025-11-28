import os
import time
import psycopg2
import pandas as pd

import folium
import panel as pn
import bokeh.plotting

time.sleep(10)

pn.extension(sizing_mode="stretch_width")

def get_metadata_from_db():
    # Connect to the database
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    query = "SELECT cml_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat FROM cml_metadata"
    return pd.read_sql_query(query, conn)

def get_data_from_db():
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    query = 'SELECT * FROM cml_data WHERE cml_id = %s AND sublink_id = %s'
    params = ('10023', 'sublink_1')
    return pd.read_sql_query(query, conn, params=params)


# Function to read data from DB and generate a Bokeh plot
def generate_map():
    df_metadata = get_metadata_from_db()

    # Create a map centered at the average latitude and longitude

    avg_lat = df_metadata['site_0_lat'].mean()
    avg_lon = df_metadata['site_0_lon'].mean()
    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=10)

    for i in range(0, len(df_metadata)):
        folium.PolyLine(
            [
                [df_metadata.iloc[i]['site_0_lat'], df_metadata.iloc[i]['site_0_lon']], 
                [df_metadata.iloc[i]['site_1_lat'], df_metadata.iloc[i]['site_1_lon']]
            ]
        ).add_to(m)
    return pn.pane.plot.Folium(m, height=400, sizing_mode='stretch_width')

def generate_time_series_plot():
    df = get_data_from_db()
    p = bokeh.plotting.figure(width=800)
    p.line(df['time'], df['rsl'])
    return pn.pane.Bokeh(p, height=200, sizing_mode='stretch_width')


pn.template.FastListTemplate(
    title="GMDI prototype", main=[generate_map(), generate_time_series_plot()]
).servable()