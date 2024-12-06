import os
import time
import folium
import psycopg2
from flask import Flask, render_template_string, request
import altair as alt
app = Flask(__name__)

# Function to read data from DB and generate a Leaflet map
def generate_map():
    # Connect to the database
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cur = conn.cursor()
    
    # Execute a query to retrieve data from the table
    cur.execute("SELECT cml_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat FROM cml_metadata")
    data = cur.fetchall()
    
    # Create a map centered at the average latitude and longitude
    latitudes = [row[2] for row in data]
    longitudes = [row[1] for row in data]
    avg_lat = sum(latitudes) / len(latitudes)
    avg_lon = sum(longitudes) / len(longitudes)
    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=4)
    
    # Loop through the data and add a line for each row
    for row in data:
        cml_id = row[0]
        site_0_lon = row[1]
        site_0_lat = row[2]
        site_1_lon = row[3]
        site_1_lat = row[4]
        folium.PolyLine([[site_0_lat, site_0_lon], [site_1_lat, site_1_lon]], color='blue', weight=2.5, opacity=1, popup=f'cml_id: {cml_id}').add_to(m)
    
    # Save the map as an HTML file
    m.save("map.html")
    
    # Close the database connection
    cur.close()
    conn.close()

# Function to query time series data from the database and add it to the altair plot
def generate_time_series_plot(cml_id=None):
    # # Connect to the database
    # conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    # cur = conn.cursor()
    
    # # Execute a query to retrieve time series data from the table
    # if cml_id:
    #     cur.execute("SELECT date, value FROM time_series_data WHERE cml_id = %s", (cml_id,))
    # else:
    #     cur.execute("SELECT date, value FROM time_series_data")
    # data = cur.fetchall()
    import pandas as pd

    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    query = 'SELECT * FROM cml_data WHERE cml_id = %s AND sublink_id = %s'
    params = ('10001', 'sublink_1')
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    # Create an altair plot
    plot = alt.Chart(df).mark_line().encode(
        x='time:T',
        y='rsl:Q'
    )
    
    # Return the plot as an HTML string
    return plot.to_html()

# Route to serve the map and time series plot
@app.route('/')
def serve_map_and_plot():
    with open('map.html', 'r') as f:
        map_html = f.read()
    
    time_series_plot_html = generate_time_series_plot()
    
    # Combine the map and time series plot HTML
    combined_html = f"{map_html}<h2>Time Series Plot</h2>{time_series_plot_html}"
    return render_template_string(combined_html)

# Route to update the time series plot based on the selected cml_id
@app.route('/update_plot', methods=['POST'])
def update_time_series_plot():
    cml_id = request.form['cml_id']
    time_series_plot_html = generate_time_series_plot(cml_id)
    return render_template_string(time_series_plot_html)

# Start the Flask server
if __name__ == "__main__":
    time.sleep(10)

    generate_map()
    app.run(host='0.0.0.0', debug=True)