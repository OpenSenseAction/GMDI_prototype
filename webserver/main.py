import os
import time
import psycopg2
import pandas as pd
import folium
import altair as alt
from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
from pathlib import Path
import uuid

app = Flask(__name__)

# Data directories
DATA_INCOMING_DIR = "/app/data_incoming"
DATA_STAGED_FOR_PARSING_DIR = "/app/data_staged_for_parsing"
DATA_ARCHIVED_DIR = "/app/data_archived"

# Create directories if they don't exist
for directory in [DATA_INCOMING_DIR, DATA_STAGED_FOR_PARSING_DIR, DATA_ARCHIVED_DIR]:
    Path(directory).mkdir(parents=True, exist_ok=True)


# Database connection helper
def get_db_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


# ==================== LANDING PAGE ROUTES ====================


@app.route("/")
def overview():
    """Landing page with overview and processing status"""
    stats = {
        "total_cmls": 0,
        "total_records": 0,
        "data_start_date": None,
        "data_end_date": None,
        "processing_status": "Not yet implemented",
    }

    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()

            # Get count of CMLs
            cur.execute("SELECT COUNT(DISTINCT cml_id) FROM cml_metadata")
            stats["total_cmls"] = cur.fetchone()[0]

            # Get count of data records
            cur.execute("SELECT COUNT(*) FROM cml_data")
            stats["total_records"] = cur.fetchone()[0]

            # Get data date range
            cur.execute("SELECT MIN(time), MAX(time) FROM cml_data")
            result = cur.fetchone()
            if result:
                stats["data_start_date"] = result[0]
                stats["data_end_date"] = result[1]

            cur.close()
            conn.close()
    except Exception as e:
        print(f"Error fetching landing stats: {e}")

    return render_template("landing.html", stats=stats)


# ==================== REAL-TIME DATA ROUTES ====================


def generate_cml_map():
    """Generate a Leaflet map showing all CMLs"""
    try:
        conn = get_db_connection()
        if not conn:
            return None

        cur = conn.cursor()
        cur.execute(
            "SELECT cml_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat FROM cml_metadata"
        )
        data = cur.fetchall()
        cur.close()
        conn.close()

        if not data:
            return None

        # Calculate average position
        latitudes = [row[2] for row in data]
        longitudes = [row[1] for row in data]
        avg_lat = sum(latitudes) / len(latitudes)
        avg_lon = sum(longitudes) / len(longitudes)

        # Create map
        m = folium.Map(location=[avg_lat, avg_lon], zoom_start=8)

        # Add CML lines
        for row in data:
            cml_id = row[0]
            site_0_lon = row[1]
            site_0_lat = row[2]
            site_1_lon = row[3]
            site_1_lat = row[4]

            # Create polyline with data attribute to store CML ID
            line = folium.PolyLine(
                [[site_0_lat, site_0_lon], [site_1_lat, site_1_lon]],
                color="blue",
                weight=2.5,
                opacity=0.8,
                popup=folium.Popup(f"CML ID: {cml_id}", max_width=200),
            )
            # Store CML ID in the feature's properties for JavaScript access
            line.options["cml_id"] = cml_id
            line.add_to(m)

        return m._repr_html_()

    except Exception as e:
        print(f"Error generating map: {e}")
        return None


def get_available_cmls():
    """Get list of available CMLs"""
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor()
        cur.execute("SELECT cml_id FROM cml_metadata ORDER BY cml_id")
        cmls = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return cmls
    except Exception as e:
        print(f"Error fetching CMLs: {e}")
        return []


def generate_time_series_plot(cml_id, sublink_id="sublink_1", hours=24):
    """Generate a time series plot for a specific CML"""
    try:
        conn = get_db_connection()
        if not conn:
            return None

        query = """
            SELECT time, rsl 
            FROM cml_data 
            WHERE cml_id = %s AND sublink_id = %s
            AND time >= NOW() - INTERVAL '%s hours'
            ORDER BY time
        """
        df = pd.read_sql_query(query, conn, params=(cml_id, sublink_id, hours))
        conn.close()

        if df.empty:
            return None

        # Create Altair plot
        df["time"] = pd.to_datetime(df["time"])
        chart = (
            alt.Chart(df)
            .mark_line(point=True)
            .encode(x="time:T", y="rsl:Q", tooltip=["time:T", "rsl:Q"])
            .properties(
                width=800, height=400, title=f"Received Signal Level - CML {cml_id}"
            )
            .interactive()
        )

        return chart.to_html()
    except Exception as e:
        print(f"Error generating time series plot: {e}")
        return None


@app.route("/realtime")
def realtime():
    """Real-time data page"""
    map_html = generate_cml_map()
    cmls = get_available_cmls()
    default_cml = cmls[0] if cmls else None
    plot_html = generate_time_series_plot(default_cml) if default_cml else None

    return render_template(
        "realtime.html",
        map_html=map_html,
        cmls=cmls,
        selected_cml=default_cml,
        plot_html=plot_html,
    )


@app.route("/api/cml-metadata")
def api_cml_metadata():
    """API endpoint for fetching CML metadata"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"cmls": []})

        cur = conn.cursor()
        cur.execute(
            "SELECT cml_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat FROM cml_metadata ORDER BY cml_id"
        )
        data = cur.fetchall()
        cur.close()
        conn.close()

        cmls = [
            {
                "id": row[0],
                "site_0_lon": row[1],
                "site_0_lat": row[2],
                "site_1_lon": row[3],
                "site_1_lat": row[4],
            }
            for row in data
        ]
        return jsonify({"cmls": cmls})
    except Exception as e:
        print(f"Error fetching CML metadata: {e}")
        return jsonify({"cmls": []})


@app.route("/api/timeseries/<cml_id>")
def api_timeseries(cml_id):
    """API endpoint for fetching time series data"""
    hours = request.args.get("hours", 24, type=int)
    plot_html = generate_time_series_plot(cml_id, hours=hours)
    if not plot_html:
        return jsonify(
            {
                "html": "<div class='alert alert-info'><i class='fas fa-info-circle'></i> No data available for this CML</div>"
            }
        )
    return jsonify({"html": plot_html})


# ==================== ARCHIVE STATISTICS ROUTES ====================


def get_archive_statistics():
    """Fetch aggregated statistics from the long-term archive"""
    stats = {
        "total_records": 0,
        "cml_count": 0,
        "date_range": {"start": None, "end": None},
        "records_per_cml": [],
        "uptime_stats": {"online": 0, "offline": 0},
    }

    try:
        conn = get_db_connection()
        if not conn:
            return stats

        cur = conn.cursor()

        # Total records
        cur.execute("SELECT COUNT(*) FROM cml_data")
        stats["total_records"] = cur.fetchone()[0]

        # CML count
        cur.execute("SELECT COUNT(DISTINCT cml_id) FROM cml_metadata")
        stats["cml_count"] = cur.fetchone()[0]

        # Date range
        cur.execute("SELECT MIN(time), MAX(time) FROM cml_data")
        result = cur.fetchone()
        if result:
            stats["date_range"]["start"] = result[0]
            stats["date_range"]["end"] = result[1]

        # Records per CML
        cur.execute(
            """
            SELECT cml_id, COUNT(*) as count 
            FROM cml_data 
            GROUP BY cml_id 
            ORDER BY count DESC 
            LIMIT 10
        """
        )
        stats["records_per_cml"] = [
            {"cml_id": row[0], "count": row[1]} for row in cur.fetchall()
        ]

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error fetching archive statistics: {e}")

    return stats


def generate_archive_charts():
    """Generate charts for archive statistics"""
    try:
        conn = get_db_connection()
        if not conn:
            return {"data_distribution": None}

        # Get data distribution by minute
        query = """
            SELECT DATE_TRUNC('minute', time) as minute, COUNT(*) as count 
            FROM cml_data 
            GROUP BY DATE_TRUNC('minute', time)
            ORDER BY minute
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            return {"data_distribution": None}

        # Convert minute column to datetime for proper sorting
        df["minute"] = pd.to_datetime(df["minute"])

        # Create bar chart
        chart = (
            alt.Chart(df)
            .mark_bar()
            .encode(x="minute:T", y="count:Q", tooltip=["minute:T", "count:Q"])
            .properties(width=900, height=400, title="Data Records per Minute")
            .interactive()
        )

        return {"data_distribution": chart.to_html()}
    except Exception as e:
        print(f"Error generating archive charts: {e}")
        return {"data_distribution": None}


@app.route("/archive")
def archive():
    """Archive statistics page"""
    stats = get_archive_statistics()
    charts = generate_archive_charts()

    return render_template(
        "archive.html", stats=stats, chart_html=charts["data_distribution"]
    )


# ==================== DATA UPLOADS ROUTES ====================


@app.route("/data-uploads")
def data_uploads():
    """Data uploads page"""
    return render_template("data_uploads.html")


# ==================== DATA UPLOAD API ====================

ALLOWED_EXTENSIONS = {"nc", "csv", "h5", "hdf5"}
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB


def allowed_file(filename):
    """Check if file extension is allowed"""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def get_file_size_mb(filepath):
    """Get file size in MB"""
    return os.path.getsize(filepath) / (1024 * 1024)


@app.route("/api/upload", methods=["POST"])
def upload_file():
    """Handle file upload via drag and drop"""
    try:
        # Check if file is in request
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files["file"]

        if file.filename == "":
            return jsonify({"error": "No file selected"}), 400

        if not allowed_file(file.filename):
            return (
                jsonify({"error": "File type not allowed. Allowed: nc, csv, h5, hdf5"}),
                400,
            )

        # Generate unique filename to avoid collisions
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        original_name = file.filename.rsplit(".", 1)[0]
        extension = file.filename.rsplit(".", 1)[1]
        new_filename = f"{original_name}_{timestamp}_{unique_id}.{extension}"

        filepath = os.path.join(DATA_INCOMING_DIR, new_filename)

        # Save file
        file.save(filepath)

        # Check file size
        file_size_mb = get_file_size_mb(filepath)
        if file_size_mb > 500:
            os.remove(filepath)
            return jsonify({"error": "File size exceeds 500 MB limit"}), 400

        return (
            jsonify(
                {
                    "success": True,
                    "filename": new_filename,
                    "original_filename": file.filename,
                    "size_mb": round(file_size_mb, 2),
                    "upload_time": timestamp,
                }
            ),
            200,
        )

    except Exception as e:
        print(f"Error uploading file: {e}")
        return jsonify({"error": "Failed to upload file"}), 500


@app.route("/api/files", methods=["GET"])
def get_files():
    """Get list of files in data_incoming and data_staged_for_parsing directories"""
    try:
        incoming_files = []
        staged_files = []

        # Get incoming files
        if os.path.exists(DATA_INCOMING_DIR):
            for filename in os.listdir(DATA_INCOMING_DIR):
                filepath = os.path.join(DATA_INCOMING_DIR, filename)
                if os.path.isfile(filepath):
                    file_size_mb = get_file_size_mb(filepath)
                    upload_time = datetime.fromtimestamp(
                        os.path.getctime(filepath)
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    incoming_files.append(
                        {
                            "filename": filename,
                            "size_mb": round(file_size_mb, 2),
                            "upload_time": upload_time,
                        }
                    )

        # Get staged files
        if os.path.exists(DATA_STAGED_FOR_PARSING_DIR):
            for filename in os.listdir(DATA_STAGED_FOR_PARSING_DIR):
                filepath = os.path.join(DATA_STAGED_FOR_PARSING_DIR, filename)
                if os.path.isfile(filepath):
                    file_size_mb = get_file_size_mb(filepath)
                    upload_time = datetime.fromtimestamp(
                        os.path.getctime(filepath)
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    staged_files.append(
                        {
                            "filename": filename,
                            "size_mb": round(file_size_mb, 2),
                            "upload_time": upload_time,
                        }
                    )

        # Sort by upload time (newest first)
        incoming_files.sort(key=lambda x: x["upload_time"], reverse=True)
        staged_files.sort(key=lambda x: x["upload_time"], reverse=True)

        return (
            jsonify({"incoming_files": incoming_files, "staged_files": staged_files}),
            200,
        )

    except Exception as e:
        print(f"Error getting files: {e}")
        return jsonify({"error": "Failed to retrieve files"}), 500


# ==================== ERROR HANDLERS ====================


@app.errorhandler(404)
def not_found(error):
    return render_template("404.html"), 404


@app.errorhandler(500)
def server_error(error):
    return render_template("500.html"), 500


# ==================== START SERVER ====================

if __name__ == "__main__":
    # Wait for database to be ready
    time.sleep(10)
    app.run(host="0.0.0.0", port=5000, debug=True)
