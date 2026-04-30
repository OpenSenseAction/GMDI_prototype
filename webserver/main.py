import json
import os
import time
import math
import psycopg2
from psycopg2 import sql as pgsql
import folium
import requests
from markupsafe import escape
from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    Response,
    redirect,
    url_for,
    flash,
)
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    logout_user,
    login_required,
    current_user,
)
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager
import uuid

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(32))
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # WSGI-level enforcement

# ── User store (loaded from file at startup) ──────────────────────────────────
_users_config_path = os.getenv("USERS_CONFIG_PATH", "/app/configs/users.json")
try:
    with open(_users_config_path) as _f:
        USERS = json.load(_f)
except FileNotFoundError:
    USERS = {}

# ── Flask-Login setup ─────────────────────────────────────────────────────────
login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message = "Please log in to access this page."


class User(UserMixin):
    def __init__(self, user_id: str):
        self.id = user_id
        self.display_name = USERS[user_id].get("display_name", user_id)


@login_manager.user_loader
def load_user(user_id: str):
    return User(user_id) if user_id in USERS else None


ALLOWED_EXTENSIONS = {"nc", "csv", "h5", "hdf5"}
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB

# Data directories
DATA_INCOMING_DIR = "/app/data_incoming"
DATA_STAGED_FOR_PARSING_DIR = "/app/data_staged_for_parsing"
DATA_ARCHIVED_DIR = "/app/data_archived"


def ensure_data_directories():
    """Create data directories if they don't exist."""
    for directory in [
        DATA_INCOMING_DIR,
        DATA_STAGED_FOR_PARSING_DIR,
        DATA_ARCHIVED_DIR,
    ]:
        Path(directory).mkdir(parents=True, exist_ok=True)


def safe_float(value):
    """Return a JSON-safe float (converting NaN/inf to None)."""
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


# ── Database helpers ─────────────────────────────────────────────────────────


def get_db_connection():
    """Admin connection as webserver_role (cross-tenant queries)."""
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


@contextmanager
def user_db_scope(user_id: str):
    """Context manager: connection scoped to user_id for one request.

    Connects as webserver_role then issues SET LOCAL ROLE <user_id>.
    SET LOCAL is automatically reverted at transaction end, so role
    bleed is impossible even on connection reuse.

    The role name is composed with pgsql.Identifier (never %s) so it
    cannot be used as a SQL injection vector.  user_id is also
    allowlisted against USERS before reaching SQL composition.
    """
    if user_id not in USERS:
        raise ValueError(f"Unknown user_id: {user_id!r}")

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        with conn.cursor() as cur:
            cur.execute(
                pgsql.SQL("SET LOCAL ROLE {}").format(pgsql.Identifier(user_id))
            )
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ==================== AUTH ROUTES ====================


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("overview"))
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username in USERS and check_password_hash(
            USERS[username]["password_hash"], password
        ):
            login_user(User(username))
            next_page = request.args.get("next")
            # Guard against open-redirect: only allow relative paths.
            if next_page and not next_page.startswith("/"):
                next_page = None
            return redirect(next_page or url_for("overview"))
        flash("Invalid username or password.")
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


# ==================== LANDING PAGE ROUTES ====================


@app.route("/")
@login_required
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
        with user_db_scope(current_user.id) as conn:
            cur = conn.cursor()

            # Get count of CMLs visible to this user (RLS enforced)
            cur.execute("SELECT COUNT(DISTINCT cml_id) FROM cml_metadata")
            stats["total_cmls"] = cur.fetchone()[0]

            # Approximate count via secure view
            cur.execute("SELECT COUNT(*) FROM cml_data_secure")
            stats["total_records"] = cur.fetchone()[0]

            # Get data date range (from 1h secure view)
            cur.execute("SELECT MIN(bucket), MAX(bucket) FROM cml_data_1h_secure")
            result = cur.fetchone()
            if result:
                stats["data_start_date"] = result[0]
                stats["data_end_date"] = result[1]

            cur.close()
    except Exception as e:
        print(f"Error fetching landing stats: {e}")

    return render_template("landing.html", stats=stats)


# ==================== REAL-TIME DATA ROUTES ====================


def generate_cml_map(user_id: str):
    """Generate a Leaflet map showing all CMLs with clickable lines"""
    try:
        with user_db_scope(user_id) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT ON (cml_id) cml_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat FROM cml_metadata ORDER BY cml_id"
            )
            data = cur.fetchall()
            cur.close()

        if not data:
            return None

        # Calculate average position
        latitudes = [row[2] for row in data]
        longitudes = [row[1] for row in data]
        avg_lat = sum(latitudes) / len(latitudes)
        avg_lon = sum(longitudes) / len(longitudes)

        # Create map
        m = folium.Map(location=[avg_lat, avg_lon], zoom_start=8)

        # Store CML IDs list for JavaScript
        cml_ids_json = json.dumps([str(row[0]) for row in data])

        # Add CML lines with onclick handlers
        for idx, row in enumerate(data):
            cml_id = row[0]
            site_0_lon = row[1]
            site_0_lat = row[2]
            site_1_lon = row[3]
            site_1_lat = row[4]

            # Create GeoJSON feature with cml_id in properties
            geojson_feature = {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[site_0_lon, site_0_lat], [site_1_lon, site_1_lat]],
                },
                "properties": {
                    "cml_id": str(cml_id),
                },
            }

            # Create a custom popup with onclick
            cml_id_js = json.dumps(str(cml_id))
            cml_id_html = str(escape(cml_id))
            popup_html = f"""
            <div>
                <strong>CML ID: {cml_id_html}</strong><br>
                <button onclick="window.handleCmlClick({cml_id_js}); return false;" style="margin-top: 5px; padding: 5px 10px; background-color: #0066cc; color: white; border: none; border-radius: 3px; cursor: pointer;">
                    Load Data
                </button>
            </div>
            """

            # Add GeoJSON layer
            geojson_layer = folium.GeoJson(
                geojson_feature,
                style_function=lambda x: {
                    "color": "blue",
                    "weight": 2.5,
                    "opacity": 0.8,
                },
                popup=folium.Popup(popup_html, max_width=200),
                name=f"CML {cml_id}",
            )

            # Add mouse event handlers via JavaScript
            geojson_layer.add_to(m)

        # Get the HTML and inject additional click handlers
        map_html = m._repr_html_()

        # Add JavaScript to enhance interactivity
        enhanced_js = (
            """
        <script>
        (function() {
            // Delay to ensure map is loaded
            setTimeout(function() {
                var cmlIds = """
            + cml_ids_json
            + """;
                
                // Find all SVG paths and attach click handlers
                function attachHandlers() {
                    // Get all paths in the SVG
                    var allPaths = document.querySelectorAll('svg path');
                    var bluePaths = [];
                    var attachedCount = 0;
                    
                    // Filter for blue paths
                    allPaths.forEach(function(path) {
                        var strokeColor = path.getAttribute('stroke');
                        if (strokeColor && strokeColor.toLowerCase() === 'blue' && !path.hasAttribute('data-cml-click-attached')) {
                            bluePaths.push(path);
                        }
                    });
                    
                    console.log('Found ' + bluePaths.length + ' blue paths out of ' + allPaths.length + ' total paths');
                    
                    bluePaths.forEach(function(path, index) {
                        if (index < cmlIds.length) {
                            var cmlId = cmlIds[index];
                            
                            // Mark as attached
                            path.setAttribute('data-cml-click-attached', 'true');
                            path.style.cursor = 'pointer';
                            
                            // Click handler
                            path.addEventListener('click', function(e) {
                                e.stopPropagation();
                                e.preventDefault();
                                console.log('Direct path click for CML:', cmlId);
                                if (typeof window.handleCmlClick === 'function') {
                                    window.handleCmlClick(cmlId);
                                } else {
                                    console.error('handleCmlClick function not found');
                                }
                            }, true);
                            
                            // Hover effects
                            path.addEventListener('mouseover', function() {
                                this.style.strokeWidth = (this.getAttribute('stroke-width') || 2.5) * 1.5;
                                this.style.opacity = '1';
                            });
                            
                            path.addEventListener('mouseout', function() {
                                this.style.strokeWidth = this.getAttribute('stroke-width') || '2.5';
                                this.style.opacity = this.getAttribute('opacity') || '0.8';
                            });
                            
                            attachedCount++;
                        }
                    });
                    
                    if (attachedCount > 0) {
                        console.log('Attached click handlers to', attachedCount, 'paths');
                    }
                }
                
                attachHandlers();
                
                // Try again after a bit more delay in case paths are added dynamically
                setTimeout(attachHandlers, 500);
                setTimeout(attachHandlers, 1500);
            }, 1000);
        })();
        </script>
        """
        )

        return map_html + enhanced_js

    except Exception as e:
        print(f"Error generating map: {e}")
        return None


def get_available_cmls(user_id: str):
    """Get list of CMLs visible to the given user."""
    try:
        with user_db_scope(user_id) as conn:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT cml_id FROM cml_metadata ORDER BY cml_id")
            cmls = [row[0] for row in cur.fetchall()]
            cur.close()
        return cmls
    except Exception as e:
        print(f"Error fetching CMLs: {e}")
        return []


@app.route("/realtime")
@login_required
def realtime():
    """Real-time data page"""
    map_html = generate_cml_map(current_user.id)
    cmls = get_available_cmls(current_user.id)
    default_cml = cmls[0] if cmls else None

    return render_template(
        "realtime.html",
        map_html=map_html,
        cmls=cmls,
        selected_cml=default_cml,
    )


@app.route("/grafana")
@login_required
def grafana_root_redirect():
    """Redirect /grafana to /grafana/ for proper subpath routing."""
    return redirect("/grafana/", code=302)


@app.route(
    "/grafana/",
    defaults={"path": ""},
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
@app.route(
    "/grafana/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
)
@login_required
def grafana_proxy(path):
    """Proxy all requests to Grafana container.

    Injects X-WEBAUTH-USER so Grafana's auth proxy mode maps the request to
    the correct Grafana user.  Any X-WEBAUTH-USER header sent by the browser
    is stripped first to prevent identity forgery.
    """
    grafana_url = f"http://grafana:3000/grafana/{path}"
    method = request.method
    headers = {
        key: value
        for key, value in request.headers
        if key.lower() not in ("host", "x-webauth-user")
    }
    headers["X-WEBAUTH-USER"] = current_user.id
    data = request.get_data()
    params = request.args

    resp = requests.request(
        method,
        grafana_url,
        headers=headers,
        params=params,
        data=data,
        cookies=request.cookies,
        allow_redirects=False,
    )

    excluded_headers = [
        "content-encoding",
        "content-length",
        "transfer-encoding",
        "connection",
    ]
    response_headers = [
        (name, value)
        for name, value in resp.headers.items()
        if name.lower() not in excluded_headers
    ]
    return Response(resp.content, resp.status_code, response_headers)


@app.route("/api/cml-metadata")
@login_required
def api_cml_metadata():
    """API endpoint for fetching CML metadata"""
    try:
        with user_db_scope(current_user.id) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT ON (cml_id) cml_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat FROM cml_metadata ORDER BY cml_id"
            )
            data = cur.fetchall()
            cur.close()

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


@app.route("/api/cml-map")
@login_required
def api_cml_map():
    """API endpoint for fetching CML data optimized for map rendering"""
    try:
        with user_db_scope(current_user.id) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT ON (cml_id) cml_id::text, site_0_lon, site_0_lat, site_1_lon, site_1_lat FROM cml_metadata ORDER BY cml_id"
            )
            data = cur.fetchall()
            cur.close()

        cmls = [
            {
                "cml_id": str(row[0]),
                "site_0": {"lon": float(row[1]), "lat": float(row[2])},
                "site_1": {"lon": float(row[3]), "lat": float(row[4])},
            }
            for row in data
        ]
        return jsonify(cmls)
    except Exception as e:
        print(f"Error fetching CML map data: {e}")
        return jsonify([])


@app.route("/api/cml-stats")
@login_required
def api_cml_stats():
    """API endpoint for fetching per-CML statistics for data quality visualization"""
    try:
        with user_db_scope(current_user.id) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    cs.cml_id::text,
                    cs.total_records,
                    cs.valid_records,
                    cs.null_records,
                    cs.completeness_percent,
                    cs.min_rsl,
                    cs.max_rsl,
                    cs.mean_rsl,
                    cs.stddev_rsl,
                    cs.last_rsl,
                    ROUND(STDDEV(cd.rsl)::numeric, 2) as stddev_last_60min
                FROM cml_stats cs
                LEFT JOIN (
                    SELECT cml_id, rsl
                    FROM cml_data_secure
                    WHERE time >= (SELECT MAX(bucket) FROM cml_data_1h_secure) - INTERVAL '60 minutes'
                ) cd ON cs.cml_id = cd.cml_id
                GROUP BY cs.cml_id, cs.total_records, cs.valid_records, cs.null_records,
                         cs.completeness_percent, cs.min_rsl, cs.max_rsl, cs.mean_rsl,
                         cs.stddev_rsl, cs.last_rsl
                ORDER BY cs.cml_id
            """
            )
            data = cur.fetchall()
            cur.close()

        stats = [
            {
                "cml_id": str(row[0]),
                "total_records": int(row[1]),
                "valid_records": int(row[2]),
                "null_records": int(row[3]),
                "completeness_percent": safe_float(row[4]),
                "min_rsl": safe_float(row[5]),
                "max_rsl": safe_float(row[6]),
                "mean_rsl": safe_float(row[7]),
                "stddev_rsl": safe_float(row[8]),
                "last_rsl": safe_float(row[9]),
                "stddev_last_60min": safe_float(row[10]),
            }
            for row in data
        ]
        return jsonify(stats)
    except Exception as e:
        print(f"Error fetching CML stats: {e}")
        return jsonify([])


@app.route("/api/data-time-range")
@login_required
def api_data_time_range():
    """API endpoint for fetching the actual time range of available data"""
    try:
        with user_db_scope(current_user.id) as conn:
            cur = conn.cursor()
            cur.execute("SELECT MIN(bucket), MAX(bucket) FROM cml_data_1h_secure")
            result = cur.fetchone()
            cur.close()

        if result and result[0] and result[1]:
            # Format as ISO 8601 strings
            return jsonify(
                {"earliest": result[0].isoformat(), "latest": result[1].isoformat()}
            )
        return jsonify({"earliest": None, "latest": None})
    except Exception as e:
        print(f"Error fetching data time range: {e}")
        return jsonify({"earliest": None, "latest": None})


# ==================== ARCHIVE STATISTICS ROUTES ====================


def get_archive_statistics(user_id: str):
    """Fetch aggregated statistics from the long-term archive for the given user."""
    stats = {
        "total_records": 0,
        "cml_count": 0,
        "date_range": {"start": None, "end": None},
    }

    try:
        with user_db_scope(user_id) as conn:
            cur = conn.cursor()

            # Row count via secure view
            cur.execute("SELECT COUNT(*) FROM cml_data_secure")
            stats["total_records"] = cur.fetchone()[0]

            # CML count (RLS enforced)
            cur.execute("SELECT COUNT(DISTINCT cml_id) FROM cml_metadata")
            stats["cml_count"] = cur.fetchone()[0]

            # Date range (from 1h secure view)
            cur.execute("SELECT MIN(bucket), MAX(bucket) FROM cml_data_1h_secure")
            result = cur.fetchone()
            if result:
                stats["date_range"]["start"] = result[0]
                stats["date_range"]["end"] = result[1]

            cur.close()
    except Exception as e:
        print(f"Error fetching archive statistics: {e}")

    return stats


@app.route("/archive")
@login_required
def archive():
    """Archive statistics page"""
    stats = get_archive_statistics(current_user.id)
    return render_template("archive.html", stats=stats)


# ==================== DATA UPLOADS ROUTES ====================


@app.route("/data-uploads")
@login_required
def data_uploads():
    """Data uploads page"""
    return render_template("data_uploads.html")


# ==================== DATA UPLOAD API ====================


def allowed_file(filename):
    """Check if file extension is allowed"""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def get_file_size_mb(filepath):
    """Get file size in MB"""
    return os.path.getsize(filepath) / (1024 * 1024)


@app.route("/api/upload", methods=["POST"])
@login_required
def upload_file():
    """Handle file upload via drag and drop"""
    try:
        # Check if file is in request
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files["file"]

        if file.filename == "":
            return jsonify({"error": "No file selected"}), 400

        safe_name = secure_filename(file.filename)
        if not safe_name or not allowed_file(safe_name):
            return (
                jsonify({"error": "File type not allowed. Allowed: nc, csv, h5, hdf5"}),
                400,
            )

        # Generate unique filename to avoid collisions
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        original_name = safe_name.rsplit(".", 1)[0]
        extension = safe_name.rsplit(".", 1)[1]
        new_filename = f"{original_name}_{timestamp}_{unique_id}.{extension}"

        filepath = os.path.join(DATA_INCOMING_DIR, new_filename)

        # Enforce size limit via Content-Length before writing to disk.
        # MAX_CONTENT_LENGTH rejects oversized requests at the WSGI layer;
        # also guard here in case that config is not set.
        content_length = request.content_length
        if content_length and content_length > MAX_FILE_SIZE:
            return jsonify({"error": "File size exceeds 500 MB limit"}), 400

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
                    "original_filename": safe_name,
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
@login_required
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
    # Create data directories
    ensure_data_directories()
    # Wait for database to be ready
    time.sleep(10)
    app.run(host="0.0.0.0", port=5000, debug=True)
