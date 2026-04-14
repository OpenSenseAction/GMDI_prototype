# Multi-User Architecture with RLS

## Executive Summary

This document describes the architecture for supporting multiple users with strong data isolation while maintaining a single external entry point and minimal resource overhead.

**Key Design Principles:**
- **Single external URL and SFTP port** (minimal network admin dependency)
- **Strong data isolation** via PostgreSQL Row-Level Security (RLS)
- **Resource efficient** (~3-4 GB for 10 users vs 250+ GB for full isolation)
- **Intelligent data lifecycle** (compression + aggregates: RAM stays constant as data grows)
- **Never delete data** (keep all raw data forever, disk is cheap)
- **Fast recent + slow historical** (30 days uncompressed, older compressed on-demand)
- **Multi-method upload** (SFTP, HTTP API, web drag-and-drop)
- **Per-user parser flexibility** for different data formats
- **Simple user onboarding** without network configuration changes

## Architecture Overview

### High-Level Design

```
┌──────────────────── External (Single Entry) ─────────────────┐
│                                                                │
│  https://company.com          (port 443)                      │
│  sftp://company.com:2222      (port 2222)                     │
│                                                                │
└────────────────────────────────────────────────────────────────┘
                           ↓
┌──────────────── Nginx Reverse Proxy (Not Controlled) ─────────┐
│                                                                │
│  :443  → webserver:5000                                       │
│  :2222 → sftp_receiver:22                                     │
│                                                                │
└────────────────────────────────────────────────────────────────┘
                           ↓
┌────────────────── Your Application Stack ─────────────────────┐
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ SFTP Server (Single Instance)                           │ │
│  │  /home/user1/uploads/  (user1's SSH key)                │ │
│  │  /home/user2/uploads/  (user2's SSH key)                │ │
│  │  /home/user3/uploads/  (user3's SSH key)                │ │
│  └─────────────────────────────────────────────────────────┘ │
│            ↓                                                   │
│       sftp_uploads                                             │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Webserver (Single Instance)                             │ │
│  │  /login            → User login (username/password)     │ │
│  │  /api/upload       → HTTP POST with API key             │ │
│  │  /upload           → Drag-and-drop form upload          │ │
│  │  /data-uploads     → Upload page                        │ │
│  │  /                 → Dashboard (RLS filtered)           │ │
│  │  /grafana/         → Embedded Grafana                   │ │
│  └─────────────────────────────────────────────────────────┘ │
│            ↓                                                   │
│       web_uploads                                              │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Ingestion Coordinator (Lightweight Service)             │ │
│  │  - Watches sftp_uploads + web_uploads                   │ │
│  │  - Identifies user from path                            │ │
│  │  - Triggers appropriate parser                          │ │
│  └─────────────────────────────────────────────────────────┘ │
│            ↓              ↓              ↓                     │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐          │
│  │  Parser 1    │ │  Parser 2    │ │  Parser 3    │          │
│  │   (user1)   │ │   (user2)   │ │   (user3)   │          │
│  │ HTTP trigger │ │ HTTP trigger │ │ HTTP trigger │          │
│  │ endpoint     │ │ endpoint     │ │ endpoint     │          │
│  └──────────────┘ └──────────────┘ └──────────────┘          │
│            ↓              ↓              ↓                     │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ TimescaleDB (RLS enabled, user_id column)               │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### Component Strategy

| Component | Strategy | Count (10 users) | Authentication Point |
|-----------|----------|------------------|---------------------|
| **SFTP** | Shared with per-user directories | 1 | SSH keys per user |
| **Webserver** | Shared with login + API/form uploads | 1 | Session/API key |
| **Ingestion Coordinator** | Shared file watcher & router | 1 | N/A (internal) |
| **Parser** | Per-user instances | 10 | DB role credentials |
| **Database** | Shared with RLS | 1 | RLS policies by role |
| **Grafana** | Shared (embedded) | 1 | Inherited from webserver |
| **Storage** | Shared with path prefixes | Volumes | Path-based isolation |

### Resource Requirements (10 Users)

| Component | Count | Memory | External Ports | Notes |
|-----------|-------|--------|----------------|-------|
| SFTP | 1 | 17 MB | 1 (port 2222) | |
| Webserver | 1 | 250 MB | 1 (via nginx) | |
| Ingestion Coordinator | 1 | 80 MB | 0 (internal) | |
| Parser | 10 | 630 MB | 0 (internal) | |
| Database | 1 | **2-3 GB** | 0 (internal) | **With compression + aggregates** |
| Grafana | 1 | 200 MB | 0 (embedded) | |
| **Total** | **15 containers** | **~3.2-4.2 GB** | **2 external ports** | **90% RAM reduction!** |

**Comparison to full stack isolation:**
- **Resource savings:** ~95% less RAM (3-4 GB vs 250-300 GB)
- **Containers:** 15 vs 50+
- **External ports:** 2 vs 50+
- **Network config complexity:** Minimal vs extensive
- **Upload methods:** 3 (SFTP, HTTP API, Web) vs 1 (SFTP only)

**Database Memory Optimization:**
- **Unoptimized:** 25-30 GB (all data uncompressed)
- **With compression:** 2-3 GB (30-day hot data + compressed archives)
- **Compression ratio:** 5-10x on old data
- **Aggregate overhead:** ~50-100 MB total

## Security Model

### Defense in Depth - Multiple Isolation Layers

| Layer | Isolation Mechanism | Security Level |
|-------|---------------------|----------------|
| **SFTP** | SSH key auth + chroot jail + separate volumes | Infrastructure |
| **File System** | Separate Docker volumes per user | Infrastructure |
| **Parser** | Each watches only their user's volume | Infrastructure |
| **Database** | RLS policies enforce row-level filtering | Database-enforced |
| **Webserver** | Login session + DB role switching | Application + DB |

### Row-Level Security (RLS) - Database Layer

PostgreSQL RLS provides **database-enforced** data isolation:

- Users cannot bypass RLS even with SQL injection
- Policies are evaluated by PostgreSQL, not application code
- Each parser connects as a different database role
- Webserver switches to user's role after login
- All queries automatically filtered by PostgreSQL

**Example RLS Policy:**
```sql
CREATE POLICY user1_isolation ON cml_data
    USING (user_id = 'user1')
    WITH CHECK (user_id = 'user1');
```

When `user1` executes `SELECT * FROM cml_data`, PostgreSQL automatically adds `WHERE user_id = 'user1'` via the `current_user`-based policy.

### Authentication Flow

#### 1. SFTP Layer (Data Upload)
```
User uploads → SSH key authentication → Chroot to /home/user1/uploads/
```
- Each user has unique SSH key pair
- SFTP server enforces chroot jail (cannot access other users' directories)
- Separate Docker volumes ensure filesystem isolation

#### 2. Parser Layer (Data Processing)
```
Parser watches /home/user1/uploads/ → Connects as user1 → Inserts with user_id='user1'
```
- Each parser instance watches only their user's volume
- Connects to database with unique role credentials
- RLS policy enforces correct user_id on INSERT

#### 3. Web Layer (Data Visualization)
```
User login → Session created → DB connection with user's role → All queries filtered by RLS
```
- Username/password authentication
- Session stores user identity
- Database connection switches to user's role
- All queries automatically scoped to user's data

## User Access Patterns

### User 1 Workflow - Multiple Upload Options

#### Option A: SFTP Upload (Automated/Scripted)
```bash
# 1. Upload data via SFTP (SSH key identifies user)
sftp -P 2222 -i ~/.ssh/user1_key user1@company.com
sftp> cd uploads
sftp> put cml_data.csv
sftp> exit
```

#### Option B: HTTP API Upload (Programmatic)
```bash
# Upload via HTTP API with API key
curl -X POST https://company.com/api/upload \
  -H "X-API-Key: your_api_key_here" \
  -F "file=@cml_data.csv"
```

```python
# Python example
import requests

api_key = "your_api_key_here"
url = "https://company.com/api/upload"

with open("cml_data.csv", "rb") as f:
    response = requests.post(
        url,
        headers={"X-API-Key": api_key},
        files={"file": f}
    )
    
print(response.json())
```

#### Option C: Web Drag-and-Drop Upload (Manual)
```bash
# 1. Login to web interface
# Browser: https://company.com/login
#   → Enter username + password

# 2. Navigate to upload page
# Browser: https://company.com/data-uploads
#   → Drag and drop files or click to browse

# 3. Upload confirmation appears with file size and status
```

#### Viewing Data (All Upload Methods)
```bash
# All uploads are processed identically and appear in dashboard
# Browser: https://company.com
#   → Dashboard shows only user1's data (RLS enforced)
#   → Real-time updates as data is parsed

# Browser: https://company.com/grafana/
#   → Embedded dashboards filtered to user1's data
```

### Network Admin Requirements (One-Time Setup)

**Minimal reverse proxy configuration (request ONCE):**

```nginx
# Web traffic
upstream gmdi_app {
    server your_host:5000;
}

server {
    listen 443 ssl;
    server_name company.com;
    
    location / {
        proxy_pass http://gmdi_app;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}

# SFTP port forwarding (TCP passthrough)
stream {
    server {
        listen 2222;
        proxy_pass your_host:2222;
    }
}
```

**Adding new users requires NO network changes.**

## Advantages vs Alternatives

### vs Full Stack Isolation (Option 2)

| Aspect | This Architecture | Full Stack Isolation |
|--------|------------------|---------------------|
| **RAM (10 users)** | 26-31 GB | 250-300 GB |
| **Containers** | 14 | 50-60 |
| **External ports** | 2 | 50+ |
| **Data isolation** | DB-enforced (RLS) | Infrastructure (separate DBs) |
| **User onboarding** | Add user, no network changes | Deploy full stack + open ports |
| **Operational complexity** | Low (1 DB, 1 webserver) | High (10 DBs, 10 webservers) |
| **Cost** | Single server | Multiple servers or large server |
| **Blast radius** | Shared DB (mitigated by backups) | Fully isolated |

### vs Multi-Tenant Without RLS (Option 1 Basic)

| Aspect | With RLS | Without RLS |
|--------|----------|-------------|
| **Data isolation** | Database-enforced | Application-level only |
| **SQL injection risk** | Protected by RLS | Could expose all users' data |
| **Compliance** | "Database-enforced isolation" | "Application-controlled" |
| **Trust model** | Don't trust application code | Must trust application code |
| **Performance** | ~5-15% overhead | No overhead |

**RLS adds minimal overhead but provides database-level security guarantees.**

## Database Schema Changes

### Tables with user_id Column

```sql
-- Modified schema with user_id
CREATE TABLE cml_data (
    time TIMESTAMPTZ NOT NULL,
    cml_id TEXT NOT NULL,
    sublink_id TEXT NOT NULL,
    rsl REAL,
    tsl REAL,
    user_id TEXT NOT NULL  -- NEW: User identifier
);

CREATE TABLE cml_metadata (
    cml_id TEXT NOT NULL,
    sublink_id TEXT NOT NULL,
    site_0_lon REAL,
    site_0_lat REAL,
    site_1_lon REAL,
    site_1_lat REAL,
    frequency REAL,
    polarization TEXT,
    length REAL,
    user_id TEXT NOT NULL,  -- NEW: User identifier
    PRIMARY KEY (cml_id, sublink_id, user_id)  -- Updated composite key
);

-- Create hypertable
SELECT create_hypertable('cml_data', 'time');
```

### Database Roles

```sql
-- User login roles — role name intentionally matches user_id value in the data.
-- This allows a single current_user-based RLS policy to cover all users,
-- and lets cml_data_1h_secure filter the aggregate without any app WHERE clause.
CREATE ROLE user1 LOGIN PASSWORD 'secure_password_1';
CREATE ROLE user2 LOGIN PASSWORD 'secure_password_2';
CREATE ROLE user3 LOGIN PASSWORD 'secure_password_3';

-- Webserver role (can switch to user roles via SET ROLE)
CREATE ROLE webserver_role LOGIN PASSWORD 'webserver_password';

-- Grant role switching capability
GRANT user1, user2, user3 TO webserver_role;
```

### RLS Policies

```sql
-- Enable RLS on tables
ALTER TABLE cml_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_metadata ENABLE ROW LEVEL SECURITY;

-- Single generic policy per table — works for all users because role name = user_id.
-- No per-user policy needed; onboarding a new user only requires CREATE ROLE.
CREATE POLICY user_data_policy ON cml_data
    FOR ALL
    USING     (user_id = current_user)
    WITH CHECK (user_id = current_user);

CREATE POLICY user_metadata_policy ON cml_metadata
    FOR ALL
    USING     (user_id = current_user)
    WITH CHECK (user_id = current_user);

-- Webserver policies (read-all for admin queries; scoped reads use SET ROLE)
CREATE POLICY webserver_read_policy ON cml_data
    FOR SELECT TO webserver_role
    USING (true);

CREATE POLICY webserver_read_metadata ON cml_metadata
    FOR SELECT TO webserver_role
    USING (true);

-- Grant table permissions (no DELETE — raw data is never deleted by design)
GRANT SELECT, INSERT, UPDATE ON cml_data, cml_metadata TO user1, user2, user3;
GRANT SELECT ON cml_data, cml_metadata TO webserver_role;

-- Security-barrier view over the continuous aggregate.
-- PostgreSQL cannot apply RLS to materialized views, so cml_data_1h itself
-- is not row-filtered.  This view enforces per-user isolation at the DB level.
CREATE VIEW cml_data_1h_secure WITH (security_barrier) AS
SELECT * FROM cml_data_1h
WHERE user_id = current_user;

GRANT SELECT ON cml_data_1h_secure TO user1, user2, user3;
GRANT SELECT ON cml_data_1h        TO webserver_role;  -- direct for admin queries
GRANT SELECT ON cml_data_1h_secure TO webserver_role;  -- via SET ROLE for user pages
```

## Docker Compose Configuration

### Shared SFTP Server

```yaml
services:
  sftp_receiver:
    build: ./sftp_receiver
    ports:
      - "2222:22"
    volumes:
      # Host keys (shared across all users)
      - ./ssh_keys/sftp_host_ed25519_key:/etc/ssh/ssh_host_ed25519_key:ro
      - ./ssh_keys/sftp_host_rsa_key:/etc/ssh/ssh_host_rsa_key:ro
      
      # Per-user authorized_keys
      - ./ssh_keys/user1/authorized_keys:/home/user1/.ssh/keys/authorized_keys:ro
      - ./ssh_keys/user2/authorized_keys:/home/user2/.ssh/keys/authorized_keys:ro
      - ./ssh_keys/user3/authorized_keys:/home/user3/.ssh/keys/authorized_keys:ro
      
      # Per-user upload directories (separate volumes for isolation)
      - sftp_user1_uploads:/home/user1/uploads
      - sftp_user2_uploads:/home/user2/uploads
      - sftp_user3_uploads:/home/user3/uploads

volumes:
  sftp_user1_uploads:
  sftp_user2_uploads:
  sftp_user3_uploads:
```

### SFTP Entrypoint Script

```bash
#!/bin/bash
# sftp_receiver/entrypoint.sh
set -e

# Create upload directories for each user
mkdir -p /home/user1/uploads /home/user2/uploads /home/user3/uploads

# Set ownership (match user IDs)
chown -R 1001:1001 /home/user1/uploads
chown -R 1002:1002 /home/user2/uploads
chown -R 1003:1003 /home/user3/uploads

# Execute SFTP server with multiple users (SSH key auth only, no passwords)
exec /entrypoint \
    user1::1001:1001:uploads \
    user2::1002:1002:uploads \
    user3::1003:1003:uploads
```

### Per-User Parsers

```yaml
services:
  parser_user1:
    build: ./parser  # Could be custom build per user
    environment:
      - DATABASE_URL=postgresql://user1:user1_password@database:5432/mydatabase
      - USER_ID=user1  # Used to insert user_id in data
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
    volumes:
      - sftp_user1_uploads:/app/data/incoming  # Watch user1's SFTP directory
      - parser_user1_archived:/app/data/archived
      - parser_user1_quarantine:/app/data/quarantine
      - ./configs/user1/parser_config.yml:/app/config.yml:ro  # User-specific config

  parser_user2:
    build: ./parser_user2  # Different parser code if needed
    environment:
      - DATABASE_URL=postgresql://user2:user2_password@database:5432/mydatabase
      - USER_ID=user2
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
    volumes:
      - sftp_user2_uploads:/app/data/incoming  # Watch user2's SFTP directory
      - parser_user2_archived:/app/data/archived
      - parser_user2_quarantine:/app/data/quarantine
      - ./configs/user2/parser_config.yml:/app/config.yml:ro

volumes:
  parser_user1_archived:
  parser_user1_quarantine:
  parser_user2_archived:
  parser_user2_quarantine:
```

### Shared Webserver

```yaml
services:
  webserver:
    build: ./webserver
    ports:
      - "5000:5000"
    depends_on:
      - database
    environment:
      - DATABASE_URL=postgresql://webserver_role:webserver_password@database:5432/mydatabase
      - SECRET_KEY=your-secret-key-here  # For session management
      - STORAGE_BACKEND=local
      - STORAGE_BASE_PATH=/app/data
    volumes:
      - webserver_data_staged:/app/data/staged
      - webserver_data_archived:/app/data/archived
      - ./configs/users.json:/app/users.json:ro  # User authentication config
```

## Code Changes Required

### Parser Modifications

```python
# parser/db_writer.py - Add user_id support

class DBWriter:
    def __init__(self, db_url: str, user_id: str = None, connect_timeout: int = 10):
        self.db_url = db_url
        self.user_id = user_id  # NEW: User identifier
        self.connect_timeout = connect_timeout
        # ...

    def write_metadata(self, df: pd.DataFrame) -> int:
        """Write metadata to database with user_id"""
        if self.user_id:
            df['user_id'] = self.user_id  # Add user_id column
        
        # Rest of implementation unchanged
        # ...

    def write_rawdata(self, df: pd.DataFrame) -> int:
        """Write raw data to database with user_id"""
        if self.user_id:
            df['user_id'] = self.user_id  # Add user_id column
        
        # Rest of implementation unchanged
        # ...
```

```python
# parser/main.py - Pass user_id to DBWriter

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "...")
    USER_ID = os.getenv("USER_ID")  # NEW
    # ... rest of config

def main():
    # ...
    db_writer = DBWriter(Config.DATABASE_URL, user_id=Config.USER_ID)
    # ...
```

### Webserver Modifications

```python
# webserver/main.py - Add authentication and role switching

from flask import Flask, session, redirect, url_for, request, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash
import json

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'change-me-in-production')

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# User class for Flask-Login
class User(UserMixin):
    def __init__(self, user_id):
        self.id = user_id

# Load user configuration
with open('/app/users.json', 'r') as f:
    USERS = json.load(f)
    # Format: {
    #   "user1": {
    #     "password_hash": "bcrypt_hash",
    #     "db_role": "user1",
    #     "db_password": "user1_db_password"
    #   }
    # }

@login_manager.user_loader
def load_user(user_id):
    if user_id in USERS:
        return User(user_id)
    return None

def get_db_connection():
    """Get DB connection with current user's role set"""
    if not current_user.is_authenticated:
        # Not logged in - use webserver role
        return psycopg2.connect(os.getenv("DATABASE_URL"))
    
    # Connect as user's specific role for RLS filtering
    user_config = USERS[current_user.id]
    
    # Create connection URL with user's role credentials
    base_url = os.getenv("DATABASE_URL")
    # Replace credentials in URL
    # postgresql://webserver_role:pass@host/db → postgresql://user1:pass@host/db
    user_db_url = base_url.replace(
        'webserver_role:' + os.getenv('WEBSERVER_PASSWORD', 'webserver_password'),
        f"{user_config['db_role']}:{user_config['db_password']}"
    )
    
    conn = psycopg2.connect(user_db_url)
    return conn

@app.route("/login", methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        # Validate credentials
        if username in USERS and check_password_hash(USERS[username]['password_hash'], password):
            user = User(username)
            login_user(user)
            
            next_page = request.args.get('next')
            return redirect(next_page or url_for('overview'))
        else:
            flash('Invalid username or password')
    
    return render_template('login.html')

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route("/")
@login_required
def overview():
    """Landing page - automatically filtered to user's data via RLS"""
    stats = {
        "total_cmls": 0,
        "total_records": 0,
        "data_start_date": None,
        "data_end_date": None,
        "username": current_user.id,  # Display username
    }
    
    try:
        conn = get_db_connection()  # Automatically uses user's role
        cur = conn.cursor()
        
        # These queries are automatically filtered by RLS to current user's data
        cur.execute("SELECT COUNT(DISTINCT cml_id) FROM cml_metadata")
        stats["total_cmls"] = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM cml_data")
        stats["total_records"] = cur.fetchone()[0]
        
        # ... rest of queries automatically scoped to user
    except Exception as e:
        print(f"Error: {e}")
    
    return render_template('overview.html', stats=stats)

# All other routes automatically filtered via get_db_connection()
```

```html
<!-- webserver/templates/login.html -->
<!DOCTYPE html>
<html>
<head>
    <title>Login - GMDI</title>
    <link rel="stylesheet" href="{{ url_for('static', filename='style.css') }}">
</head>
<body>
    <div class="login-container">
        <h1>GMDI Login</h1>
        
        {% with messages = get_flashed_messages() %}
            {% if messages %}
                <div class="alert alert-danger">
                    {{ messages[0] }}
                </div>
            {% endif %}
        {% endwith %}
        
        <form method="POST" action="{{ url_for('login') }}">
            <div class="form-group">
                <label for="username">Username</label>
                <input type="text" id="username" name="username" required autofocus>
            </div>
            
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" required>
            </div>
            
            <button type="submit">Log In</button>
        </form>
    </div>
</body>
</html>
```

## User Onboarding Process

### Adding a New User (user4)

```bash
#!/bin/bash
# scripts/add_user.sh user4

set -e

USER_NAME=$1
USER_ID=$((1000 + $(grep -c "user.*::" sftp_receiver/entrypoint.sh) + 1))

echo "Adding user: $USER_NAME with UID: $USER_ID"

# 1. Generate SSH keys for SFTP access
echo "Generating SSH keys..."
mkdir -p ssh_keys/${USER_NAME}
ssh-keygen -t ed25519 -f ssh_keys/${USER_NAME}/id_ed25519 -N "" -C "${USER_NAME}@gmdi"
cp ssh_keys/${USER_NAME}/id_ed25519.pub ssh_keys/${USER_NAME}/authorized_keys

# 2. Add user to SFTP entrypoint script
echo "Adding to SFTP server..."
sed -i.bak "/^exec \/entrypoint/s/$/ \\\\\n    ${USER_NAME}::${USER_ID}:${USER_ID}:uploads/" sftp_receiver/entrypoint.sh

# 3. Add volumes to docker-compose.yml (manual or template-based)
echo "Add these volumes to docker-compose.yml:"
echo "  - ./ssh_keys/${USER_NAME}/authorized_keys:/home/${USER_NAME}/.ssh/keys/authorized_keys:ro"
echo "  - sftp_${USER_NAME}_uploads:/home/${USER_NAME}/uploads"
echo ""
echo "Add volume definition:"
echo "  sftp_${USER_NAME}_uploads:"

# 4. Create database role and RLS policies
echo "Creating database role and policies..."
docker exec -i gmdi_prototype-database-1 psql -U myuser -d mydatabase <<SQL
-- Create role
CREATE ROLE ${USER_NAME}_role LOGIN PASSWORD '$(openssl rand -base64 32)';

-- Enable RLS (if not already enabled)
ALTER TABLE cml_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_metadata ENABLE ROW LEVEL SECURITY;

-- Create RLS policies
CREATE POLICY ${USER_NAME}_data_policy ON cml_data
    FOR ALL TO ${USER_NAME}_role
    USING (user_id = '${USER_NAME}')
    WITH CHECK (user_id = '${USER_NAME}');

CREATE POLICY ${USER_NAME}_metadata_policy ON cml_metadata
    FOR ALL TO ${USER_NAME}_role
    USING (user_id = '${USER_NAME}')
    WITH CHECK (user_id = '${USER_NAME}');

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON cml_data, cml_metadata TO ${USER_NAME}_role;

-- Allow webserver to switch to this role
GRANT ${USER_NAME}_role TO webserver_role;
SQL

# 5. Add parser service to docker-compose.yml
echo "Add parser_${USER_NAME} service to docker-compose.yml (see template)"

# 6. Add user to webserver users.json
echo "Add user to configs/users.json:"
echo "  \"${USER_NAME}\": {"
echo "    \"password_hash\": \"<bcrypt_hash>\","
echo "    \"db_role\": \"${USER_NAME}_role\","
echo "    \"db_password\": \"<password_from_step_4>\""
echo "  }"

echo ""
echo "✓ User ${USER_NAME} prepared!"
echo "📋 Next steps:"
echo "  1. Update docker-compose.yml with volumes and parser service"
echo "  2. Update configs/users.json with user credentials"
echo "  3. Run: docker compose up -d"
echo "  4. Provide user with:"
echo "     - SSH private key: ssh_keys/${USER_NAME}/id_ed25519"
echo "     - SFTP host: company.com:2222"
echo "     - Web login credentials"
```

---

# Implementation Plan

## High-Level Summary

The implementation consists of 6 main phases:

1. **Database Migration** - Add `user_id` columns, enable RLS; update compression `segmentby` and `cml_data_1h` aggregate for `user_id` (compression and 1h aggregate already in `init.sql`)
2. **SFTP Multi-User Setup** - Configure shared SFTP server with per-user directories
3. **Web and API Upload Methods** - Add HTTP API uploads and web drag-and-drop (no separate coordinator service; uploads write directly to the per-user incoming directory the parser already watches)
4. **Parser Modifications** - Add `user_id` support and per-user deployment
5. **Webserver Authentication** - Add login system and role-based DB access
6. **User Onboarding Automation** - Create scripts and templates for adding users

**Estimated Timeline:** 1-2 weeks

**Key Performance Targets:**
- **Database RAM:** 2-3 GB (constant, regardless of data age) via compression + aggregates
- **Total System RAM:** 3-5 GB for 10 users (95% reduction vs full isolation)
- **Data Retention:** Unlimited - never delete raw data (disk is cheap, RAM stays constant)
- **Query Performance:** 10-100× faster on historical data (via continuous aggregates)
- **Storage Efficiency:** 5-10× compression on data older than 30 days
- **Decompression Speed:** 100-500 MB/sec (fast enough for on-demand queries)

**Dependencies:**
- PostgreSQL 9.5+ (for RLS support) ✓ (TimescaleDB includes this)
- Python packages: `flask-login`, `werkzeug` (for password hashing)
- No external network changes required initially

## PR Plan

Each PR targets `main` directly. PRs 1–4 are fully backward-compatible (no behaviour change for the
current single-user setup). PR 5 (auth) is the only user-visible breaking change.

| PR | Branch | Scope | Backward compatible? |
|----|--------|-------|---------------------|
| 1 | `feat/db-add-user-id` | Add `user_id` columns, update `cml_data_1h` GROUP BY and compression `segmentby`; migration SQL + updated `init.sql` | ✅ Yes |
| 2 | `feat/db-roles-rls` | Create per-user DB roles, enable RLS, add policies; update `init.sql`; `scripts/test_rls.sh` | ✅ Yes (superuser bypasses RLS) |
| 3 | `feat/parser-user-id` | `USER_ID` env var; `db_writer.py` injects it; updated tests | ✅ Yes (`USER_ID` unset → no change) |
| 4 | `feat/sftp-multi-user` | Multi-user SFTP entrypoint, per-user volumes & parsers in `docker-compose.yml`, SSH key generation scripts | ✅ Yes |
| 5 | `feat/webserver-auth` | Flask-Login, `auth.py`, login/logout routes, per-user DB connections, login template; updated tests | ⚠️ Breaking — all routes require login |
| 6 | `feat/web-api-upload` | HTTP API endpoint (`/api/upload` + API-key auth) and drag-and-drop UI on `/data-uploads`; writes to user's SFTP incoming dir | ✅ Yes |
| 7 | `feat/user-onboarding` | `scripts/add_user.sh`, `scripts/hash_password.py`, updated README | ✅ Yes |

**Branching note:** No long-lived dev branch needed. PRs 1–4 are safe to merge to `main` immediately and do not affect the running single-user system. PR 5 is the recommended "go live" milestone for multi-user.

---

## Detailed Implementation Steps

### Phase 1: Database Migration (Priority: HIGH)

**Objective:** Add user_id columns, enable Row-Level Security, and optimize for low memory usage

**Memory Optimization Strategy:**
- **Last 7 days (hot data):** Uncompressed, fast queries (compression policy already set to 7 days in `init.sql`)
- **Older data (warm/cold):** Compressed ~10-20×, slower but queryable, **~0 MB RAM**
- **1-hour aggregate (`cml_data_1h`):** Already in `init.sql`; needs `user_id` added to GROUP BY
- **Retention:** **NEVER delete data** - keep all raw data forever (disk is cheap)
- **RAM behavior:** Stays constant ~2-3 GB regardless of total data size
- **Target:** Constant DB RAM regardless of how many users are added

#### Step 1.1: Backup Current Database
```bash
# Create backup before making changes
docker exec gmdi_prototype-database-1 pg_dump -U myuser -d mydatabase > backup_pre_multiuser.sql
```

**Time estimate:** 10 minutes  
**Risk:** Low (read-only operation)

#### Step 1.2: Add user_id Columns to Existing Tables
```sql
-- database/migrations/001_add_user_id.sql
ALTER TABLE cml_data ADD COLUMN user_id TEXT;
ALTER TABLE cml_metadata ADD COLUMN user_id TEXT;
ALTER TABLE cml_stats ADD COLUMN user_id TEXT;

-- Set existing data to default user (migrate current data)
UPDATE cml_data SET user_id = 'user1' WHERE user_id IS NULL;
UPDATE cml_metadata SET user_id = 'user1' WHERE user_id IS NULL;
UPDATE cml_stats SET user_id = 'user1' WHERE user_id IS NULL;

-- Make columns NOT NULL
ALTER TABLE cml_data ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE cml_metadata ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE cml_stats ALTER COLUMN user_id SET NOT NULL;

-- Update primary keys to include user_id
ALTER TABLE cml_metadata DROP CONSTRAINT cml_metadata_pkey;
ALTER TABLE cml_metadata ADD PRIMARY KEY (cml_id, sublink_id, user_id);

ALTER TABLE cml_stats DROP CONSTRAINT cml_stats_pkey;
ALTER TABLE cml_stats ADD PRIMARY KEY (cml_id, user_id);

-- Create indexes for performance
CREATE INDEX idx_cml_data_user_id ON cml_data(user_id);
CREATE INDEX idx_cml_metadata_user_id ON cml_metadata(user_id);
```

**Execution:**
```bash
docker exec -i gmdi_prototype-database-1 psql -U myuser -d mydatabase < database/migrations/001_add_user_id.sql
```

**Time estimate:** 30 minutes (depends on data volume)  
**Risk:** Medium (schema changes, test on staging first)  
**Rollback:** Restore from backup

#### Step 1.3: Create Database Roles
```sql
-- database/migrations/002_create_roles.sql

-- Create roles for first 3 users
CREATE ROLE user1_role LOGIN PASSWORD 'secure_password_1';
CREATE ROLE user2_role LOGIN PASSWORD 'secure_password_2';
CREATE ROLE user3_role LOGIN PASSWORD 'secure_password_3';

-- Create webserver role with ability to switch to user roles
CREATE ROLE webserver_role LOGIN PASSWORD 'webserver_secure_password';
GRANT user1_role, user2_role, user3_role TO webserver_role;

-- Grant table access
GRANT SELECT, INSERT, UPDATE, DELETE ON cml_data, cml_metadata, cml_stats TO user1_role, user2_role, user3_role;
GRANT SELECT ON cml_data, cml_metadata, cml_stats TO webserver_role;
```

**Time estimate:** 15 minutes  
**Risk:** Low

#### Step 1.4: Enable Row-Level Security
```sql
-- database/migrations/003_enable_rls.sql

-- Enable RLS on tables
ALTER TABLE cml_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_stats ENABLE ROW LEVEL SECURITY;

-- Create policies for user1
CREATE POLICY user1_data_all ON cml_data
    FOR ALL TO user1_role
    USING (user_id = 'user1')
    WITH CHECK (user_id = 'user1');

CREATE POLICY user1_metadata_all ON cml_metadata
    FOR ALL TO user1_role
    USING (user_id = 'user1')
    WITH CHECK (user_id = 'user1');

CREATE POLICY user1_stats_all ON cml_stats
    FOR ALL TO user1_role
    USING (user_id = 'user1')
    WITH CHECK (user_id = 'user1');

-- Create policies for user2
CREATE POLICY user2_data_all ON cml_data
    FOR ALL TO user2_role
    USING (user_id = 'user2')
    WITH CHECK (user_id = 'user2');

CREATE POLICY user2_metadata_all ON cml_metadata
    FOR ALL TO user2_role
    USING (user_id = 'user2')
    WITH CHECK (user_id = 'user2');

CREATE POLICY user2_stats_all ON cml_stats
    FOR ALL TO user2_role
    USING (user_id = 'user2')
    WITH CHECK (user_id = 'user2');

-- Create policies for user3
CREATE POLICY user3_data_all ON cml_data
    FOR ALL TO user3_role
    USING (user_id = 'user3')
    WITH CHECK (user_id = 'user3');

CREATE POLICY user3_metadata_all ON cml_metadata
    FOR ALL TO user3_role
    USING (user_id = 'user3')
    WITH CHECK (user_id = 'user3');

CREATE POLICY user3_stats_all ON cml_stats
    FOR ALL TO user3_role
    USING (user_id = 'user3')
    WITH CHECK (user_id = 'user3');

-- Webserver can read all (will SET ROLE to filter)
CREATE POLICY webserver_read_all_data ON cml_data
    FOR SELECT TO webserver_role
    USING (true);

CREATE POLICY webserver_read_all_metadata ON cml_metadata
    FOR SELECT TO webserver_role
    USING (true);

CREATE POLICY webserver_read_all_stats ON cml_stats
    FOR SELECT TO webserver_role
    USING (true);

-- Bypass RLS for superuser (for admin operations)
ALTER TABLE cml_data FORCE ROW LEVEL SECURITY;
ALTER TABLE cml_metadata FORCE ROW LEVEL SECURITY;
ALTER TABLE cml_stats FORCE ROW LEVEL SECURITY;
```

**Time estimate:** 20 minutes  
**Risk:** Low

#### Step 1.5: Update init.sql for Future Deployments
Update `database/init.sql` to include user_id columns and RLS setup by default for clean deployments.

**Time estimate:** 30 minutes  
**Risk:** Low (doesn't affect existing deployment)

#### Step 1.6: Test RLS Policies
```bash
# Test script: test_rls.sh
#!/bin/bash

echo "Testing RLS policies..."

# Test 1: user1_role can only see user1 data
echo "Test 1: user1_role sees only user1 data"
docker exec -i gmdi_prototype-database-1 psql -U user1_role -d mydatabase -c \
  "SELECT COUNT(*), COUNT(DISTINCT user_id) FROM cml_data; SELECT DISTINCT user_id FROM cml_data;"

# Test 2: user1_role cannot insert user2 data
echo "Test 2: user1_role cannot insert user2 data (should fail)"
docker exec -i gmdi_prototype-database-1 psql -U user1_role -d mydatabase -c \
  "INSERT INTO cml_data (time, cml_id, sublink_id, user_id) VALUES (NOW(), 'test', 'test', 'user2');" \
  && echo "FAIL: Should have been rejected" || echo "PASS: Correctly rejected"

# Test 3: webserver_role can see all data
echo "Test 3: webserver_role sees all data"
docker exec -i gmdi_prototype-database-1 psql -U webserver_role -d mydatabase -c \
  "SELECT user_id, COUNT(*) FROM cml_data GROUP BY user_id;"

echo "RLS tests complete!"
```

**Time estimate:** 30 minutes  
**Risk:** Low (read-only tests)

#### Step 1.7: Update Compression `segmentby` for `user_id`

> **Already implemented:** `init.sql` already enables compression with `add_compression_policy('cml_data', INTERVAL '7 days')`. However, the current `compress_segmentby = 'cml_id, sublink_id'` does not include `user_id`. Adding it ensures per-user queries only decompress the relevant segment.

```sql
-- database/migrations/004_update_compression_segmentby.sql

-- Decompress all existing chunks (required before changing segmentby)
SELECT decompress_chunk(c) FROM show_chunks('cml_data') c;

-- Update compression settings to include user_id
ALTER TABLE cml_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id, cml_id, sublink_id',
    timescaledb.compress_orderby   = 'time DESC'
);

-- Re-compress chunks older than 7 days
SELECT compress_chunk(c)
FROM show_chunks('cml_data', older_than => INTERVAL '7 days') c;
```

**Time estimate:** 20 minutes  
**Risk:** Low — compression is transparent to queries; decompress/recompress is non-destructive

#### Step 1.8: Recreate `cml_data_1h` Aggregate with `user_id`

> **Already implemented:** `init.sql` already creates `cml_data_1h` with a refresh policy, and the webserver/Grafana already use it. However, `user_id` is not in the `GROUP BY`, so RLS policies cannot be applied to it. It must be dropped and recreated.

```sql
-- database/migrations/005_update_aggregate_user_id.sql

-- Drop existing view (CASCADE removes dependent policies/grants)
DROP MATERIALIZED VIEW cml_data_1h CASCADE;

-- Recreate with user_id in GROUP BY
CREATE MATERIALIZED VIEW cml_data_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    user_id,
    cml_id,
    sublink_id,
    MIN(rsl)  AS rsl_min,
    MAX(rsl)  AS rsl_max,
    AVG(rsl)  AS rsl_avg,
    MIN(tsl)  AS tsl_min,
    MAX(tsl)  AS tsl_max,
    AVG(tsl)  AS tsl_avg
FROM cml_data
GROUP BY bucket, user_id, cml_id, sublink_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('cml_data_1h',
    start_offset      => INTERVAL '2 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Grant read access (per-user RLS policies are added in migration 003)
GRANT SELECT ON cml_data_1h TO webserver_role;
```

**Note:** The view is recreated `WITH NO DATA` and backfilled by the refresh policy within an hour. The webserver and Grafana queries are unchanged — they already filter by `cml_id`; after PR 2 they will also be filtered by RLS on `user_id` automatically.

**Time estimate:** 15 minutes  
**Risk:** Low — brief gap in Grafana historical data while backfill runs (~1 refresh cycle)

The remaining steps from the original plan (tiered storage, memory model overview, Grafana query tuning, compression benchmarks) were informational and are already addressed by the existing `init.sql` implementation. There is no further action required for them in this PR.

<!-- Steps 1.9–1.12 of the original plan (tiered storage, memory model, Grafana aggregate queries, compression tests) are removed — compression and 1h aggregates are already fully operational in init.sql. The only action needed is Steps 1.7–1.8 above to add user_id support to both. -->

**Phase 1 Total:** ~2 hours + testing

---

### Phase 2: SFTP Multi-User Setup (Priority: HIGH)

**Objective:** Configure shared SFTP server to support multiple users with isolated directories

#### Step 2.1: Generate SSH Keys for Users
```bash
#!/bin/bash
# scripts/generate_user_keys.sh

for user in user1 user2 user3; do
    mkdir -p ssh_keys/${user}
    
    # Generate ED25519 key pair (more secure than RSA)
    ssh-keygen -t ed25519 \
        -f ssh_keys/${user}/id_ed25519 \
        -N "" \
        -C "${user}@gmdi"
    
    # Create authorized_keys file
    cp ssh_keys/${user}/id_ed25519.pub ssh_keys/${user}/authorized_keys
    
    echo "Generated keys for ${user}"
done

echo "✓ SSH keys generated for all users"
```

**Time estimate:** 10 minutes  
**Risk:** Low

#### Step 2.2: Update SFTP Entrypoint Script
```bash
# sftp_receiver/entrypoint.sh
#!/bin/bash
set -e

# Create upload directories for each user
mkdir -p /home/user1/uploads /home/user2/uploads /home/user3/uploads

# Set ownership (match user IDs defined below)
chown -R 1001:1001 /home/user1/uploads
chown -R 1002:1002 /home/user2/uploads
chown -R 1003:1003 /home/user3/uploads

# Execute SFTP server with multiple users
# Format: username::UID:GID:upload_directory
# Empty password field means SSH key authentication only
exec /entrypoint \
    user1::1001:1001:uploads \
    user2::1002:1002:uploads \
    user3::1003:1003:uploads
```

**Time estimate:** 15 minutes  
**Risk:** Low

#### Step 2.3: Update Docker Compose for SFTP
```yaml
# docker-compose.yml - SFTP section
services:
  sftp_receiver:
    build: ./sftp_receiver
    ports:
      - "2222:22"
    volumes:
      # SFTP server host keys (shared)
      - ./ssh_keys/sftp_host_ed25519_key:/etc/ssh/ssh_host_ed25519_key:ro
      - ./ssh_keys/sftp_host_rsa_key:/etc/ssh/ssh_host_rsa_key:ro
      
      # Per-user authorized_keys (for SSH key authentication)
      - ./ssh_keys/user1/authorized_keys:/home/user1/.ssh/keys/authorized_keys:ro
      - ./ssh_keys/user2/authorized_keys:/home/user2/.ssh/keys/authorized_keys:ro
      - ./ssh_keys/user3/authorized_keys:/home/user3/.ssh/keys/authorized_keys:ro
      
      # Per-user upload directories (separate volumes for isolation)
      - sftp_user1_uploads:/home/user1/uploads
      - sftp_user2_uploads:/home/user2/uploads
      - sftp_user3_uploads:/home/user3/uploads

volumes:
  sftp_user1_uploads:
  sftp_user2_uploads:
  sftp_user3_uploads:
```

**Time estimate:** 20 minutes  
**Risk:** Low

#### Step 2.4: Test SFTP Access
```bash
#!/bin/bash
# scripts/test_sftp_access.sh

echo "Testing SFTP access for all users..."

for user in user1 user2 user3; do
    echo "Testing ${user}..."
    
    # Test SFTP connection
    sftp -P 2222 -i ssh_keys/${user}/id_ed25519 \
        -o StrictHostKeyChecking=no \
        ${user}@localhost <<EOF
cd uploads
pwd
ls
exit
EOF
    
    if [ $? -eq 0 ]; then
        echo "✓ ${user} SFTP access working"
    else
        echo "✗ ${user} SFTP access failed"
    fi
done

echo "SFTP tests complete!"
```

**Time estimate:** 20 minutes  
**Risk:** Low

**Phase 2 Total:** ~1 hour + testing

---

### Phase 3: Web and API Upload Methods (Priority: HIGH)

**Objective:** Enable HTTP API and web drag-and-drop uploads in addition to SFTP.

**Design decision:** No separate ingestion coordinator service is needed. Both upload methods save files directly into the user's per-user incoming directory — the same Docker volume the per-user parser already watches via `FileWatcher`. The existing file-watching pipeline picks them up automatically.

~~#### Step 3.1: Create Ingestion Coordinator Service~~

> **Removed.** The ingestion coordinator described in the original plan is unnecessary overhead. Each per-user parser already watches its own incoming volume; web/API uploads write directly to that volume. No HTTP trigger endpoint or coordinator container is needed.
#### Step 3.2: Add API Upload Endpoint to Webserver

```python
# webserver/main.py - Add API upload endpoint

from flask import request, jsonify
from werkzeug.utils import secure_filename
import os
from pathlib import Path

# API key management (simple approach - can be enhanced)
API_KEYS = {
    'user1': os.getenv('USER1_API_KEY', 'user1_api_key_change_me'),
    'user2': os.getenv('USER2_API_KEY', 'user2_api_key_change_me'),
    'user3': os.getenv('USER3_API_KEY', 'user3_api_key_change_me'),
}

def authenticate_api_key():
    """Authenticate request by API key"""
    api_key = request.headers.get('X-API-Key')
    if not api_key:
        return None
    
    for user_id, key in API_KEYS.items():
        if api_key == key:
            return user_id
    
    return None

@app.route("/api/upload", methods=['POST'])
def api_upload():
    """API endpoint for file uploads via HTTP POST"""
    
    # Authenticate
    user_id = authenticate_api_key()
    if not user_id:
        return jsonify({"error": "Invalid or missing API key"}), 401
    
    # Check file in request
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Empty filename"}), 400
    
    # Validate file extension
    allowed_extensions = {'.csv', '.nc', '.txt'}
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in allowed_extensions:
        return jsonify({"error": f"Invalid file type. Allowed: {allowed_extensions}"}), 400
    
    # Save to web uploads directory
    filename = secure_filename(file.filename)
    upload_dir = Path(os.getenv('WEB_UPLOADS_DIR', '/app/uploads/web')) / user_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = upload_dir / filename
    file.save(str(file_path))
    
    logger.info(f"API upload from {user_id}: {filename} ({file_path.stat().st_size} bytes)")
    
    return jsonify({
        "success": True,
        "filename": filename,
        "size": file_path.stat().st_size,
        "user_id": user_id,
        "upload_method": "api"
    }), 201
```

**Time estimate:** 1.5 hours  
**Risk:** Low

#### Step 3.3: Add Drag-and-Drop Upload to Webserver

```html
<!-- webserver/templates/data-uploads.html - Enhanced with drag-and-drop -->
<div class="upload-container">
    <h2>Upload Data Files</h2>
    
    <div id="drop-zone" class="drop-zone">
        <p>Drag & drop files here or click to browse</p>
        <input type="file" id="file-input" multiple accept=".csv,.nc,.txt" style="display:none">
    </div>
    
    <div id="upload-progress" style="display:none">
        <h3>Uploading...</h3>
        <div class="progress-bar">
            <div id="progress-fill" class="progress-fill"></div>
        </div>
        <p id="progress-text">0%</p>
    </div>
    
    <div id="upload-results"></div>
</div>

<script>
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('file-input');
const progressDiv = document.getElementById('upload-progress');
const progressFill = document.getElementById('progress-fill');
const progressText = document.getElementById('progress-text');
const resultsDiv = document.getElementById('upload-results');

// Click to browse
dropZone.addEventListener('click', () => fileInput.click());

// Drag and drop handlers
dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropZone.classList.add('drag-over');
});

dropZone.addEventListener('dragleave', () => {
    dropZone.classList.remove('drag-over');
});

dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    handleFiles(e.dataTransfer.files);
});

fileInput.addEventListener('change', (e) => {
    handleFiles(e.target.files);
});

function handleFiles(files) {
    if (files.length === 0) return;
    
    progressDiv.style.display = 'block';
    resultsDiv.innerHTML = '';
    
    const formData = new FormData();
    for (let file of files) {
        formData.append('file', file);
    }
    
    // Upload via form-based endpoint (uses session auth)
    fetch('/upload', {
        method: 'POST',
        body: formData,
    })
    .then(response => response.json())
    .then(data => {
        progressDiv.style.display = 'none';
        if (data.success) {
            resultsDiv.innerHTML = `<div class="success">✓ Uploaded ${data.filename} (${formatBytes(data.size)})</div>`;
        } else {
            resultsDiv.innerHTML = `<div class="error">✗ Upload failed: ${data.error}</div>`;
        }
    })
    .catch(error => {
        progressDiv.style.display = 'none';
        resultsDiv.innerHTML = `<div class="error">✗ Upload failed: ${error}</div>`;
    });
}

function formatBytes(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024*1024) return (bytes/1024).toFixed(1) + ' KB';
    return (bytes/(1024*1024)).toFixed(1) + ' MB';
}
</script>
```

```python
# webserver/main.py - Add form-based upload endpoint

@app.route("/upload", methods=['POST'])
@login_required
def web_upload():
    """Web form upload endpoint (uses session authentication)"""
    
    user_id = current_user.id
    
    # Check file in request
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Empty filename"}), 400
    
    # Validate file extension
    allowed_extensions = {'.csv', '.nc', '.txt'}
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in allowed_extensions:
        return jsonify({"error": f"Invalid file type. Allowed: {allowed_extensions}"}), 400
    
    # Save to web uploads directory
    filename = secure_filename(file.filename)
    upload_dir = Path(os.getenv('WEB_UPLOADS_DIR', '/app/uploads/web')) / user_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = upload_dir / filename
    file.save(str(file_path))
    
    logger.info(f"Web upload from {user_id}: {filename} ({file_path.stat().st_size} bytes)")
    
    return jsonify({
        "success": True,
        "filename": filename,
        "size": file_path.stat().st_size,
        "user_id": user_id,
        "upload_method": "web"
    }), 201
```

**Time estimate:** 2 hours  
**Risk:** Low

#### Step 3.4: Test Web and API Uploads

```bash
#!/bin/bash
# scripts/test_multi_upload.sh

echo "Testing multi-method data ingestion..."

# Test 1: SFTP upload (existing method)
echo "\nTest 1: SFTP upload"
echo "time,cml_id,sublink_id,rsl,tsl" > /tmp/test_sftp.csv
echo "2026-02-27 12:00:00,CML_SFTP,A,50.5,55.2" >> /tmp/test_sftp.csv

sftp -P 2222 -i ssh_keys/user1/id_ed25519 user1@localhost <<EOF
cd uploads
put /tmp/test_sftp.csv
exit
EOF

# Test 2: HTTP API upload
echo "\nTest 2: HTTP API upload"
echo "time,cml_id,sublink_id,rsl,tsl" > /tmp/test_api.csv
echo "2026-02-27 12:00:00,CML_API,A,51.5,56.2" >> /tmp/test_api.csv

curl -X POST http://localhost:5000/api/upload \
  -H "X-API-Key: user1_api_key_change_me" \
  -F "file=@/tmp/test_api.csv"

# Test 3: Web form upload (requires login)
echo "\nTest 3: Web form upload (manual test required)"
echo "Visit: http://localhost:5000/data-uploads"
echo "Login and drag-and-drop test_web.csv"

# Wait for processing
echo "\nWaiting for processing..."
sleep 15

# Verify data in database
echo "\nVerifying data in database:"
docker exec gmdi_prototype-database-1 psql -U myuser -d mydatabase -c \
  "SELECT cml_id, user_id, COUNT(*) FROM cml_data WHERE cml_id LIKE 'CML_%' GROUP BY cml_id, user_id;"

echo "\nMulti-method ingestion test complete!"
```

**Time estimate:** 1 hour  
**Risk:** Low

**Phase 3 Total:** ~4 hours + testing

---

### Phase 4: Parser Modifications (Priority: HIGH)

**Objective:** Modify parser to add user_id to data and deploy per-user instances

#### Step 3.1: Modify DBWriter Class
```python
# parser/db_writer.py

class DBWriter:
    def __init__(self, db_url: str, user_id: str = None, connect_timeout: int = 10):
        self.db_url = db_url
        self.user_id = user_id  # NEW: User identifier
        self.connect_timeout = connect_timeout
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.max_retries = 3
        self.retry_backoff_seconds = 2

    def write_metadata(self, df: pd.DataFrame) -> int:
        """Write metadata DataFrame to database."""
        if df.empty:
            logger.warning("Empty metadata DataFrame, skipping write")
            return 0
        
        # Add user_id if configured
        if self.user_id:
            df = df.copy()
            df['user_id'] = self.user_id
        
        # Ensure required columns
        required_cols = ['cml_id', 'sublink_id', 'user_id'] if self.user_id else ['cml_id', 'sublink_id']
        # ... rest of existing logic

    def write_rawdata(self, df: pd.DataFrame) -> int:
        """Write raw data DataFrame to database."""
        if df.empty:
            logger.warning("Empty raw data DataFrame, skipping write")
            return 0
        
        # Add user_id if configured
        if self.user_id:
            df = df.copy()
            df['user_id'] = self.user_id
        
        # Ensure required columns
        required_cols = ['time', 'cml_id', 'sublink_id', 'user_id'] if self.user_id else ['time', 'cml_id', 'sublink_id']
        # ... rest of existing logic
```

**Time estimate:** 1 hour  
**Risk:** Medium (core functionality, needs testing)

#### Step 3.2: Update Parser Main Entry Point
```python
# parser/main.py

class Config:
    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://myuser:mypassword@database:5432/mydatabase"
    )
    USER_ID = os.getenv("USER_ID")  # NEW: User identifier from environment
    INCOMING_DIR = Path(os.getenv("PARSER_INCOMING_DIR", "data/incoming"))
    ARCHIVED_DIR = Path(os.getenv("PARSER_ARCHIVED_DIR", "data/archived"))
    QUARANTINE_DIR = Path(os.getenv("PARSER_QUARANTINE_DIR", "data/quarantine"))
    PARSER_ENABLED = os.getenv("PARSER_ENABLED", "True").lower() in ("1", "true", "yes")
    PROCESS_EXISTING_ON_STARTUP = os.getenv(
        "PROCESS_EXISTING_ON_STARTUP", "True"
    ).lower() in ("1", "true", "yes")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

def main():
    setup_logging()
    logger = logging.getLogger("parser.service")
    
    file_manager = FileManager(
        str(Config.INCOMING_DIR),
        str(Config.ARCHIVED_DIR),
        str(Config.QUARANTINE_DIR),
    )
    
    # Pass user_id to DBWriter
    db_writer = DBWriter(Config.DATABASE_URL, user_id=Config.USER_ID)
    
    logger.info(f"Starting parser service for user: {Config.USER_ID or 'default'}")
    # ... rest of existing logic
```

**Time estimate:** 30 minutes  
**Risk:** Low

#### Step 3.3: Create Per-User Parser Services in Docker Compose
```yaml
# docker-compose.yml - Parser services

services:
  # User 1 Parser
  parser_user1:
    build: ./parser
    depends_on:
      - database
    environment:
      - DATABASE_URL=postgresql://user1_role:secure_password_1@database:5432/mydatabase
      - USER_ID=user1
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
      - PARSER_ENABLED=true
      - PROCESS_EXISTING_ON_STARTUP=true
      - LOG_LEVEL=INFO
    volumes:
      - sftp_user1_uploads:/app/data/incoming
      - parser_user1_archived:/app/data/archived
      - parser_user1_quarantine:/app/data/quarantine
      # Optional: user-specific config
      # - ./configs/user1/parser_config.yml:/app/config.yml:ro

  # User 2 Parser
  parser_user2:
    build: ./parser  # Or ./parser_user2 for custom parser
    depends_on:
      - database
    environment:
      - DATABASE_URL=postgresql://user2_role:secure_password_2@database:5432/mydatabase
      - USER_ID=user2
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
      - PARSER_ENABLED=true
      - PROCESS_EXISTING_ON_STARTUP=true
      - LOG_LEVEL=INFO
    volumes:
      - sftp_user2_uploads:/app/data/incoming
      - parser_user2_archived:/app/data/archived
      - parser_user2_quarantine:/app/data/quarantine

  # User 3 Parser
  parser_user3:
    build: ./parser
    depends_on:
      - database
    environment:
      - DATABASE_URL=postgresql://user3_role:secure_password_3@database:5432/mydatabase
      - USER_ID=user3
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
      - PARSER_ENABLED=true
      - PROCESS_EXISTING_ON_STARTUP=true
      - LOG_LEVEL=INFO
    volumes:
      - sftp_user3_uploads:/app/data/incoming
      - parser_user3_archived:/app/data/archived
      - parser_user3_quarantine:/app/data/quarantine

volumes:
  # Parser volumes
  parser_user1_archived:
  parser_user1_quarantine:
  parser_user2_archived:
  parser_user2_quarantine:
  parser_user3_archived:
  parser_user3_quarantine:
```

**Time estimate:** 1 hour  
**Risk:** Low

#### Step 3.4: Test Parser with user_id
```bash
#!/bin/bash
# scripts/test_parser_user_id.sh

echo "Testing parser user_id insertion..."

# Upload test file as user1
echo "time,cml_id,sublink_id,rsl,tsl" > /tmp/test_data.csv
echo "2026-02-26 12:00:00,CML001,A,50.5,55.2" >> /tmp/test_data.csv

sftp -P 2222 -i ssh_keys/user1/id_ed25519 user1@localhost <<EOF
cd uploads
put /tmp/test_data.csv
exit
EOF

# Wait for parser to process
sleep 10

# Check if data has correct user_id
docker exec gmdi_prototype-database-1 psql -U myuser -d mydatabase -c \
  "SELECT cml_id, user_id FROM cml_data WHERE cml_id = 'CML001';"

echo "Parser test complete!"
```

**Time estimate:** 30 minutes  
**Risk:** Low

**Phase 4 Total:** ~3 hours + testing

---

### Phase 5: Webserver Authentication (Priority: MEDIUM)

**Objective:** Add login system and role-based database access to webserver

#### Step 4.1: Add Dependencies
```toml
# webserver/requirements.txt (additions)
flask-login==0.6.3
werkzeug==3.0.1  # For password hashing
```

```bash
# Rebuild webserver image
docker compose build webserver
```

**Time estimate:** 10 minutes  
**Risk:** Low

#### Step 4.2: Create User Configuration File
```json
// configs/users.json
{
  "user1": {
    "password_hash": "$2b$12$...",  // Use bcrypt to hash password
    "db_role": "user1_role",
    "db_password": "secure_password_1",
    "display_name": "User 1"
  },
  "user2": {
    "password_hash": "$2b$12$...",
    "db_role": "user2_role",
    "db_password": "secure_password_2",
    "display_name": "User 2"
  },
  "user3": {
    "password_hash": "$2b$12$...",
    "db_role": "user3_role",
    "db_password": "secure_password_3",
    "display_name": "User 3"
  }
}
```

```python
# scripts/hash_password.py - Helper to generate password hashes
from werkzeug.security import generate_password_hash

password = input("Enter password: ")
hash = generate_password_hash(password, method='pbkdf2:sha256')
print(f"Hash: {hash}")
```

**Time estimate:** 20 minutes  
**Risk:** Low

#### Step 4.3: Implement Authentication in Webserver
```python
# webserver/auth.py - New file for authentication logic

from flask_login import UserMixin
from werkzeug.security import check_password_hash
import json
import os

class User(UserMixin):
    def __init__(self, user_id, config):
        self.id = user_id
        self.db_role = config['db_role']
        self.db_password = config['db_password']
        self.display_name = config.get('display_name', user_id)

def load_users():
    """Load user configuration from JSON file"""
    config_path = os.getenv('USERS_CONFIG_PATH', '/app/users.json')
    if not os.path.exists(config_path):
        return {}
    
    with open(config_path, 'r') as f:
        return json.load(f)

USERS_CONFIG = load_users()

def authenticate_user(username, password):
    """Authenticate user by username and password"""
    if username not in USERS_CONFIG:
        return None
    
    user_config = USERS_CONFIG[username]
    if check_password_hash(user_config['password_hash'], password):
        return User(username, user_config)
    
    return None

def get_user(user_id):
    """Get user by ID"""
    if user_id in USERS_CONFIG:
        return User(user_id, USERS_CONFIG[user_id])
    return None
```

**Time estimate:** 30 minutes  
**Risk:** Low

#### Step 4.4: Update Webserver Main File
```python
# webserver/main.py - Add authentication (key changes)

from flask import Flask, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from auth import authenticate_user, get_user
import psycopg2
import os

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'change-me-in-production')

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    return get_user(user_id)

def get_db_connection():
    """Get DB connection with current user's role set"""
    if not current_user.is_authenticated:
        # Not logged in - redirect to login or use guest access
        return None
    
    # Build connection URL with user's role credentials
    db_host = os.getenv('DB_HOST', 'database')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'mydatabase')
    
    db_url = f"postgresql://{current_user.db_role}:{current_user.db_password}@{db_host}:{db_port}/{db_name}"
    
    conn = psycopg2.connect(db_url)
    return conn

@app.route("/login", methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('overview'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = authenticate_user(username, password)
        if user:
            login_user(user)
            next_page = request.args.get('next')
            return redirect(next_page or url_for('overview'))
        else:
            flash('Invalid username or password', 'error')
    
    return render_template('login.html')

@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash('You have been logged out', 'info')
    return redirect(url_for('login'))

@app.route("/")
@login_required
def overview():
    """Landing page with overview - automatically filtered to user's data via RLS"""
    stats = {
        "total_cmls": 0,
        "total_records": 0,
        "data_start_date": None,
        "data_end_date": None,
        "username": current_user.display_name,
    }

    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()

            # These queries automatically filtered by RLS to current user's data
            cur.execute("SELECT COUNT(DISTINCT cml_id) FROM cml_metadata")
            stats["total_cmls"] = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM cml_data")
            stats["total_records"] = cur.fetchone()[0]

            # Get date range
            cur.execute("SELECT MIN(time), MAX(time) FROM cml_data")
            row = cur.fetchone()
            if row[0]:
                stats["data_start_date"] = row[0].strftime("%Y-%m-%d")
                stats["data_end_date"] = row[1].strftime("%Y-%m-%d")

            cur.close()
            conn.close()
    except Exception as e:
        print(f"Database error: {e}")
        flash('Error loading data', 'error')

    return render_template("overview.html", stats=stats)

# All other routes remain the same - they automatically use get_db_connection()
# which returns a connection with RLS-filtered access
```

**Time estimate:** 2 hours  
**Risk:** Medium (affects all routes)

#### Step 4.5: Create Login Template
```html
<!-- webserver/templates/login.html -->
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - GMDI</title>
    <link rel="stylesheet" href="{{ url_for('static', filename='style.css') }}">
    <style>
        .login-container {
            max-width: 400px;
            margin: 100px auto;
            padding: 30px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .login-container h1 {
            text-align: center;
            margin-bottom: 30px;
        }
        .form-group {
            margin-bottom: 20px;
        }
        .form-group label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }
        .form-group input {
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 16px;
        }
        .btn-login {
            width: 100%;
            padding: 12px;
            background: #007bff;
            color: white;
            border: none;
            border-radius: 4px;
            font-size: 16px;
            cursor: pointer;
        }
        .btn-login:hover {
            background: #0056b3;
        }
        .alert {
            padding: 10px;
            margin-bottom: 20px;
            border-radius: 4px;
        }
        .alert-error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
    </style>
</head>
<body>
    <div class="login-container">
        <h1>GMDI Login</h1>
        
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <div class="alert alert-{{ category }}">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        
        <form method="POST" action="{{ url_for('login') }}">
            <div class="form-group">
                <label for="username">Username</label>
                <input type="text" id="username" name="username" required autofocus>
            </div>
            
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" required>
            </div>
            
            <button type="submit" class="btn-login">Log In</button>
        </form>
    </div>
</body>
</html>
```

**Time estimate:** 30 minutes  
**Risk:** Low

#### Step 4.6: Update Base Template with Logout
```html
<!-- webserver/templates/base.html - Add logout button -->
<nav>
    <!-- existing nav items -->
    {% if current_user.is_authenticated %}
        <span>Logged in as: {{ current_user.display_name }}</span>
        <a href="{{ url_for('logout') }}">Logout</a>
    {% endif %}
</nav>
```

**Time estimate:** 15 minutes  
**Risk:** Low

#### Step 4.7: Update Docker Compose for Webserver
```yaml
# docker-compose.yml - Webserver section
services:
  webserver:
    build: ./webserver
    ports:
      - "5000:5000"
    depends_on:
      - database
    environment:
      - SECRET_KEY=your-secret-key-change-in-production
      - DB_HOST=database
      - DB_PORT=5432
      - DB_NAME=mydatabase
      - USERS_CONFIG_PATH=/app/users.json
      - STORAGE_BACKEND=local
      - STORAGE_BASE_PATH=/app/data
    volumes:
      - webserver_data_staged:/app/data/staged
      - webserver_data_archived:/app/data/archived
      - ./configs/users.json:/app/users.json:ro  # Mount user config
```

**Time estimate:** 15 minutes  
**Risk:** Low

#### Step 4.8: Test Authentication and RLS
```bash
#!/bin/bash
# scripts/test_webserver_auth.sh

echo "Testing webserver authentication..."

# Test 1: Login as user1
echo "Test 1: Login as user1"
curl -c cookies.txt -X POST http://localhost:5000/login \
    -d "username=user1&password=user1_password"

# Test 2: Access dashboard (should see only user1 data)
echo "Test 2: Access dashboard as user1"
curl -b cookies.txt http://localhost:5000/ | grep "total_cmls"

# Test 3: Logout
echo "Test 3: Logout"
curl -b cookies.txt http://localhost:5000/logout

# Test 4: Access without login (should redirect)
echo "Test 4: Access without login"
curl -L http://localhost:5000/ | grep "Login"

rm cookies.txt
echo "Authentication tests complete!"
```

**Time estimate:** 30 minutes  
**Risk:** Low

**Phase 5 Total:** ~4.5 hours + testing

---

### Phase 6: User Onboarding Automation (Priority: LOW)

**Objective:** Create scripts and templates to simplify adding new users

#### Step 5.1: Create User Onboarding Script Template
```bash
#!/bin/bash
# scripts/add_user.sh - Complete user onboarding script

set -e

if [ -z "$1" ]; then
    echo "Usage: ./add_user.sh <username>"
    exit 1
fi

USER_NAME=$1
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/.."

# Calculate next available UID
LAST_UID=$(grep -oP 'user\d+::\K\d+' "$PROJECT_ROOT/sftp_receiver/entrypoint.sh" | sort -n | tail -1)
USER_ID=$((LAST_UID + 1))

echo "================================================"
echo "Adding new user: $USER_NAME"
echo "Assigned UID: $USER_ID"
echo "================================================"

# Step 1: Generate SSH keys
echo ""
echo "[1/6] Generating SSH keys..."
mkdir -p "$PROJECT_ROOT/ssh_keys/$USER_NAME"
ssh-keygen -t ed25519 \
    -f "$PROJECT_ROOT/ssh_keys/$USER_NAME/id_ed25519" \
    -N "" \
    -C "${USER_NAME}@gmdi" \
    -q
cp "$PROJECT_ROOT/ssh_keys/$USER_NAME/id_ed25519.pub" \
   "$PROJECT_ROOT/ssh_keys/$USER_NAME/authorized_keys"
echo "✓ SSH keys generated"

# Step 2: Generate secure passwords
echo ""
echo "[2/6] Generating secure passwords..."
DB_PASSWORD=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-25)
WEB_PASSWORD=$(openssl rand -base64 16 | tr -d "=+/" | cut -c1-12)
echo "✓ Passwords generated"
echo "   DB Password: $DB_PASSWORD"
echo "   Web Password: $WEB_PASSWORD"

# Step 3: Update SFTP entrypoint
echo ""
echo "[3/6] Updating SFTP configuration..."
# Add user to entrypoint.sh before the 'exec' line
sed -i.bak "/^exec \/entrypoint/i\\    ${USER_NAME}::${USER_ID}:${USER_ID}:uploads \\\\" \
    "$PROJECT_ROOT/sftp_receiver/entrypoint.sh"
echo "✓ SFTP configuration updated"

# Step 4: Create database role and RLS policies
echo ""
echo "[4/6] Creating database role and RLS policies..."
docker exec -i $(docker ps -qf "name=database") psql -U myuser -d mydatabase <<SQL
-- Create role
CREATE ROLE ${USER_NAME}_role LOGIN PASSWORD '$DB_PASSWORD';

-- Create RLS policies for cml_data
CREATE POLICY ${USER_NAME}_data_policy ON cml_data
    FOR ALL TO ${USER_NAME}_role
    USING (user_id = '${USER_NAME}')
    WITH CHECK (user_id = '${USER_NAME}');

-- Create RLS policies for cml_metadata
CREATE POLICY ${USER_NAME}_metadata_policy ON cml_metadata
    FOR ALL TO ${USER_NAME}_role
    USING (user_id = '${USER_NAME}')
    WITH CHECK (user_id = '${USER_NAME}');

-- Create RLS policies for cml_stats
CREATE POLICY ${USER_NAME}_stats_policy ON cml_stats
    FOR ALL TO ${USER_NAME}_role
    USING (user_id = '${USER_NAME}')
    WITH CHECK (user_id = '${USER_NAME}');

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON cml_data, cml_metadata, cml_stats TO ${USER_NAME}_role;

-- Allow webserver to switch to this role
GRANT ${USER_NAME}_role TO webserver_role;
SQL
echo "✓ Database role and policies created"

# Step 5: Add to users.json
echo ""
echo "[5/6] Adding user to webserver configuration..."
python3 - <<PYTHON
import json
import os
from werkzeug.security import generate_password_hash

users_file = '$PROJECT_ROOT/configs/users.json'

# Load existing users
if os.path.exists(users_file):
    with open(users_file, 'r') as f:
        users = json.load(f)
else:
    users = {}

# Add new user
users['$USER_NAME'] = {
    'password_hash': generate_password_hash('$WEB_PASSWORD', method='pbkdf2:sha256'),
    'db_role': '${USER_NAME}_role',
    'db_password': '$DB_PASSWORD',
    'display_name': 'User ${USER_NAME}'
}

# Save
with open(users_file, 'w') as f:
    json.dump(users, f, indent=2)

print('✓ User added to users.json')
PYTHON

# Step 6: Generate docker-compose snippet
echo ""
echo "[6/6] Generating docker-compose configuration..."
cat > "/tmp/${USER_NAME}_docker_snippet.yml" <<YAML
  # Add to services section:
  parser_${USER_NAME}:
    build: ./parser
    depends_on:
      - database
    environment:
      - DATABASE_URL=postgresql://${USER_NAME}_role:${DB_PASSWORD}@database:5432/mydatabase
      - USER_ID=${USER_NAME}
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
      - PARSER_ENABLED=true
      - PROCESS_EXISTING_ON_STARTUP=true
      - LOG_LEVEL=INFO
    volumes:
      - sftp_${USER_NAME}_uploads:/app/data/incoming
      - parser_${USER_NAME}_archived:/app/data/archived
      - parser_${USER_NAME}_quarantine:/app/data/quarantine

  # Add to sftp_receiver volumes:
      - ./ssh_keys/${USER_NAME}/authorized_keys:/home/${USER_NAME}/.ssh/keys/authorized_keys:ro
      - sftp_${USER_NAME}_uploads:/home/${USER_NAME}/uploads

  # Add to volumes section:
  sftp_${USER_NAME}_uploads:
  parser_${USER_NAME}_archived:
  parser_${USER_NAME}_quarantine:
YAML
echo "✓ Docker compose snippet saved to /tmp/${USER_NAME}_docker_snippet.yml"

# Summary
echo ""
echo "================================================"
echo "✓ User $USER_NAME added successfully!"
echo "================================================"
echo ""
echo "📋 Next steps:"
echo ""
echo "1. Manually update docker-compose.yml with the snippet from:"
echo "   /tmp/${USER_NAME}_docker_snippet.yml"
echo ""
echo "2. Restart services:"
echo "   docker compose up -d"
echo ""
echo "3. Provide user with the following:"
echo "   - SSH private key: ssh_keys/${USER_NAME}/id_ed25519"
echo "   - SFTP host: company.com:2222"
echo "   - SFTP username: ${USER_NAME}"
echo "   - Web URL: https://company.com"
echo "   - Web username: ${USER_NAME}"
echo "   - Web password: ${WEB_PASSWORD}"
echo ""
echo "================================================"
```

**Time estimate:** 2 hours  
**Risk:** Low

#### Step 5.2: Create Docker Compose Template Generator
```python
# scripts/generate_compose_snippet.py
import sys

def generate_user_compose(username, user_id, db_password):
    """Generate docker-compose snippet for new user"""
    
    snippet = f"""
  # Parser for {username}
  parser_{username}:
    build: ./parser
    depends_on:
      - database
    environment:
      - DATABASE_URL=postgresql://{username}_role:{db_password}@database:5432/mydatabase
      - USER_ID={username}
      - PARSER_INCOMING_DIR=/app/data/incoming
      - PARSER_ARCHIVED_DIR=/app/data/archived
      - PARSER_QUARANTINE_DIR=/app/data/quarantine
      - PARSER_ENABLED=true
      - PROCESS_EXISTING_ON_STARTUP=true
      - LOG_LEVEL=INFO
    volumes:
      - sftp_{username}_uploads:/app/data/incoming
      - parser_{username}_archived:/app/data/archived
      - parser_{username}_quarantine:/app/data/quarantine

# SFTP volume additions (add to sftp_receiver service):
      - ./ssh_keys/{username}/authorized_keys:/home/{username}/.ssh/keys/authorized_keys:ro
      - sftp_{username}_uploads:/home/{username}/uploads

# Volume definitions (add to volumes section):
  sftp_{username}_uploads:
  parser_{username}_archived:
  parser_{username}_quarantine:
"""
    
    return snippet

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: python generate_compose_snippet.py <username> <uid> <db_password>")
        sys.exit(1)
    
    username = sys.argv[1]
    user_id = sys.argv[2]
    db_password = sys.argv[3]
    
    print(generate_user_compose(username, user_id, db_password))
```

**Time estimate:** 1 hour  
**Risk:** Low

#### Step 5.3: Create Documentation
Create comprehensive documentation in `docs/user-onboarding.md` with:
- Step-by-step manual process
- Automated script usage
- Troubleshooting guide
- User credential delivery template

**Time estimate:** 2 hours  
**Risk:** Low

**Phase 6 Total:** ~5 hours

---

## Testing Strategy

### Unit Tests
- Parser: Test user_id insertion in DBWriter
- Webserver: Test authentication logic
- Database: Test RLS policies

### Integration Tests
- End-to-end user flow: SFTP upload → Parser → Database → Webserver
- Multi-user isolation: Verify users cannot see each other's data
- Authentication: Test login/logout flows

### Security Tests
- SQL injection attempts with RLS
- Cross-user data access attempts
- SFTP chroot escape attempts

### Performance Tests
- RLS overhead measurement (compare queries with/without RLS)
- Multi-user concurrent load testing
- Database query performance with user_id indexes

## Rollback Plan

### Phase Rollback Procedures

**Phase 1 (Database):**
```bash
# Restore from backup
docker exec -i gmdi_prototype-database-1 psql -U myuser -d mydatabase < backup_pre_multiuser.sql
```

**Phase 2-4 (Code changes):**
```bash
# Revert code changes
git reset --hard <commit_before_changes>

# Rebuild and restart
docker compose down
docker compose build
docker compose up -d
```

## Timeline Summary

| Phase | Description | Estimated Time |
|-------|-------------|----------------|
| PR | Phase | Description | Notes |
|----|-------|-------------|-------|
| 1 | Phase 1 (partial) | DB schema: `user_id` columns + migration | No behaviour change |
| 2 | Phase 1 (partial) | DB roles + RLS policies | No behaviour change |
| 3 | Phase 4 | Parser: `user_id` injection | Backward-compatible |
| 4 | Phase 2 | SFTP multi-user + docker-compose | Backward-compatible |
| 5 | Phase 5 | Webserver auth | Breaking: login required |
| 6 | Phase 3 | Web/API upload endpoints | Additive |
| 7 | Phase 6 | Onboarding scripts + docs | Additive |

## Success Criteria

- ✅ Multiple users can upload via SFTP with separate SSH keys
- ✅ Users can upload via HTTP API with API key authentication
- ✅ Users can upload via web interface with drag-and-drop
- ✅ Each user's data is isolated at database level (RLS verified)
- ✅ Users can log into webserver and see only their data
- ✅ No cross-user data leakage (verified by security tests)
- ✅ Single external URL for all users
- ✅ **Database RAM ≤ 3 GB** for 10 users (with compression + aggregates)
- ✅ **Total system RAM ≤ 5 GB** for 10 users
- ✅ Historical queries use aggregates (10-100× faster than raw data)
- ✅ Recent data (30 days) accessible in < 100ms
- ✅ Compression ratio ≥ 5× for data older than 30 days
- ✅ User onboarding takes < 30 minutes with automation script

## Future Enhancements

1. **Enhanced Ingestion System**
   - Message queue (Redis/RabbitMQ) for better reliability
   - Retry mechanism for failed uploads
   - Upload progress tracking and notifications
   - Webhook support for upload completion events
   - S3/MinIO support for large file uploads

2. **Grafana Multi-User Support**
   - Per-user data sources
   - User-specific dashboards
   - RBAC configuration

3. **Advanced SFTP Features**
   - Upload quotas per user
   - Rate limiting
   - Audit logging

4. **Admin Dashboard**
   - User management UI
   - Resource usage monitoring
   - Automated user provisioning

5. **Data Archiving**
   - Automated cold storage for old data
   - Per-user archive policies
   - Compressed archive tables

6. **High Availability**
   - PostgreSQL replication
   - Load balancing for webserver
   - SFTP failover

---

## Appendix: Configuration Repository Structure

For the separate private config repository:

```
gmdi-configs/  (private repo)
├── README.md
├── users.json                    # User authentication config
├── .env.production               # Production environment variables
├── user1/
│   ├── authorized_keys           # SFTP SSH keys
│   ├── parser_config.yml         # Parser-specific config
│   └── credentials.txt           # User credentials (encrypted)
├── user2/
│   ├── authorized_keys
│   ├── parser_config.yml
│   └── credentials.txt
├── user3/
│   ├── authorized_keys
│   ├── parser_config.yml
│   └── credentials.txt
└── scripts/
    ├── deploy.sh                 # Deployment script
    └── sync_to_production.sh     # Config sync script
```

This separation keeps sensitive credentials out of the main codebase while maintaining version control and audit trails.
