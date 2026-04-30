This monorepo contains the following components:
1. **Data Parser** - Parses CML data and metadata CSV files from SFTP uploads into the database
2. **Database** - TimescaleDB for storing time series data and metadata
3. **Data Processor** - **(Stub implementation)** Placeholder for future data analysis and processing logic
4. **Webserver** - Main user-facing web application with interactive visualizations
5. **Grafana** - Real-time dashboards for CML data visualization
6. **MNO Data Source Simulator** - Simulates real-time CML data from MNO sources via SFTP
7. **SFTP Receiver** - Receives uploaded CML data files

## Webserver Pages

The webserver provides an intuitive interface with four main pages:

- **Landing Page** (`/`) - System overview with data statistics and processing status
- **Real-Time Data** (`/realtime`) - Interactive CML network map with Grafana-embedded time series plots
- **Archive** (`/archive`) - Long-term archive statistics and data distribution analysis
- **Data Uploads** (`/data-uploads`) - File upload interface for CML data files

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Git

### Setup

1. Clone the repository:
   ```sh
   git clone https://github.com/OpenSenseAction/GMDI_prototype.git
   cd GMDI_prototype
   ```

2. Generate SSH keys for SFTP server:
   ```sh
   cd ssh_keys
   ./generate_ssh_keys.sh
   cd ..
   ```

3. Build and run the containers:
   ```sh
   docker compose up -d
   ```

4. Access the services:
   
   - **Webserver (Main UI)**: http://localhost:5000
   - **Grafana Dashboards**: http://localhost:3000
   - **Database**: localhost:5432
   - **SFTP Server**: localhost:2222
   
   *Note: The processor service (port 5002) is currently a minimal stub implementation.*

## Data Flow

1. **MNO Simulator** → generates CML data from NetCDF files and uploads via SFTP to **SFTP Receiver**
2. **Parser** → watches SFTP upload directory and processes CSV files (both metadata and data)
3. **Parser** → validates and writes parsed data to **Database** (TimescaleDB)
4. **Webserver** → serves UI and provides API access to database
5. **Grafana** → visualizes real-time data from database with embedded dashboards

## Archive Data

On `docker compose up` the `archive_generator` service automatically generates
a 1-day archive at 10-second resolution from the 3-month OpenMRG NetCDF file
and the `archive_loader` service bulk-loads it into the database.

**Defaults** (overridable via environment variables):
- **728 CML sublinks** (364 unique CML IDs) covering Berlin area
- **~6.3M data rows** at 10-second intervals over 1 day
- Generates in ~15 s, loads in ~15 s

**NetCDF source file** (`openMRG_cmls_20150827_3months.nc`, ~193 MB) is
gitignored. If not present in `parser/example_data/`, it is downloaded
automatically at startup via `NETCDF_FILE_URL`.

### Configuring the archive

```sh
# Longer archive or different resolution via environment variables:
ARCHIVE_DAYS=7 ARCHIVE_INTERVAL_SECONDS=60 docker compose up -d
```

| Variable | Default | Description |
|---|---|---|
| `ARCHIVE_DAYS` | `1` | Days of history to generate |
| `ARCHIVE_INTERVAL_SECONDS` | `10` | Time step in seconds |
| `NETCDF_FILE_URL` | KIT download link | URL to fetch the NetCDF file if absent |

### Reloading archive data

```sh
docker compose down -v  # Remove volumes
docker compose up -d    # Regenerate and reload from scratch
```

### Loading a larger archive directly from NetCDF

For a full 3-month archive at native 10-second resolution (~579M rows):

```sh
docker compose run --rm -e DB_HOST=database parser \
  python /app/parser/parse_netcdf_archive.py
```

Use `ARCHIVE_MAX_DAYS` to limit the time window (default: 7 days,
`0` = no limit). Requires at least 4 GB RAM for the full dataset.

## Storage Backend

The webserver supports multiple storage backends for received files:

- **Local filesystem** (default) - For development and testing
- **MinIO** - S3-compatible object storage (optional)
- **AWS S3** - Production object storage (configure via environment variables)

To use MinIO, uncomment the `minio` service in `docker-compose.yml` and set:
```yaml
environment:
  - STORAGE_BACKEND=minio
  - STORAGE_S3_BUCKET=cml-data
  - STORAGE_S3_ENDPOINT=http://minio:9000
```

## Multi-Tenancy

Each tenant has:
- a PostgreSQL login role whose name **equals** the `user_id` stored in the data tables
- a Grafana organisation (org) with a dedicated datasource connecting as that role
- a Flask login account in `webserver/configs/users.json`

Row-Level Security on `cml_metadata` and `cml_stats`, plus the
`cml_data_1h_secure` security-barrier view, ensure each DB role only reads its
own data without any application-level filtering.

### Adding a new tenant from a deployment repo

The canonical deployment pattern is a **separate git repo** that includes this
repo as a git submodule and overrides configuration with a
`docker-compose.override.yml`.

#### 1. Database — add a migration in the deployment repo

Create a SQL migration file (e.g. `migrations/008_add_acme.sql`):

```sql
-- Idempotent: safe to re-run
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'acme') THEN
        CREATE ROLE acme LOGIN PASSWORD 'change-me-in-production';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO acme;
GRANT SELECT, INSERT, UPDATE ON cml_data     TO acme;
GRANT SELECT, INSERT, UPDATE ON cml_metadata TO acme;
GRANT SELECT, INSERT, UPDATE ON cml_stats    TO acme;
GRANT EXECUTE ON FUNCTION update_cml_stats(TEXT, TEXT) TO acme;
GRANT SELECT ON cml_data_secure    TO acme;
GRANT SELECT ON cml_data_1h_secure TO acme;
GRANT acme TO webserver_role;
```

Apply it to the running database:

```sh
docker compose exec -T database psql -U myuser -d mydatabase \
  < migrations/008_add_acme.sql
```

No new RLS policies are needed; the generic `WHERE user_id = current_user`
policies cover every role automatically.

#### 2. Grafana bootstrap — extend `init_grafana.py` via override

In your deployment repo, create an override that replaces `ORGS` / `USERS` in
`grafana/init_grafana.py`, or mount a patched copy of the file.  The simplest
approach is to extend via environment variables.  Until `init_grafana.py`
supports env-driven tenant lists, the easiest override is to **replace the
script** with a deployment-repo copy that appends the new tenant:

```python
# deployment-repo/grafana/init_grafana.py  (copy of the upstream file + additions)

ORGS = [
    {"id": 1, "name": "demo_openmrg"},
    {"id": 2, "name": "demo_orange_cameroun"},
    {"id": 3, "name": "acme"},          # ← new tenant
]

USERS = [
    {"login": "demo_openmrg",        "org_id": 1, "role": "Viewer"},
    {"login": "demo_orange_cameroun", "org_id": 2, "role": "Viewer"},
    {"login": "acme",                 "org_id": 3, "role": "Viewer"},  # ← new
]
```

And add the datasource + dashboard copy call in `__main__`:

```python
create_datasource_for_org(
    org_id=3,
    name="PostgreSQL",
    uid="ds_acme",
    user="acme",
    password="change-me-in-production",
)
copy_dashboards_to_org(target_org_id=3, source_org_id=1)
```

Mount the patched script via `docker-compose.override.yml`:

```yaml
services:
  init_grafana:
    volumes:
      - ./grafana/init_grafana.py:/app/init_grafana.py:ro
```

#### 3. Webserver users — mount an overridden `users.json`

The deployment repo should provide its own `webserver/configs/users.json`
(already live-mounted, no rebuild needed):

```json
{
    "demo_openmrg":        { "password_hash": "scrypt:...", "display_name": "OpenMRG Demo",      "grafana_org_id": 1 },
    "demo_orange_cameroun":{ "password_hash": "scrypt:...", "display_name": "Orange Cameroun Demo","grafana_org_id": 2 },
    "acme":                { "password_hash": "scrypt:...", "display_name": "Acme Corp",           "grafana_org_id": 3 }
}
```

Generate a password hash with:

```sh
docker compose run --rm webserver python3 -c \
  "from werkzeug.security import generate_password_hash; print(generate_password_hash('your-password'))"
```

#### 4. SFTP keys — mount from the deployment repo

The `sftp_receiver` reads authorised keys from `ssh_keys/authorized_keys` and
per-user key directories.  Add the new tenant's public key there via the
deployment repo's volume mounts in `docker-compose.override.yml`.

#### 5. Apply and restart

```sh
# Apply the DB migration (only needed once per database volume lifetime)
docker compose exec -T database psql -U myuser -d mydatabase \
  < migrations/008_add_acme.sql

# Restart so init_grafana re-runs bootstrap (creates org 3, datasource, copies dashboards)
docker compose restart init_grafana
# Or on a fresh stack: docker compose up -d
```

`init_grafana` is idempotent — re-running it on an existing stack is safe.

