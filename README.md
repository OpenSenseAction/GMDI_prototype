This monorepo contains the following components:
1. **Data Parser** - Parses CML data and metadata CSV files from SFTP uploads into the database
2. **Database** - TimescaleDB for storing time series data and metadata
3. **Data Processor** - **(Stub implementation)** Placeholder for future data analysis and processing logic
4. **Webserver** - Main user-facing web application with interactive visualizations (runs on gunicorn WSGI server)
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

3. Regenerate user-specific config from `users.yml` (only needed after editing `users.yml`):
   ```sh
   docker run --rm -v "$PWD:/repo" python:3.11-slim \
     sh -c 'pip install pyyaml -q && python /repo/scripts/generate_config.py --repo-root /repo'
   ```

4. **(Optional) Configure production database credentials**:  
   For production deployments, override the default database superuser credentials via environment variables. Create a `.env` file:
   ```sh
   DB_SUPERUSER=myuser
   DB_SUPERUSER_PASSWORD=your-secure-password-here
   ```
   **Important**: The defaults (`myuser:mypassword`) are for local development only and must be changed in production.

5. Build and run the containers:
   ```sh
   docker compose up -d
   ```

6. Access the services:
   
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
- a Grafana organisation with a dedicated datasource connecting as that role
- a Flask login account in `webserver/configs/users.json`

Row-Level Security on `cml_metadata` and `cml_stats`, plus the
`cml_data_secure` / `cml_data_1h_secure` security-barrier views, ensure each
DB role only reads its own data without any application-level filtering.

### `users.yml` — single source of truth

All user configuration lives in [`users.yml`](users.yml).  After editing it,
run the generator to regenerate every derived artifact:

```sh
docker run --rm -v "$PWD:/repo" python:3.11-slim \
  sh -c 'pip install pyyaml -q && python /repo/scripts/generate_config.py --repo-root /repo'
```

The generator writes (or updates) these files — **do not edit them by hand**:

| File | What it contains |
|------|------------------|
| `docker-compose.override.yml` | One parser service + named volume per source; SFTP key mounts and `command` |
| `sftp_receiver/entrypoint.sh` | `mkdir`/`chown` per user and source subdir |
| `webserver/configs/users.json` | Password-hash skeleton for new users (existing hashes are preserved) |
| `grafana/provisioning/datasources/postgres.yml` | One PostgreSQL datasource per user/org |
| `grafana/init_grafana.py` | `ORGS` and `USERS` lists |
| `database/migrations/NNN_add_<id>.sql` | `CREATE ROLE` + `GRANT` statements (only for genuinely new users) |
| `ssh_keys/<id>/` | ED25519 key pair (generated if the directory does not exist) |

### Adding a new user

```sh
# 1. Add an entry to users.yml (edit by hand)

# 2. Run the generator
docker run --rm -v "$PWD:/repo" python:3.11-slim \
  sh -c 'pip install pyyaml -q && python /repo/scripts/generate_config.py --repo-root /repo'

# 3. Set the webserver login password
python scripts/set_password.py <user_id>

# 4. Apply the new SQL migration
docker compose exec -T database psql -U myuser -d mydatabase \
  < database/migrations/NNN_add_<user_id>.sql

# 5. Commit everything
git add users.yml docker-compose.override.yml sftp_receiver/entrypoint.sh \
    webserver/configs/users.json database/migrations/ \
    grafana/provisioning/datasources/postgres.yml grafana/init_grafana.py \
    ssh_keys/<user_id>/
git commit -m "onboard user <user_id>"

# 6. Redeploy affected services
docker compose up -d --no-deps sftp_receiver parser_<user_id>_<source_id> grafana
```

No new RLS policies are needed — the generic `WHERE user_id = current_user`
policy covers every role automatically.

### `users.yml` schema

```yaml
users:
  - id: acme               # PostgreSQL role = SFTP username = user_id in data rows
    uid: 1003              # Linux UID inside the SFTP container (must be unique)
    display_name: Acme Corp
    grafana_org_id: 3      # Grafana organisation number (1-based)
    sources:
      - id: berlin         # Subdir: /home/acme/uploads/berlin/
        parser: openmrg    # Parser image/config to use (openmrg or orange_cameroun)
```

A user with multiple independent CML subnetworks gets one parser per source;
all sources share the same `user_id` so Grafana sees their data unified.

