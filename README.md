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

The database can be initialized with archive CML data using two methods:

### Method 1: CSV Files (Default, Fast)

Pre-generated CSV files included in the repository:
- **728 CML sublinks** (364 unique CML IDs) covering Berlin area
- **~1.5M data rows** at 5-minute intervals over 7 days
- **Gzip-compressed** (~7.6 MB total, included in repo)
- **Loads in ~3 seconds** via PostgreSQL COPY

Files are located in `/database/archive_data/` and loaded automatically on first database startup.

### Method 2: Load from NetCDF (For Larger/Higher Resolution Archives)

Load data directly from the full 3-month NetCDF archive with configurable time range:

#### Default: 7 Days at 10-Second Resolution (~44M rows, ~5 minutes)

```sh
# Rebuild parser if needed
docker compose build parser

# Start database
docker compose up -d database

# Load last 7 days from NetCDF
docker compose run --rm -e DB_HOST=database parser python /app/parser/parse_netcdf_archive.py
```

#### Custom Time Range

Use `ARCHIVE_MAX_DAYS` to control how much data to load:

```sh
# Load last 14 days (~88M rows, ~10 minutes)
docker compose run --rm -e DB_HOST=database -e ARCHIVE_MAX_DAYS=14 parser python /app/parser/parse_netcdf_archive.py

# Load full 3 months (~579M rows, ~1 hour)
docker compose run --rm -e DB_HOST=database -e ARCHIVE_MAX_DAYS=0 parser python /app/parser/parse_netcdf_archive.py
```

**Note**: Set `ARCHIVE_MAX_DAYS=0` to disable the time limit and load the entire dataset. Larger datasets require more database memory (recommend at least 4GB RAM for full 3-month archive).

**Features**:
- Auto-downloads 3-month NetCDF file (~209 MB) on first run
- **10-second resolution** (vs 5-minute for CSV method)
- **Automatic timestamp shifting** - data ends at current time
- **Progress reporting** with batch-by-batch status (~155K rows/sec)
- PostgreSQL COPY for maximum performance
- Configurable time window to balance demo realism vs load time

The NetCDF file is downloaded to `parser/example_data/openMRG_cmls_20150827_3months.nc` and gitignored.

### Managing Archive Data

To regenerate CSV archive data:
```sh
python mno_data_source_simulator/generate_archive.py
```

To reload archive data (either method):
```sh
docker compose down -v  # Remove volumes
docker compose up -d    # Restart with fresh database
```

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

