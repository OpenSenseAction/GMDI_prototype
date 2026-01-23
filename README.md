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

