This monorepo contains the following components:
1. **Data Parser** - Extracts CML measurements from NetCDF datasets
2. **Metadata Processor** - Handles CML network metadata
3. **Database** - TimescaleDB for storing time series data and metadata
4. **Data Processor** - Analyzes and processes stored data
5. **Webserver** - Main user-facing web application with three pages
6. **Visualization** - Low-level visualization and analysis tools (Leaflet)
7. **MNO Data Source Simulator** - Simulates real-time CML data from MNO sources via SFTP

## Webserver Pages

The webserver provides an intuitive interface with three main pages:

- **Landing Page** (`/`) - System overview with data statistics and processing status
- **Real-Time Data** (`/realtime`) - Interactive CML network map and live time series plots
- **Archive** (`/archive`) - Long-term archive statistics and data distribution analysis

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
   - **Metadata Parser**: http://localhost:5001
   - **Processor**: http://localhost:5002
   - **Visualization Tools**: http://localhost:5003
   - **Parser**: http://localhost:5004
   - **Grafana Dashboards**: http://localhost:3000
   - **Database**: localhost:5432
   - **SFTP Server**: localhost:2222

## Data Flow

1. **MNO Simulator** → generates fake CML data from NetCDF files
2. **MNO Simulator** → uploads data via SFTP to **SFTP Receiver**
3. **Webserver** → monitors SFTP uploads directory
4. **Webserver** → processes and stores data in **Database**
5. **Grafana** → visualizes real-time data from database

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

