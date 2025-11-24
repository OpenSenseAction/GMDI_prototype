This monorepo contains the following components:
1. **Data Parser** - Extracts CML measurements from NetCDF datasets
2. **Metadata Processor** - Handles CML network metadata
3. **Database** - TimescaleDB for storing time series data and metadata
4. **Data Processor** - Analyzes and processes stored data
5. **Webserver** - Main user-facing web application with three pages
6. **Visualization** - Low-level visualization and analysis tools (Leaflet)

## Webserver Pages

The webserver provides an intuitive interface with three main pages:

- **Landing Page** (`/`) - System overview with data statistics and processing status
- **Real-Time Data** (`/realtime`) - Interactive CML network map and live time series plots
- **Archive** (`/archive`) - Long-term archive statistics and data distribution analysis

## Getting Started

1. Clone the repository:
   ```sh
   git clone https://github.com/OpenSenseAction/GMDI_prototype.git
   cd GMDI_prototype
   ```

2. Build and run the containers:

    ```sh
    docker-compose up -d
    ```

3. Access the services:
    
    - **Webserver (Main UI)**: http://localhost:5000
    - **Metadata Parser**: http://localhost:5001
    - **Processor**: http://localhost:5002
    - **Visualization Tools**: http://localhost:5003
    - **Parser**: http://localhost:5004
    - **Database**: localhost:5432

