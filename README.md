This monorepo contains the following components:
1. Data Parser
2. Database (TimescaleDB and Metadata)
3. Data Processor
4. Data Visualization (Leaflet)

## Getting Started
1. Clone the repository:
   ```sh
   git clone https://github.com/yourusername/repo_name.git
   cd repo_name
   ```

2. Build and run the containers:

    ```sh
    docker-compose up -d
    ```

3. Access the services:
    
    - Parser: http://localhost:5000
    - Metadata Parser: http://localhost:5001
    - Processor: http://localhost:5002
    - Visualization: http://localhost:5003

