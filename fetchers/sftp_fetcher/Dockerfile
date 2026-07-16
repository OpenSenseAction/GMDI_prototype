FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY fetchers/sftp_fetcher/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy shared library
COPY fetchers/shared /app/fetchers/shared

# Copy sftp_fetcher code
COPY fetchers/sftp_fetcher /app/fetchers/sftp_fetcher

# Create directories for state and incoming files
RUN mkdir -p /app/state /app/incoming

# Set Python path to include /app
ENV PYTHONPATH=/app

# Default command (override in docker-compose.yml)
CMD ["python", "-m", "fetchers.sftp_fetcher.main", "--config", "/app/config.yml", "--source", "default"]
