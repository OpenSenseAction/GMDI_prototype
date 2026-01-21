FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY data_generator.py .
COPY sftp_uploader.py .
COPY main.py .
COPY config.yml .

# Create directory for example data
RUN mkdir -p /app/example_data

# Create directories for data management
RUN mkdir -p /app/data_to_upload /app/data_uploaded

# Run the application
CMD ["python", "main.py"]
