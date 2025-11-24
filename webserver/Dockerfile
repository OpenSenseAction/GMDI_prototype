FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p /app/data_incoming /app/data_staged_for_parsing /app/data_archived

# Expose port
EXPOSE 5000

# Run the application
CMD ["python", "main.py"]
