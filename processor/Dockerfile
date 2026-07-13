FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY *.py ./
COPY workflows/ ./workflows/

# Create directories for config and state
RUN mkdir -p /app/config /app/data/state

# Set environment variables
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]