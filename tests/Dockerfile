FROM python:3.11-slim

WORKDIR /workspace

# Install test dependencies
COPY tests/requirements.txt /workspace/tests/requirements.txt
RUN pip install --no-cache-dir -r tests/requirements.txt

# Copy test configuration and files
COPY pyproject.toml /workspace/
COPY tests/ /workspace/tests/

# Copy SSH keys (needed for SFTP connection tests)
COPY ssh_keys/ /workspace/ssh_keys/

# Set working directory
WORKDIR /workspace

# Default command runs integration tests
CMD ["pytest", "tests/integration/", "-v", "-m", "integration"]
