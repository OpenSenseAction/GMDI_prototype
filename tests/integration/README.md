# Integration Tests

This directory contains end-to-end integration tests for the GMDI prototype.

## Test Files

- `test_e2e_sftp_pipeline.py` - Complete SFTP data pipeline validation

## Requirements

### Docker Environment
- Docker and Docker Compose installed and running
- Services started: `docker compose up -d`

### SSH Keys
Generate SSH keys before running tests:
```bash
cd ssh_keys
./generate_ssh_keys.sh
cd ..
```

### Required Services
The following services must be running:
- `sftp_receiver` - SFTP server for receiving data
- `webserver` - Web application with file access
- `mno_simulator` - (Optional) For testing live uploads

## Running Tests

### Run integration tests using Docker Compose (recommended):
```bash
# Ensure services are running
docker compose up -d sftp_receiver webserver mno_simulator

# Run tests in isolated container
docker compose run --rm integration_tests

# Or run specific tests
docker compose run --rm integration_tests pytest tests/integration/test_e2e_sftp_pipeline.py::test_sftp_server_accessible -v
```

### Run tests locally (requires Python environment):
```bash
# Install dependencies
pip install -r tests/requirements.txt

# Run all integration tests
pytest tests/integration/ -v -m integration

# Run specific test file
pytest tests/integration/test_e2e_sftp_pipeline.py -v -m integration

# Run with detailed output
pytest tests/integration/test_e2e_sftp_pipeline.py -v -s -m integration
```

## Test Coverage

### Test 1: SFTP Server Accessibility
Verifies SFTP server accepts SSH key authentication.

### Test 2: Upload Directory Writable
Confirms SFTP uploads directory has correct permissions.

### Test 3: MNO Simulator Uploading
Validates MNO simulator is actively uploading CSV files (requires mno_simulator running).

### Test 4: Webserver File Access
Verifies webserver can read files from SFTP uploads directory.

### Test 5: End-to-End Data Flow
Complete pipeline validation from upload to access.

### Test 6: Storage Backend Configuration
Checks webserver storage backend environment variables.

## Troubleshooting

### "Docker is not running"
Start Docker Desktop or Docker daemon.

### "SSH keys not generated"
Run the key generation script:
```bash
cd ssh_keys && ./generate_ssh_keys.sh
```

### "Required service 'X' is not running"
Start services:
```bash
docker compose up -d sftp_receiver webserver mno_simulator
```

### "Could not connect to SFTP server"
Check service status and logs:
```bash
docker compose ps sftp_receiver
docker compose logs sftp_receiver
```

### "No CSV files found"
Wait for MNO simulator to upload (60 second cycle), or manually upload:
```bash
sftp -P 2222 -i ssh_keys/id_rsa cml_user@localhost
```

## CI/CD Integration

These tests can be integrated into GitHub Actions or other CI/CD pipelines:

```yaml
- name: Generate SSH Keys
  run: cd ssh_keys && ./generate_ssh_keys.sh
  
- name: Start Services
  run: docker compose up -d sftp_receiver webserver mno_simulator
  
- name: Wait for Services
  run: sleep 10
  
- name: Run Integration Tests
  run: docker compose run --rm integration_tests
  
- name: Cleanup
  run: docker compose down
```

Note: The `integration_tests` service uses a Docker profile and won't start automatically with `docker compose up`. It must be explicitly run with `docker compose run`.
