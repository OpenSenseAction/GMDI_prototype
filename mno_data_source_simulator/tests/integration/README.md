# Integration Testing for SFTP Uploader

This directory contains integration tests that verify the SFTP uploader works with a real SFTP server.

## Prerequisites

- Docker and Docker Compose installed
- pytest installed (`pip install pytest`)

## Running Integration Tests

### 1. Start the Test SFTP Server

```bash
cd tests/integration
docker-compose -f docker-compose-test.yml up -d
```

This starts a containerized SFTP server on port 2222 with:
- **Host:** localhost
- **Port:** 2222
- **Username:** test_user
- **Password:** test_password
- **Base directory:** /upload
- **Test creates:** /upload/cml_data (automatically)

### 2. Set Up Host Key Verification (for strict security tests)

```bash
# Add the test server's host key to known_hosts
ssh-keyscan -p 2222 localhost >> /tmp/test_known_hosts

# Set environment variable for tests
export KNOWN_HOSTS_PATH=/tmp/test_known_hosts
```

Alternatively, the tests will use `~/.ssh/known_hosts` if the environment variable is not set.

### 3. Run the Integration Tests

```bash
# From the mno_data_source_simulator directory
pytest tests/integration/ -v -m integration

# Or run all tests including unit tests
pytest tests/ -v
```

### 3. Stop the Test SFTP Server

```bash
cd tests/integration
docker-compose -f docker-compose-test.yml down -v
```

The `-v` flag removes volumes to clean up test data.

## What's Tested

Integration tests verify:
- ✅ Actual SFTP connection establishment with host key verification
- ✅ Real file uploads to SFTP server
- ✅ Multiple file batch uploads
- ✅ File verification on remote server
- ✅ Context manager with real connections
- ✅ Password and SSH key authentication methods

## Troubleshooting

### Connection Refused

If tests fail with "Connection refused":
1. Check if the SFTP server is running: `docker ps | grep sftp_test_server`
2. Wait a few seconds after starting the container for SSH to initialize
3. Check logs: `docker-compose -f docker-compose-test.yml logs`

### Permission Errors

The SFTP uploader automatically creates subdirectories (e.g., `/upload/cml_data`). The docker-compose configuration uses tmpfs with proper ownership to ensure write permissions.

## Manual Testing

You can also manually test the SFTP server:

```bash
# Using sftp command-line client
sftp -P 2222 test_user@localhost

# Using Python paramiko (from Python shell)
import paramiko
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('localhost', port=2222, username='test_user', password='test_password')
sftp = client.open_sftp()
sftp.listdir('/upload')
```
