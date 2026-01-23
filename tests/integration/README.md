# Integration Tests

This directory contains end-to-end integration tests for the GMDI prototype.

## Test Files

- `test_e2e_sftp_pipeline.py` - Complete SFTP data pipeline validation including parser and database integration

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
- `database` - PostgreSQL database for parsed data
- `parser` - Parser service to process uploaded files
- `mno_simulator` - (Optional) For testing live uploads

## Running Tests

### Run integration tests using Docker Compose (recommended):
```bash
# Ensure services are running
docker compose up -d sftp_receiver webserver mno_simulator database parser

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

The integration tests validate different aspects of the data pipeline. Tests fall into three categories:
1. **Infrastructure tests** - Validate service connectivity and configuration
2. **Pipeline flow tests** - Validate data movement through the system
3. **Data integrity tests** - Validate data persistence and correctness

**Key Design Decision:** Since the parser processes files immediately upon upload, tests cannot rely on checking for files in the SFTP directory. Instead, **pipeline tests validate successful processing by checking the database** - if data exists in the database with correct structure and integrity, the entire pipeline (MNO→SFTP→Parser→Database) must be working.

### Test 1: SFTP Server Accessibility
**Type:** Infrastructure test  
**Purpose:** Verifies SFTP server accepts SSH key authentication  
**What it checks:**
- SFTP server is running and accessible
- SSH key authentication works
- Connection can be established

**Debugging:** If fails, check SFTP service status and SSH key configuration

---

### Test 2: Upload Directory Writable
**Type:** Infrastructure test  
**Purpose:** Confirms SFTP uploads directory has correct permissions  
**What it checks:**
- Write permissions on `/uploads` directory
- File creation succeeds
- File cleanup works

**Debugging:** If fails, check Docker volume permissions and SFTP user configuration

---

### Test 3: MNO Simulator Upload & Parser Processing
**Type:** Pipeline flow test  
**Purpose:** Validates MNO simulator generates data and parser processes it into the database  
**What it checks:**
- Database contains data rows (proof of successful upload→parse→DB flow)
- Database contains metadata rows (expected ~728 with composite key schema: 2 sublinks per CML)
- Data timestamps are recent (sanity check)

**Note:** This validates the **full upload-to-database flow** by checking the end result (data in DB) rather than intermediate steps.

**Debugging:** 
- If no data: Check MNO simulator is running: `docker compose ps mno_simulator`
- If no data: Check parser is running: `docker compose ps parser`
- Query database directly: `docker compose exec database psql -U myuser -d mydatabase -c "SELECT COUNT(*) FROM cml_data;"`
- Check parser logs: `docker compose logs parser | grep -E "ERROR|Quarantined"`

---

### Test 4: Webserver File Access
**Type:** Infrastructure test (skipped in Docker)  
**Purpose:** Verifies webserver can read files from SFTP uploads directory  
**What it checks:**
- Webserver has access to shared volume
- File reading works via Docker exec

**Note:** Only runs when tests execute outside Docker container (local development)

**Debugging:** Check volume mount configuration in `docker-compose.yml`

---

### Test 5: Full MNO → SFTP → Parser → Database Pipeline
**Type:** Pipeline flow test  
**Purpose:** Validates complete data flow from source to database with integrity checks  
**What it checks:**
- Database contains both data and metadata
- All data records have corresponding metadata (referential integrity using composite key: cml_id + sublink_id)
- No orphaned records exist

**Note:** This test validates **data integrity** across the full pipeline.

**Debugging:**
- Check data/metadata counts in test output
- Verify referential integrity: `docker compose exec database psql -U myuser -d mydatabase`
  ```sql
  SELECT COUNT(*) FROM cml_data r
  WHERE NOT EXISTS (
    SELECT 1 FROM cml_metadata m 
    WHERE m.cml_id = r.cml_id AND m.sublink_id = r.sublink_id
  );
  ```
- Check for parser errors: `docker compose logs parser | grep ERROR`

---

### Test 6: Storage Backend Configuration
**Type:** Infrastructure test (skipped in Docker)  
**Purpose:** Checks webserver storage backend environment variables  
**What it checks:**
- Storage type is configured
- Configuration values are set correctly

**Note:** Only runs when tests execute outside Docker container

---

### Test 7: Parser Database Integration
**Type:** Data integrity test  
**Purpose:** Validates parser writes correct data to PostgreSQL database  
**What it checks:**
1. **Table existence:** `cml_metadata` and `cml_data` tables exist
2. **Data presence:** Both tables contain records
3. **Data structure:** Sample queries validate column structure
4. **Referential integrity:** All `(cml_id, sublink_id)` pairs in data table have metadata (composite key)
5. **Data correctness:** TSL/RSL values are numeric, timestamps are valid

**Note:** This is the **end-to-end validation** - if this passes, data successfully flowed from MNO → SFTP → Parser → Database.

**Debugging:**
- Test output shows table names and row counts
- Check database directly: `docker compose exec database psql -U myuser -d mydatabase`
- Query tables: `SELECT COUNT(*) FROM cml_metadata;` and `SELECT COUNT(*) FROM cml_data;`
- Check for errors: `docker compose logs parser | grep -E "ERROR|Failed"`

---

## Test Execution Flow

The tests are designed to run sequentially, building on each other:

1. **Tests 1-2** validate SFTP infrastructure is working
2. **Test 3** validates MNO→SFTP→Parser data flow  
3. **Test 5** validates Parser successfully processes files
4. **Test 7** validates Parser→Database data persistence

If Test 7 passes, the entire pipeline is confirmed working end-to-end.

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
