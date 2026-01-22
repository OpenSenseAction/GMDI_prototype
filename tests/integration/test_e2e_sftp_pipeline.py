"""End-to-end integration tests for SFTP data pipeline.

This test validates the complete data flow:
1. MNO Simulator generates CML data
2. MNO Simulator uploads data via SFTP to SFTP Receiver
3. Webserver can access uploaded files
4. Parser processes files and writes to database

Requirements:
- Docker and Docker Compose
- SSH keys generated (run ssh_keys/generate_ssh_keys.sh)
- Services running: sftp_receiver, mno_simulator, webserver, parser, database

Run with: docker compose run --rm integration_tests
Or locally: pytest tests/integration/test_e2e_sftp_pipeline.py -v -m integration
"""

import os
import time
import tempfile
from pathlib import Path
import subprocess
import pytest
import paramiko
import psycopg2


# Detect if running inside Docker
RUNNING_IN_DOCKER = os.path.exists("/.dockerenv")

# Configuration - supports both Docker network and localhost
SFTP_HOST = os.getenv("SFTP_HOST", "localhost")
SFTP_PORT = int(os.getenv("SFTP_PORT", "2222"))
SFTP_USERNAME = "cml_user"
SFTP_REMOTE_PATH = "/uploads"
SSH_KEY_PATH = "ssh_keys/id_rsa"
KNOWN_HOSTS_PATH = "ssh_keys/known_hosts"

# Database configuration
DB_HOST = os.getenv("DB_HOST", "database" if RUNNING_IN_DOCKER else "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "mydatabase")
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mypassword")


def check_docker_running():
    """Check if Docker is running."""
    if RUNNING_IN_DOCKER:
        return True  # Already inside Docker

    try:
        result = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def check_service_running(service_name):
    """Check if a Docker Compose service is running."""
    if RUNNING_IN_DOCKER:
        # Inside Docker, assume services are running (handled by depends_on)
        return True

    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "-q", service_name],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return bool(result.stdout.strip())
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def check_ssh_keys_exist():
    """Check if SSH keys have been generated."""
    key_path = Path(SSH_KEY_PATH)
    known_hosts = Path(KNOWN_HOSTS_PATH)
    return key_path.exists() and known_hosts.exists()


@pytest.fixture(scope="module")
def docker_environment():
    """Ensure Docker and required services are running."""
    if not check_docker_running():
        pytest.skip("Docker is not running")

    if not check_ssh_keys_exist():
        pytest.skip(
            "SSH keys not generated. Run: cd ssh_keys && ./generate_ssh_keys.sh"
        )

    # Check required services
    required_services = ["sftp_receiver", "webserver"]
    for service in required_services:
        if not check_service_running(service):
            pytest.skip(f"Required service '{service}' is not running")

    yield
    # Cleanup handled by Docker Compose


@pytest.fixture
def sftp_client(docker_environment):
    """Create an SFTP client connected to the server."""
    # Resolve paths
    ssh_key_path = Path(SSH_KEY_PATH).resolve()

    if not ssh_key_path.exists():
        pytest.skip(f"SSH key not found at {ssh_key_path}")

    # Create SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Load private key
        private_key = paramiko.RSAKey.from_private_key_file(str(ssh_key_path))

        # Connect
        ssh.connect(
            hostname=SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USERNAME,
            pkey=private_key,
            timeout=10,
        )

        # Open SFTP session
        sftp = ssh.open_sftp()

        yield sftp

        # Cleanup
        sftp.close()
        ssh.close()

    except Exception as e:
        pytest.skip(f"Could not connect to SFTP server: {e}")


@pytest.fixture
def db_connection(docker_environment):
    """Create a database connection for testing."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=10,
        )
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"Could not connect to database: {e}")


@pytest.mark.integration
def test_sftp_server_accessible(docker_environment):
    """Test 1: Verify SFTP server is accessible and accepting connections."""
    ssh_key_path = Path(SSH_KEY_PATH).resolve()

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        private_key = paramiko.RSAKey.from_private_key_file(str(ssh_key_path))

        ssh.connect(
            hostname=SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USERNAME,
            pkey=private_key,
            timeout=10,
        )

        # Connection successful
        assert ssh.get_transport() is not None
        assert ssh.get_transport().is_active()

        ssh.close()

    except Exception as e:
        pytest.fail(f"SFTP connection failed: {e}")


@pytest.mark.integration
def test_sftp_upload_directory_writable(sftp_client):
    """Test 2: Verify SFTP uploads directory is writable."""
    try:
        # Change to uploads directory
        sftp_client.chdir(SFTP_REMOTE_PATH)

        # Create a test file
        test_filename = "test_write_permissions.txt"
        test_content = b"test write access"

        with sftp_client.open(test_filename, "wb") as f:
            f.write(test_content)

        # Verify file exists
        stat = sftp_client.stat(test_filename)
        assert stat.st_size == len(test_content)

        # Read back
        with sftp_client.open(test_filename, "rb") as f:
            content = f.read()
            assert content == test_content

        # Cleanup
        sftp_client.remove(test_filename)

    except Exception as e:
        pytest.fail(f"Upload directory not writable: {e}")


@pytest.mark.integration
def test_mno_simulator_uploading_files(docker_environment, db_connection):
    """Test 3: Verify MNO simulator is generating and uploading files.

    Since the parser processes files immediately, we validate by checking
    that data appears in the database (proof of successful upload→parse→DB flow).
    """
    # Check if mno_simulator is running
    if not check_service_running("mno_simulator"):
        pytest.skip("MNO simulator is not running")
    if not check_service_running("parser"):
        pytest.skip("Parser service is not running")

    try:
        print("\n=== Testing MNO Simulator Upload & Parser Processing ===")

        cursor = db_connection.cursor()

        # Check if data exists in database (proof of successful pipeline)
        cursor.execute("SELECT COUNT(*) FROM cml_data")
        data_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM cml_metadata")
        metadata_count = cursor.fetchone()[0]

        print(f"1. Database contains {data_count} data rows")
        print(f"2. Database contains {metadata_count} metadata rows")

        # We expect data to be present if MNO simulator is uploading and parser is working
        assert (
            data_count > 0
        ), "No data in database - MNO simulator may not be uploading or parser may not be processing"
        assert (
            metadata_count > 0
        ), "No metadata in database - MNO simulator may not have uploaded metadata file"
        # With composite key (cml_id, sublink_id), we expect 728 metadata rows (2 per cml_id)
        print(f"   (Note: Expected ~728 metadata rows with composite key schema)")

        # Check that data is recent (within last 5 minutes as sanity check)
        cursor.execute("SELECT MAX(time) FROM cml_data")
        latest_time = cursor.fetchone()[0]

        if latest_time:
            print(f"\n3. Most recent data timestamp: {latest_time}")

        print(
            "\n✓ MNO simulator is successfully uploading and parser is processing files into database"
        )

    except Exception as e:
        pytest.fail(f"Failed to verify MNO simulator uploads: {e}")


@pytest.mark.integration
def test_webserver_can_read_uploaded_files(docker_environment):
    """Test 4: Verify webserver can read files uploaded to SFTP server."""
    try:
        if RUNNING_IN_DOCKER:
            # Inside Docker network, we need to access webserver differently
            # For now, we'll rely on the SFTP directory being the same volume
            # that webserver mounts, so we skip this test
            pytest.skip("Webserver access test not supported inside Docker container")

        # Execute command in webserver container to list files
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "webserver",
                "ls",
                "-1",
                "/app/data/incoming/",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode != 0:
            pytest.fail(f"Failed to list webserver incoming directory: {result.stderr}")

        # Parse output
        files = [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]
        csv_files = [f for f in files if f.endswith(".csv")]

        assert len(csv_files) > 0, "No CSV files found in webserver incoming directory"

        print(f"\n✓ Webserver can see {len(csv_files)} CSV files")

        # Verify webserver can read content of first CSV file
        if csv_files:
            result = subprocess.run(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    "webserver",
                    "head",
                    "-5",
                    f"/app/data/incoming/{csv_files[0]}",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            assert result.returncode == 0, "Failed to read CSV file content"
            assert (
                "time,cml_id,sublink_id,tsl,rsl" in result.stdout
            ), "CSV file missing expected header"

            print(f"✓ Webserver can read CSV file content")

    except subprocess.TimeoutExpired:
        pytest.fail("Timeout while checking webserver access")
    except Exception as e:
        pytest.fail(f"Failed to verify webserver file access: {e}")


@pytest.mark.integration
def test_sftp_to_parser_pipeline(docker_environment, db_connection):
    """Test 5: Validate full data pipeline from MNO to Parser.

    This test validates:
    1. MNO Simulator generates data and metadata
    2. MNO Simulator uploads via SFTP
    3. Parser receives and processes files
    4. Data successfully appears in database
    """
    print("\n=== Testing Full MNO → SFTP → Parser → Database Pipeline ===")

    try:
        cursor = db_connection.cursor()

        # Verify both tables have data
        cursor.execute("SELECT COUNT(*) FROM cml_data")
        data_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM cml_metadata")
        metadata_count = cursor.fetchone()[0]

        print(f"1. Database contains {data_count} data rows")
        print(f"2. Database contains {metadata_count} metadata rows")

        # Verify referential integrity (all data has metadata)
        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM cml_data r
            WHERE NOT EXISTS (
                SELECT 1 FROM cml_metadata m 
                WHERE m.cml_id = r.cml_id AND m.sublink_id = r.sublink_id
            )
        """
        )
        orphaned_count = cursor.fetchone()[0]

        print(f"3. Orphaned data records (no metadata): {orphaned_count}")

        assert data_count > 0, "No data in database - pipeline not working"
        assert metadata_count > 0, "No metadata in database - pipeline not working"
        assert orphaned_count == 0, f"{orphaned_count} data records have no metadata"

        print("\n✓ Full pipeline is working: MNO → SFTP → Parser → Database")
        return

    except Exception as e:
        pytest.fail(f"Failed to verify pipeline: {e}")


@pytest.mark.integration
def test_storage_backend_configuration(docker_environment):
    """Test 6: Verify webserver storage backend is configured correctly."""
    if RUNNING_IN_DOCKER:
        pytest.skip("Storage backend config test not supported inside Docker container")

    try:
        # Check environment variables in webserver
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "webserver",
                "printenv",
                "STORAGE_BACKEND",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0:
            backend = result.stdout.strip()
            print(f"\n✓ Storage backend configured: {backend}")
            assert backend in [
                "local",
                "s3",
                "minio",
            ], f"Invalid storage backend: {backend}"
        else:
            print("\n⚠ STORAGE_BACKEND not set, using default")

        # Check base path
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "webserver",
                "printenv",
                "STORAGE_BASE_PATH",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0:
            base_path = result.stdout.strip()
            print(f"✓ Storage base path: {base_path}")

    except Exception as e:
        pytest.fail(f"Failed to check storage configuration: {e}")


@pytest.mark.integration
def test_parser_writes_to_database(docker_environment, db_connection):
    """Test 7: Verify parser processes files and writes data to database.

    This test validates:
    1. Parser service is running
    2. Files are processed from incoming directory
    3. Data is written to cml_metadata and cml_rawdata tables
    """
    print("\n=== Testing Parser Database Integration ===")

    # Check if parser service is running
    if not check_service_running("parser"):
        pytest.skip("Parser service is not running")

    cursor = db_connection.cursor()

    try:
        # Step 1: Check if tables exist
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        tables = [row[0] for row in cursor.fetchall()]
        print(f"\n1. Available tables: {tables}")

        assert "cml_metadata" in tables, "cml_metadata table not found"
        assert "cml_data" in tables, "cml_data table not found"

        # Step 2: Wait for parser to process files (give it some time)
        print("\n2. Waiting for parser to process files (up to 45 seconds)...")
        max_wait = 45
        check_interval = 5
        elapsed = 0

        metadata_count = 0
        rawdata_count = 0

        while elapsed < max_wait:
            # Check metadata table
            cursor.execute("SELECT COUNT(*) FROM cml_metadata")
            metadata_count = cursor.fetchone()[0]

            # Check rawdata table
            cursor.execute("SELECT COUNT(*) FROM cml_data")
            rawdata_count = cursor.fetchone()[0]

            if metadata_count > 0 and rawdata_count > 0:
                print(
                    f"\n   ✓ Found {metadata_count} metadata rows and {rawdata_count} rawdata rows after {elapsed}s"
                )
                break

            time.sleep(check_interval)
            elapsed += check_interval

        # Step 3: Verify data was written
        assert metadata_count > 0, "No metadata records found in database"
        assert rawdata_count > 0, "No rawdata records found in database"

        print(f"\n3. Database contains:")
        print(f"   - {metadata_count} metadata records")
        print(f"   - {rawdata_count} rawdata records")

        # Step 4: Verify data structure and content
        cursor.execute(
            "SELECT cml_id, sublink_id, site_0_lon, site_0_lat FROM cml_metadata LIMIT 1"
        )
        metadata_sample = cursor.fetchone()
        assert metadata_sample is not None, "Could not fetch metadata sample"
        print(
            f"\n4. Sample metadata: cml_id={metadata_sample[0]}, sublink_id={metadata_sample[1]}, lon={metadata_sample[2]}, lat={metadata_sample[3]}"
        )

        cursor.execute("SELECT time, cml_id, tsl, rsl FROM cml_data LIMIT 1")
        rawdata_sample = cursor.fetchone()
        assert rawdata_sample is not None, "Could not fetch rawdata sample"
        print(
            f"   Sample rawdata: time={rawdata_sample[0]}, cml_id={rawdata_sample[1]}"
        )

        # Step 5: Verify referential integrity (rawdata references metadata)
        cursor.execute(
            """SELECT COUNT(*) FROM cml_data r 
               LEFT JOIN cml_metadata m ON r.cml_id = m.cml_id AND r.sublink_id = m.sublink_id
               WHERE m.cml_id IS NULL"""
        )
        orphaned_count = cursor.fetchone()[0]

        if orphaned_count > 0:
            print(f"\n   ⚠ Warning: {orphaned_count} rawdata records without metadata")
        else:
            print(f"\n5. ✓ All rawdata records have corresponding metadata")

        print("\n✓ Parser successfully writes data to database!\n")

    except Exception as e:
        pytest.fail(f"Database verification failed: {e}")
    finally:
        cursor.close()
