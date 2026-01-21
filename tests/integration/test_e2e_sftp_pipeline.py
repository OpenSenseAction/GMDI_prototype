"""End-to-end integration tests for SFTP data pipeline.

This test validates the complete data flow:
1. MNO Simulator generates CML data
2. MNO Simulator uploads data via SFTP to SFTP Receiver
3. Webserver can access uploaded files

Requirements:
- Docker and Docker Compose
- SSH keys generated (run ssh_keys/generate_ssh_keys.sh)
- Services running: sftp_receiver, mno_simulator, webserver

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


# Configuration - supports both Docker network and localhost
SFTP_HOST = os.getenv("SFTP_HOST", "localhost")
SFTP_PORT = int(os.getenv("SFTP_PORT", "2222"))
SFTP_USERNAME = "cml_user"
SFTP_REMOTE_PATH = "/uploads"
SSH_KEY_PATH = "ssh_keys/id_rsa"
KNOWN_HOSTS_PATH = "ssh_keys/known_hosts"

# Detect if running inside Docker
RUNNING_IN_DOCKER = os.path.exists("/.dockerenv")


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
def test_mno_simulator_uploading_files(docker_environment, sftp_client):
    """Test 3: Verify MNO simulator is uploading files to SFTP server."""
    # Check if mno_simulator is running
    if not check_service_running("mno_simulator"):
        pytest.skip("MNO simulator is not running")

    try:
        # Change to uploads directory
        sftp_client.chdir(SFTP_REMOTE_PATH)

        # List files before
        files_before = set(sftp_client.listdir())
        csv_files_before = [f for f in files_before if f.endswith(".csv")]

        # Wait for at least one upload cycle (60 seconds + buffer)
        # But first check if files already exist
        if len(csv_files_before) > 0:
            # Files already exist, test passes
            assert len(csv_files_before) > 0
            return

        # Wait for new files
        print("\nWaiting up to 90 seconds for MNO simulator to upload files...")
        max_wait = 90
        check_interval = 5
        elapsed = 0

        while elapsed < max_wait:
            time.sleep(check_interval)
            elapsed += check_interval

            files_current = set(sftp_client.listdir())
            csv_files_current = [f for f in files_current if f.endswith(".csv")]

            if len(csv_files_current) > len(csv_files_before):
                print(f"\n✓ Found {len(csv_files_current)} CSV files after {elapsed}s")
                assert len(csv_files_current) > 0
                return

        pytest.fail(
            f"No new CSV files appeared in {max_wait}s. "
            "MNO simulator may not be uploading."
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
def test_e2e_data_flow_complete(docker_environment, sftp_client):
    """Test 5: End-to-end validation of complete data flow.

    This test validates:
    1. MNO Simulator generates data
    2. MNO Simulator uploads via SFTP
    3. Files appear in SFTP server
    4. Webserver can access the files (if not in Docker)
    """
    print("\n=== Testing End-to-End SFTP Data Pipeline ===\n")

    # Step 1: Verify SFTP server has files
    try:
        sftp_client.chdir(SFTP_REMOTE_PATH)
        sftp_files = sftp_client.listdir()
        csv_files_sftp = [f for f in sftp_files if f.endswith(".csv")]

        print(f"1. SFTP server has {len(csv_files_sftp)} CSV files")
        assert len(csv_files_sftp) > 0, "No CSV files on SFTP server"

    except Exception as e:
        pytest.fail(f"Failed to access SFTP server: {e}")

    # Step 2: Verify webserver can see the same files (only if not in Docker)
    if not RUNNING_IN_DOCKER:
        try:
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

            files = [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]
            csv_files_webserver = [f for f in files if f.endswith(".csv")]

            print(f"2. Webserver can see {len(csv_files_webserver)} CSV files")
            assert len(csv_files_webserver) > 0, "Webserver cannot see CSV files"

        except Exception as e:
            pytest.fail(f"Failed to check webserver: {e}")
    else:
        print("2. Webserver access check skipped (running inside Docker)")

    # Step 3: Verify file content is readable
    try:
        test_file = csv_files_sftp[0]

        if not RUNNING_IN_DOCKER:
            result = subprocess.run(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    "webserver",
                    "cat",
                    f"/app/data/incoming/{test_file}",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            assert result.returncode == 0, "Failed to read file"
            assert len(result.stdout) > 0, "File is empty"
            assert "time,cml_id" in result.stdout, "Invalid CSV format"

            print(f"3. Webserver can read file content ({len(result.stdout)} bytes)")
        else:
            # Read via SFTP instead
            with sftp_client.open(test_file, "r") as f:
                content = f.read()
                assert len(content) > 0, "File is empty"
                # Decode if bytes
                if isinstance(content, bytes):
                    content = content.decode("utf-8")
                assert "time,cml_id" in content, "Invalid CSV format"
                print(f"3. File content readable via SFTP ({len(content)} bytes)")

    except Exception as e:
        pytest.fail(f"Failed to read file content: {e}")

    # Step 4: Verify MNO simulator is still running
    if check_service_running("mno_simulator"):
        print("4. MNO simulator is running")
    else:
        print("4. MNO simulator is not running (warning)")

    print("\n✓ End-to-end SFTP pipeline is working correctly!\n")


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
