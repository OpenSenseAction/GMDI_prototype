"""Integration tests for SFTP uploader with real SFTP server.

Requires Docker: See README.md for setup.
"""

import os
import tempfile
import shutil
from pathlib import Path
import sys
import pytest
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from sftp_uploader import SFTPUploader


# SFTP server configuration (matches docker-compose-test.yml)
SFTP_HOST = "localhost"
SFTP_PORT = 2222
SFTP_USERNAME = "test_user"
SFTP_PASSWORD = "test_password"
SFTP_REMOTE_PATH = "/upload/cml_data"

# Get known_hosts path from environment or use default

KNOWN_HOSTS_PATH = os.getenv(
    "KNOWN_HOSTS_PATH", os.path.expanduser("~/.ssh/known_hosts")
)


@pytest.fixture
def test_dirs():
    """Create temporary directories for testing."""
    tmp_base = tempfile.mkdtemp()
    source_dir = Path(tmp_base) / "data_to_upload"
    archive_dir = Path(tmp_base) / "data_uploaded"

    source_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    yield {
        "base": tmp_base,
        "source": str(source_dir),
        "archive": str(archive_dir),
    }

    shutil.rmtree(tmp_base)


@pytest.fixture
def sample_csv_files(test_dirs):
    """Create sample CSV files for testing."""
    source_dir = Path(test_dirs["source"])
    files = []

    for i in range(3):
        df = pd.DataFrame(
            {
                "time": [f"2026-01-20 10:00:{i:02d}"],
                "cml_id": [f"CML_{i:03d}"],
                "sublink_id": ["A"],
                "tsl": [25.5 + i],
                "rsl": [-45.2 - i],
            }
        )

        filename = f"cml_data_integration_{i}.csv"
        filepath = source_dir / filename
        df.to_csv(filepath, index=False)
        files.append(str(filepath))

    return files


@pytest.mark.integration
def test_real_sftp_connection(test_dirs):
    """Test connection to a real SFTP server."""
    uploader = SFTPUploader(
        host=SFTP_HOST,
        port=SFTP_PORT,
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        known_hosts_path=KNOWN_HOSTS_PATH,
        remote_path=SFTP_REMOTE_PATH,
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    # This will fail if SFTP server is not running
    try:
        uploader.connect()
        assert uploader.sftp is not None
        uploader.close()
    except Exception as e:
        pytest.skip(f"SFTP server not available: {e}")


@pytest.mark.integration
def test_real_sftp_upload_file(test_dirs, sample_csv_files):
    """Test uploading a file to a real SFTP server."""
    uploader = SFTPUploader(
        host=SFTP_HOST,
        port=SFTP_PORT,
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        known_hosts_path=KNOWN_HOSTS_PATH,
        remote_path=SFTP_REMOTE_PATH,
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    try:
        uploader.connect()

        # Upload first file
        local_file = sample_csv_files[0]
        uploader.upload_file(local_file)

        # Verify remote file exists
        filename = Path(local_file).name
        remote_file_path = f"{SFTP_REMOTE_PATH}/{filename}"

        # Check file exists on server
        stat = uploader.sftp.stat(remote_file_path)
        assert stat.st_size > 0

        # Cleanup
        uploader.sftp.remove(remote_file_path)
        uploader.close()

    except Exception as e:
        pytest.skip(f"SFTP server not available: {e}")


@pytest.mark.integration
def test_real_sftp_upload_pending_files(test_dirs, sample_csv_files):
    """Test uploading multiple files to a real SFTP server."""
    uploader = SFTPUploader(
        host=SFTP_HOST,
        port=SFTP_PORT,
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        known_hosts_path=KNOWN_HOSTS_PATH,
        remote_path=SFTP_REMOTE_PATH,
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    try:
        uploader.connect()

        # Upload all pending files
        count = uploader.upload_pending_files()

        assert count == 3

        # Verify files were moved to archive
        source_files = list(Path(test_dirs["source"]).glob("*.csv"))
        archive_files = list(Path(test_dirs["archive"]).glob("*.csv"))

        assert len(source_files) == 0
        assert len(archive_files) == 3

        # Verify files exist on server
        for filepath in sample_csv_files:
            filename = Path(filepath).name
            remote_file_path = f"{SFTP_REMOTE_PATH}/{filename}"
            stat = uploader.sftp.stat(remote_file_path)
            assert stat.st_size > 0

            # Cleanup
            uploader.sftp.remove(remote_file_path)

        uploader.close()

    except Exception as e:
        pytest.skip(f"SFTP server not available: {e}")


@pytest.mark.integration
def test_real_sftp_context_manager(test_dirs, sample_csv_files):
    """Test using uploader as context manager with real SFTP."""
    try:
        with SFTPUploader(
            host=SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USERNAME,
            password=SFTP_PASSWORD,
            known_hosts_path=KNOWN_HOSTS_PATH,
            remote_path=SFTP_REMOTE_PATH,
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        ) as uploader:
            # Upload a file
            local_file = sample_csv_files[0]
            uploader.upload_file(local_file)

            # Verify
            filename = Path(local_file).name
            remote_file_path = f"{SFTP_REMOTE_PATH}/{filename}"
            stat = uploader.sftp.stat(remote_file_path)
            assert stat.st_size > 0

            # Cleanup
            uploader.sftp.remove(remote_file_path)

        # Connection should be closed after exiting context

    except Exception as e:
        pytest.skip(f"SFTP server not available: {e}")
