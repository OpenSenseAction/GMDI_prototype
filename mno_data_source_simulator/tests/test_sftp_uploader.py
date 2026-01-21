"""Unit tests for the SFTP uploader module."""

import tempfile
import shutil
from pathlib import Path
import sys
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import paramiko

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from sftp_uploader import SFTPUploader


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

        filename = f"cml_data_2026012010{i:02d}00.csv"
        filepath = source_dir / filename
        df.to_csv(filepath, index=False)
        files.append(str(filepath))

    return files


@pytest.fixture
def mock_sftp():
    """Create a mock SFTP client."""
    with patch("paramiko.SSHClient") as mock_ssh:
        mock_client = MagicMock()
        mock_sftp_client = MagicMock()

        # Setup the mock chain
        mock_ssh.return_value = mock_client
        mock_client.open_sftp.return_value = mock_sftp_client

        # Mock stat to simulate directory existence check
        mock_sftp_client.stat.side_effect = FileNotFoundError()

        # Mock file operations
        mock_file = MagicMock()
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)
        mock_sftp_client.file.return_value = mock_file

        yield {
            "ssh_class": mock_ssh,
            "client": mock_client,
            "sftp": mock_sftp_client,
            "file": mock_file,
        }


def test_uploader_initialization(test_dirs):
    """Test that uploader initializes correctly."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    assert uploader.host == "localhost"
    assert uploader.port == 22
    assert uploader.username == "test_user"
    assert uploader.remote_path == "/upload"
    assert uploader.source_dir.exists()
    assert uploader.archive_dir.exists()


def test_uploader_connection(test_dirs, mock_sftp):
    """Test SFTP connection establishment."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    uploader.connect()

    # Verify SSH client was created and configured
    mock_sftp["ssh_class"].assert_called_once()
    mock_sftp["client"].set_missing_host_key_policy.assert_called_once()

    # Verify connection was established
    mock_sftp["client"].connect.assert_called_once_with(
        hostname="localhost",
        port=22,
        username="test_user",
        password="test_pass",
    )

    # Verify SFTP session was opened
    mock_sftp["client"].open_sftp.assert_called_once()

    uploader.close()


def test_get_pending_files(test_dirs, sample_csv_files):
    """Test getting list of pending files."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    pending = uploader.get_pending_files()

    assert len(pending) == 3
    assert all(f.suffix == ".csv" for f in pending)
    assert all(f.exists() for f in pending)


def test_get_pending_files_empty(test_dirs):
    """Test getting pending files when directory is empty."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    pending = uploader.get_pending_files()
    assert len(pending) == 0


def test_upload_file(test_dirs, sample_csv_files, mock_sftp):
    """Test uploading a single file."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    uploader.connect()

    # Upload first file
    local_file = sample_csv_files[0]
    remote_path = uploader.upload_file(local_file)

    # Verify the file was uploaded
    mock_sftp["sftp"].put.assert_called_once()
    assert remote_path == "/upload/" + Path(local_file).name

    uploader.close()


def test_upload_dataframe_as_csv(test_dirs, mock_sftp):
    """Test uploading a DataFrame directly as CSV."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    uploader.connect()

    # Create test DataFrame
    df = pd.DataFrame(
        {
            "time": ["2026-01-20 10:00:00"],
            "cml_id": ["CML_001"],
            "tsl": [25.5],
            "rsl": [-45.2],
        }
    )

    # Upload DataFrame
    remote_path = uploader.upload_dataframe_as_csv(df, filename="test_data.csv")

    # Verify file was created on remote
    mock_sftp["sftp"].file.assert_called_once_with("/upload/test_data.csv", "w")
    mock_sftp["file"].write.assert_called_once()

    # Verify the written content contains CSV data
    written_data = mock_sftp["file"].write.call_args[0][0]
    assert "time,cml_id,tsl,rsl" in written_data
    assert "CML_001" in written_data

    uploader.close()


def test_upload_pending_files(test_dirs, sample_csv_files, mock_sftp):
    """Test uploading all pending files."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    uploader.connect()

    # Upload all pending files
    count = uploader.upload_pending_files()

    # Verify all files were uploaded
    assert count == 3
    assert mock_sftp["sftp"].put.call_count == 3

    # Verify files were moved to archive
    source_files = list(Path(test_dirs["source"]).glob("*.csv"))
    archive_files = list(Path(test_dirs["archive"]).glob("*.csv"))

    assert len(source_files) == 0
    assert len(archive_files) == 3

    uploader.close()


def test_upload_pending_files_no_connection(test_dirs, sample_csv_files):
    """Test that upload fails gracefully without connection."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    # Try to upload without connecting
    with pytest.raises(RuntimeError, match="SFTP connection not established"):
        uploader.upload_file(sample_csv_files[0])


def test_context_manager(test_dirs, mock_sftp):
    """Test using uploader as context manager."""
    with SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    ) as uploader:
        # Verify connection was established
        assert uploader.sftp is not None

    # Verify connection was closed
    mock_sftp["sftp"].close.assert_called_once()
    mock_sftp["client"].close.assert_called_once()


def test_upload_with_connection_error(test_dirs):
    """Test handling of connection errors."""
    uploader = SFTPUploader(
        host="invalid.host.example.com",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    # Should raise exception on connection failure
    with pytest.raises(Exception):
        uploader.connect()


def test_upload_continues_on_individual_file_error(
    test_dirs, sample_csv_files, mock_sftp
):
    """Test that upload continues even if one file fails."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    uploader.connect()

    # Make the second upload fail
    def side_effect(local, remote):
        if "10010" in local:  # Second file
            raise Exception("Upload failed")

    mock_sftp["sftp"].put.side_effect = side_effect

    # Upload should continue despite one failure
    count = uploader.upload_pending_files()

    # Only 2 files should succeed
    assert count == 2

    # One file should remain in source directory
    source_files = list(Path(test_dirs["source"]).glob("*.csv"))
    assert len(source_files) == 1

    uploader.close()


def test_remote_directory_creation(test_dirs, mock_sftp):
    """Test that remote directory is created if it doesn't exist."""
    uploader = SFTPUploader(
        host="localhost",
        port=22,
        username="test_user",
        password="test_pass",
        remote_path="/upload/cml_data",
        source_dir=test_dirs["source"],
        archive_dir=test_dirs["archive"],
    )

    uploader.connect()

    # Verify mkdir was called (directory didn't exist)
    assert mock_sftp["sftp"].mkdir.call_count > 0

    uploader.close()
