"""
Tests for storage backend abstraction layer.
"""

import os
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from storage import (
    LocalStorageBackend,
    S3StorageBackend,
    get_storage_backend,
)


# ==================== LocalStorageBackend Tests ====================


class TestLocalStorageBackend:
    """Test suite for LocalStorageBackend."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path)

    @pytest.fixture
    def storage(self, temp_dir):
        """Create a LocalStorageBackend instance."""
        return LocalStorageBackend(base_path=temp_dir)

    def test_initialization(self, temp_dir):
        """Test storage backend initialization."""
        storage = LocalStorageBackend(base_path=temp_dir)
        assert storage.base_path == Path(temp_dir).resolve()
        assert storage.base_path.exists()

    def test_initialization_creates_directory(self, temp_dir):
        """Test that initialization creates base directory if it doesn't exist."""
        new_path = os.path.join(temp_dir, "new_dir")
        storage = LocalStorageBackend(base_path=new_path)
        assert Path(new_path).exists()

    def test_exists_file_exists(self, storage, temp_dir):
        """Test exists() returns True for existing file."""
        test_file = Path(temp_dir) / "test.txt"
        test_file.write_text("content")
        assert storage.exists("test.txt") is True

    def test_exists_file_not_exists(self, storage):
        """Test exists() returns False for non-existing file."""
        assert storage.exists("nonexistent.txt") is False

    def test_write_and_read_file(self, storage):
        """Test writing and reading a file."""
        data = b"Hello, World!"
        storage.write_file("test.txt", data)
        assert storage.read_file("test.txt") == data

    def test_write_file_creates_subdirectories(self, storage):
        """Test that write_file creates necessary subdirectories."""
        data = b"test data"
        storage.write_file("subdir1/subdir2/file.txt", data)
        assert storage.exists("subdir1/subdir2/file.txt")
        assert storage.read_file("subdir1/subdir2/file.txt") == data

    def test_list_files_empty_directory(self, storage):
        """Test listing files in empty directory."""
        files = storage.list_files(".")
        assert files == []

    def test_list_files_with_files(self, storage):
        """Test listing files in directory with files."""
        storage.write_file("file1.txt", b"data1")
        storage.write_file("file2.txt", b"data2")
        storage.write_file("subdir/file3.txt", b"data3")

        files = storage.list_files(".")
        assert "file1.txt" in files
        assert "file2.txt" in files
        # Glob pattern matches only in root directory
        root_txt_files = [f for f in files if "/" not in f and f.endswith(".txt")]
        assert len(root_txt_files) == 2

    def test_list_files_with_pattern(self, storage):
        """Test listing files with glob pattern."""
        storage.write_file("file1.csv", b"data1")
        storage.write_file("file2.txt", b"data2")
        storage.write_file("file3.csv", b"data3")

        csv_files = storage.list_files(".", pattern="*.csv")
        # Only root level CSV files
        assert len(csv_files) == 2
        assert all(f.endswith(".csv") for f in csv_files)

    def test_list_files_nonexistent_directory(self, storage):
        """Test listing files in non-existent directory returns empty list."""
        files = storage.list_files("nonexistent")
        assert files == []

    def test_move_file(self, storage):
        """Test moving a file."""
        storage.write_file("source.txt", b"data")
        storage.move_file("source.txt", "destination.txt")

        assert storage.exists("destination.txt")
        assert not storage.exists("source.txt")
        assert storage.read_file("destination.txt") == b"data"

    def test_move_file_to_subdirectory(self, storage):
        """Test moving a file to a subdirectory."""
        storage.write_file("source.txt", b"data")
        storage.move_file("source.txt", "subdir/destination.txt")

        assert storage.exists("subdir/destination.txt")
        assert not storage.exists("source.txt")

    def test_delete_file(self, storage):
        """Test deleting a file."""
        storage.write_file("test.txt", b"data")
        assert storage.exists("test.txt")

        storage.delete_file("test.txt")
        assert not storage.exists("test.txt")

    def test_delete_nonexistent_file(self, storage):
        """Test deleting non-existent file doesn't raise error."""
        storage.delete_file("nonexistent.txt")  # Should not raise

    def test_get_file_size(self, storage):
        """Test getting file size."""
        data = b"1234567890"
        storage.write_file("test.txt", data)
        assert storage.get_file_size("test.txt") == len(data)

    def test_path_traversal_protection(self, storage):
        """Test that path traversal attempts are blocked."""
        with pytest.raises(ValueError, match="outside base_path"):
            storage.exists("../../../etc/passwd")

        with pytest.raises(ValueError, match="outside base_path"):
            storage.read_file("../sensitive.txt")

        with pytest.raises(ValueError, match="outside base_path"):
            storage.write_file("../../../tmp/malicious.txt", b"data")

    def test_path_normalization(self, storage):
        """Test that paths are normalized correctly."""
        storage.write_file("./test.txt", b"data")
        assert storage.exists("test.txt")

        storage.write_file("subdir/../test2.txt", b"data")
        assert storage.exists("test2.txt")


# ==================== S3StorageBackend Tests ====================


def test_s3_import_error_without_boto3():
    """Test that S3StorageBackend raises ImportError when boto3 is not installed."""
    # Only run this test if boto3 is NOT installed
    try:
        import boto3

        pytest.skip("boto3 is installed - cannot test ImportError")
    except ImportError:
        pass

    with pytest.raises(ImportError, match="boto3 is required"):
        S3StorageBackend(bucket="test-bucket")


@pytest.mark.skipif(
    not hasattr(sys.modules.get("boto3", None), "client"),
    reason="boto3 not installed - S3 tests skipped",
)
class TestS3StorageBackend:
    """Test suite for S3StorageBackend."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        import boto3

        mock_client = MagicMock()

        with patch.object(boto3, "client", return_value=mock_client):
            yield mock_client

    @pytest.fixture
    def storage(self, mock_s3_client):
        """Create an S3StorageBackend instance with mocked client."""
        return S3StorageBackend(
            bucket="test-bucket",
            endpoint_url="http://localhost:9000",
            access_key="test-key",
            secret_key="test-secret",
        )

    def test_initialization(self, mock_s3_client):
        """Test S3 storage backend initialization."""
        storage = S3StorageBackend(
            bucket="my-bucket",
            endpoint_url="http://minio:9000",
            access_key="access",
            secret_key="secret",
            region="us-west-2",
        )

        assert storage.bucket == "my-bucket"

    def test_initialization_without_endpoint(self, mock_s3_client):
        """Test initialization without custom endpoint (AWS S3)."""
        storage = S3StorageBackend(
            bucket="my-bucket", access_key="access", secret_key="secret"
        )

        assert storage.bucket == "my-bucket"

    def test_exists_file_exists(self, storage, mock_s3_client):
        """Test exists() returns True when file exists."""
        mock_s3_client.head_object.return_value = {"ContentLength": 100}

        assert storage.exists("test.txt") is True
        mock_s3_client.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="test.txt"
        )

    def test_exists_file_not_exists(self, storage, mock_s3_client):
        """Test exists() returns False when file doesn't exist."""
        from botocore.exceptions import ClientError

        mock_s3_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadObject"
        )

        assert storage.exists("test.txt") is False

    def test_read_file(self, storage, mock_s3_client):
        """Test reading a file from S3."""
        mock_body = MagicMock()
        mock_body.read.return_value = b"file content"
        mock_s3_client.get_object.return_value = {"Body": mock_body}

        data = storage.read_file("test.txt")

        assert data == b"file content"
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="test.txt"
        )

    def test_read_file_not_found(self, storage, mock_s3_client):
        """Test reading non-existent file raises FileNotFoundError."""
        from botocore.exceptions import ClientError

        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )

        with pytest.raises(FileNotFoundError, match="File not found in S3"):
            storage.read_file("test.txt")

    def test_write_file(self, storage, mock_s3_client):
        """Test writing a file to S3."""
        data = b"test data"
        storage.write_file("test.txt", data)

        mock_s3_client.put_object.assert_called_once_with(
            Bucket="test-bucket", Key="test.txt", Body=data
        )

    def test_write_file_error(self, storage, mock_s3_client):
        """Test write_file raises IOError on failure."""
        from botocore.exceptions import ClientError

        mock_s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "PutObject"
        )

        with pytest.raises(IOError, match="Error writing to S3"):
            storage.write_file("test.txt", b"data")

    def test_list_files_empty(self, storage, mock_s3_client):
        """Test listing files in empty bucket/prefix."""
        mock_s3_client.list_objects_v2.return_value = {}

        files = storage.list_files("incoming")
        assert files == []

    def test_list_files_with_files(self, storage, mock_s3_client):
        """Test listing files in S3."""
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "incoming/file1.csv"},
                {"Key": "incoming/file2.csv"},
                {"Key": "incoming/subdir/"},  # Directory marker
                {"Key": "incoming/file3.txt"},
            ]
        }

        files = storage.list_files("incoming")

        assert len(files) == 3
        assert "incoming/file1.csv" in files
        assert "incoming/file2.csv" in files
        assert "incoming/file3.txt" in files
        assert "incoming/subdir/" not in files  # Directories excluded

    def test_list_files_with_pattern(self, storage, mock_s3_client):
        """Test listing files with pattern matching."""
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.csv"},
                {"Key": "file2.txt"},
                {"Key": "file3.csv"},
            ]
        }

        files = storage.list_files("", pattern="*.csv")

        assert len(files) == 2
        assert "file1.csv" in files
        assert "file3.csv" in files

    def test_move_file(self, storage, mock_s3_client):
        """Test moving a file in S3."""
        storage.move_file("source.txt", "destination.txt")

        mock_s3_client.copy_object.assert_called_once_with(
            Bucket="test-bucket",
            CopySource={"Bucket": "test-bucket", "Key": "source.txt"},
            Key="destination.txt",
        )
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="source.txt"
        )

    def test_move_file_error(self, storage, mock_s3_client):
        """Test move_file raises IOError on failure."""
        from botocore.exceptions import ClientError

        mock_s3_client.copy_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "CopyObject"
        )

        with pytest.raises(IOError, match="Error moving file in S3"):
            storage.move_file("source.txt", "dest.txt")

    def test_delete_file(self, storage, mock_s3_client):
        """Test deleting a file from S3."""
        storage.delete_file("test.txt")

        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="test.txt"
        )

    def test_delete_file_error_logged(self, storage, mock_s3_client):
        """Test delete_file logs errors but doesn't raise."""
        from botocore.exceptions import ClientError

        mock_s3_client.delete_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}}, "DeleteObject"
        )

        # Should not raise exception
        storage.delete_file("test.txt")

    def test_get_file_size(self, storage, mock_s3_client):
        """Test getting file size from S3."""
        mock_s3_client.head_object.return_value = {"ContentLength": 12345}

        size = storage.get_file_size("test.txt")

        assert size == 12345
        mock_s3_client.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="test.txt"
        )

    def test_get_file_size_not_found(self, storage, mock_s3_client):
        """Test get_file_size raises FileNotFoundError for missing file."""
        from botocore.exceptions import ClientError

        mock_s3_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadObject"
        )

        with pytest.raises(FileNotFoundError, match="File not found in S3"):
            storage.get_file_size("test.txt")


# ==================== Factory Function Tests ====================


class TestGetStorageBackend:
    """Test suite for get_storage_backend factory function."""

    def test_default_local_backend(self, tmp_path):
        """Test factory returns LocalStorageBackend by default."""
        with patch.dict(os.environ, {"STORAGE_BASE_PATH": str(tmp_path)}, clear=True):
            storage = get_storage_backend()
            assert isinstance(storage, LocalStorageBackend)

    def test_local_backend_with_custom_path(self, tmp_path):
        """Test factory creates LocalStorageBackend with custom path."""
        custom_path = tmp_path / "custom"
        with patch.dict(
            os.environ,
            {"STORAGE_BACKEND": "local", "STORAGE_BASE_PATH": str(custom_path)},
        ):
            storage = get_storage_backend()
            assert isinstance(storage, LocalStorageBackend)
            assert str(storage.base_path) == str(custom_path)

    def test_s3_backend(self):
        """Test factory creates S3StorageBackend."""
        try:
            import boto3

            has_boto3 = True
        except ImportError:
            has_boto3 = False

        if not has_boto3:
            pytest.skip("boto3 not installed")

        mock_client = MagicMock()
        with patch.object(boto3, "client", return_value=mock_client), patch.dict(
            os.environ,
            {
                "STORAGE_BACKEND": "s3",
                "STORAGE_S3_BUCKET": "my-bucket",
                "STORAGE_S3_ACCESS_KEY": "key",
                "STORAGE_S3_SECRET_KEY": "secret",
            },
        ):
            storage = get_storage_backend()
            assert isinstance(storage, S3StorageBackend)
            assert storage.bucket == "my-bucket"

    def test_minio_backend(self):
        """Test factory creates S3StorageBackend for MinIO."""
        try:
            import boto3

            has_boto3 = True
        except ImportError:
            has_boto3 = False

        if not has_boto3:
            pytest.skip("boto3 not installed")

        mock_client = MagicMock()
        with patch.object(boto3, "client", return_value=mock_client), patch.dict(
            os.environ,
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_S3_BUCKET": "my-bucket",
                "STORAGE_S3_ENDPOINT": "http://minio:9000",
                "STORAGE_S3_ACCESS_KEY": "key",
                "STORAGE_S3_SECRET_KEY": "secret",
            },
        ):
            storage = get_storage_backend()
            assert isinstance(storage, S3StorageBackend)

    def test_s3_backend_missing_bucket(self):
        """Test factory raises error if S3 bucket not specified."""
        with patch.dict(os.environ, {"STORAGE_BACKEND": "s3"}):
            with pytest.raises(ValueError, match="STORAGE_S3_BUCKET is required"):
                get_storage_backend()

    def test_minio_backend_missing_endpoint(self):
        """Test factory raises error if MinIO endpoint not specified."""
        with patch.dict(
            os.environ, {"STORAGE_BACKEND": "minio", "STORAGE_S3_BUCKET": "bucket"}
        ):
            with pytest.raises(ValueError, match="STORAGE_S3_ENDPOINT is required"):
                get_storage_backend()

    def test_unknown_backend(self):
        """Test factory raises error for unknown backend type."""
        with patch.dict(os.environ, {"STORAGE_BACKEND": "unknown"}):
            with pytest.raises(ValueError, match="Unknown storage backend"):
                get_storage_backend()
