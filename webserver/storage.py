"""
Storage abstraction layer for flexible backend support.

Supports multiple storage backends:
- Local filesystem (development/testing)
- S3-compatible object storage (MinIO, AWS S3, etc.)

This allows easy switching between storage backends via environment variables.
"""

import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, BinaryIO, Optional
import logging

logger = logging.getLogger(__name__)


class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a file exists at the given path."""
        pass

    @abstractmethod
    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """
        List files in a directory.

        Args:
            path: Directory path to list
            pattern: Glob pattern for filtering files (default: "*")

        Returns:
            List of file paths
        """
        pass

    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """
        Read file contents as bytes.

        Args:
            path: File path to read

        Returns:
            File contents as bytes
        """
        pass

    @abstractmethod
    def write_file(self, path: str, data: bytes) -> None:
        """
        Write bytes to a file.

        Args:
            path: File path to write to
            data: File contents as bytes
        """
        pass

    @abstractmethod
    def move_file(self, src: str, dst: str) -> None:
        """
        Move a file from source to destination.

        Args:
            src: Source file path
            dst: Destination file path
        """
        pass

    @abstractmethod
    def delete_file(self, path: str) -> None:
        """
        Delete a file.

        Args:
            path: File path to delete
        """
        pass

    @abstractmethod
    def get_file_size(self, path: str) -> int:
        """
        Get file size in bytes.

        Args:
            path: File path

        Returns:
            File size in bytes
        """
        pass


class LocalStorageBackend(StorageBackend):
    """Local filesystem storage backend."""

    def __init__(self, base_path: str = "/app/data"):
        """
        Initialize local storage backend.

        Args:
            base_path: Base directory for all storage operations
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized LocalStorageBackend with base_path: {self.base_path}")

    def _resolve_path(self, path: str) -> Path:
        """Resolve a path relative to base_path and ensure it's within base_path."""
        full_path = (self.base_path / path).resolve()
        # Security check: ensure path is within base_path
        if not str(full_path).startswith(str(self.base_path.resolve())):
            raise ValueError(f"Path {path} is outside base_path")
        return full_path

    def exists(self, path: str) -> bool:
        """Check if a file exists."""
        return self._resolve_path(path).exists()

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """List files matching pattern in directory."""
        dir_path = self._resolve_path(path)
        if not dir_path.exists():
            return []

        # Return paths relative to base_path
        files = []
        for file_path in dir_path.glob(pattern):
            if file_path.is_file():
                rel_path = file_path.relative_to(self.base_path)
                files.append(str(rel_path))
        return sorted(files)

    def read_file(self, path: str) -> bytes:
        """Read file contents."""
        file_path = self._resolve_path(path)
        with open(file_path, "rb") as f:
            return f.read()

    def write_file(self, path: str, data: bytes) -> None:
        """Write data to file."""
        file_path = self._resolve_path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(data)

    def move_file(self, src: str, dst: str) -> None:
        """Move file from src to dst."""
        src_path = self._resolve_path(src)
        dst_path = self._resolve_path(dst)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src_path), str(dst_path))

    def delete_file(self, path: str) -> None:
        """Delete a file."""
        file_path = self._resolve_path(path)
        if file_path.exists():
            file_path.unlink()

    def get_file_size(self, path: str) -> int:
        """Get file size in bytes."""
        file_path = self._resolve_path(path)
        return file_path.stat().st_size


class S3StorageBackend(StorageBackend):
    """S3-compatible storage backend (AWS S3, MinIO, etc.)."""

    def __init__(
        self,
        bucket: str,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-east-1",
    ):
        """
        Initialize S3 storage backend.

        Args:
            bucket: S3 bucket name
            endpoint_url: S3 endpoint URL (for MinIO or custom S3)
            access_key: AWS access key ID
            secret_key: AWS secret access key
            region: AWS region
        """
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError:
            raise ImportError(
                "boto3 is required for S3StorageBackend. "
                "Install with: pip install boto3"
            )

        self.bucket = bucket
        self.ClientError = ClientError

        # Initialize S3 client
        client_kwargs = {"region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        if access_key and secret_key:
            client_kwargs["aws_access_key_id"] = access_key
            client_kwargs["aws_secret_access_key"] = secret_key

        self.s3_client = boto3.client("s3", **client_kwargs)
        logger.info(
            f"Initialized S3StorageBackend with bucket: {bucket}, "
            f"endpoint: {endpoint_url or 'AWS'}"
        )

    def exists(self, path: str) -> bool:
        """Check if a file exists in S3."""
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=path)
            return True
        except self.ClientError:
            return False

    def list_files(self, path: str, pattern: str = "*") -> List[str]:
        """List files in S3 with optional pattern matching."""
        prefix = path.rstrip("/") + "/" if path else ""

        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        except self.ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            return []

        if "Contents" not in response:
            return []

        files = []
        for obj in response["Contents"]:
            key = obj["Key"]
            # Skip directories (keys ending with /)
            if key.endswith("/"):
                continue

            # Simple pattern matching (only supports * wildcard)
            if pattern != "*":
                import fnmatch

                if not fnmatch.fnmatch(key, pattern):
                    continue

            files.append(key)

        return sorted(files)

    def read_file(self, path: str) -> bytes:
        """Read file from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=path)
            return response["Body"].read()
        except self.ClientError as e:
            raise FileNotFoundError(f"File not found in S3: {path}") from e

    def write_file(self, path: str, data: bytes) -> None:
        """Write data to S3."""
        try:
            self.s3_client.put_object(Bucket=self.bucket, Key=path, Body=data)
        except self.ClientError as e:
            raise IOError(f"Error writing to S3: {path}") from e

    def move_file(self, src: str, dst: str) -> None:
        """Move file within S3 (copy then delete)."""
        try:
            # Copy to new location
            self.s3_client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": src},
                Key=dst,
            )
            # Delete original
            self.s3_client.delete_object(Bucket=self.bucket, Key=src)
        except self.ClientError as e:
            raise IOError(f"Error moving file in S3: {src} -> {dst}") from e

    def delete_file(self, path: str) -> None:
        """Delete file from S3."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=path)
        except self.ClientError as e:
            logger.error(f"Error deleting S3 object {path}: {e}")

    def get_file_size(self, path: str) -> int:
        """Get file size from S3."""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket, Key=path)
            return response["ContentLength"]
        except self.ClientError as e:
            raise FileNotFoundError(f"File not found in S3: {path}") from e


def get_storage_backend() -> StorageBackend:
    """
    Factory function to create storage backend based on environment variables.

    Environment variables:
        STORAGE_BACKEND: 'local' (default), 's3', or 'minio'
        STORAGE_BASE_PATH: Base path for local storage (default: /app/data)
        STORAGE_S3_BUCKET: S3 bucket name (required for s3/minio)
        STORAGE_S3_ENDPOINT: S3 endpoint URL (required for minio)
        STORAGE_S3_ACCESS_KEY: S3 access key
        STORAGE_S3_SECRET_KEY: S3 secret key
        STORAGE_S3_REGION: S3 region (default: us-east-1)

    Returns:
        Configured StorageBackend instance
    """
    backend_type = os.getenv("STORAGE_BACKEND", "local").lower()

    if backend_type == "local":
        base_path = os.getenv("STORAGE_BASE_PATH", "/app/data")
        return LocalStorageBackend(base_path=base_path)

    elif backend_type in ["s3", "minio"]:
        bucket = os.getenv("STORAGE_S3_BUCKET")
        if not bucket:
            raise ValueError("STORAGE_S3_BUCKET is required for S3/MinIO backend")

        endpoint_url = None
        if backend_type == "minio":
            endpoint_url = os.getenv("STORAGE_S3_ENDPOINT")
            if not endpoint_url:
                raise ValueError("STORAGE_S3_ENDPOINT is required for MinIO backend")

        return S3StorageBackend(
            bucket=bucket,
            endpoint_url=endpoint_url,
            access_key=os.getenv("STORAGE_S3_ACCESS_KEY"),
            secret_key=os.getenv("STORAGE_S3_SECRET_KEY"),
            region=os.getenv("STORAGE_S3_REGION", "us-east-1"),
        )

    else:
        raise ValueError(
            f"Unknown storage backend: {backend_type}. "
            f"Supported: 'local', 's3', 'minio'"
        )
