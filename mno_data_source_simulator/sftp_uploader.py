"""
SFTP uploader module for the MNO simulator.

This module handles uploading CML data to an SFTP server.
"""

import paramiko
import io
import logging
import shutil
from datetime import datetime
from typing import Optional, List
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


class SFTPUploader:
    """Upload CML data to an SFTP server."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        remote_path: str,
        source_dir: str = "data_to_upload",
        archive_dir: str = "data_uploaded",
    ):
        """
        Initialize the SFTP uploader.

        Parameters
        ----------
        host : str
            SFTP server hostname.
        port : int
            SFTP server port.
        username : str
            SFTP username.
        password : str
            SFTP password.
        remote_path : str
            Remote directory path where files will be uploaded.
        source_dir : str, optional
            Local directory to read files from (default: "data_to_upload").
        archive_dir : str, optional
            Local directory to move uploaded files to (default: "data_uploaded").
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.remote_path = remote_path
        self.source_dir = Path(source_dir)
        self.archive_dir = Path(archive_dir)
        self.client: Optional[paramiko.SSHClient] = None
        self.sftp: Optional[paramiko.SFTPClient] = None

        # Create directories if they don't exist
        self.source_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Source directory: {self.source_dir}")
        logger.info(f"Archive directory: {self.archive_dir}")

    def connect(self):
        """Establish SFTP connection."""
        try:
            logger.info(f"Connecting to SFTP server: {self.host}:{self.port}")

            # Create SSH client
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to the server
            self.client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )

            # Open SFTP session
            self.sftp = self.client.open_sftp()

            # Ensure remote directory exists
            self._ensure_remote_directory()

            logger.info("SFTP connection established")
        except Exception as e:
            logger.error(f"Failed to connect to SFTP server: {e}")
            raise

    def _ensure_remote_directory(self):
        """Ensure the remote directory exists, create if it doesn't."""
        try:
            self.sftp.stat(self.remote_path)
            logger.debug(f"Remote directory exists: {self.remote_path}")
        except FileNotFoundError:
            logger.info(f"Creating remote directory: {self.remote_path}")
            # Create directory recursively
            dirs = self.remote_path.split("/")
            current_path = ""
            for d in dirs:
                if d:
                    current_path += "/" + d
                    try:
                        self.sftp.stat(current_path)
                    except FileNotFoundError:
                        self.sftp.mkdir(current_path)

    def upload_dataframe_as_csv(
        self, df: pd.DataFrame, filename: Optional[str] = None
    ) -> str:
        """
        Upload a DataFrame as CSV to the SFTP server.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to upload.
        filename : str, optional
            Filename to use. If None, generates timestamp-based filename.

        Returns
        -------
        str
            Remote file path of the uploaded file.
        """
        if self.sftp is None:
            raise RuntimeError("SFTP connection not established. Call connect() first.")

        # Generate filename if not provided
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cml_data_{timestamp}.csv"

        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        # Upload file
        remote_file_path = f"{self.remote_path}/{filename}"

        try:
            logger.info(f"Uploading file: {remote_file_path}")

            # Write CSV data to remote file
            with self.sftp.file(remote_file_path, "w") as remote_file:
                remote_file.write(csv_data)

            logger.info(f"Successfully uploaded {len(df)} rows to {remote_file_path}")
            return remote_file_path

        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            raise

    def upload_file(self, local_path: str, remote_filename: Optional[str] = None):
        """
        Upload a local file to the SFTP server.

        Parameters
        ----------
        local_path : str
            Path to the local file.
        remote_filename : str, optional
            Remote filename. If None, uses the local filename.

        Returns
        -------
        str
            Remote file path of the uploaded file.
        """
        if self.sftp is None:
            raise RuntimeError("SFTP connection not established. Call connect() first.")

        if remote_filename is None:
            remote_filename = local_path.split("/")[-1]

        remote_file_path = f"{self.remote_path}/{remote_filename}"

        try:
            logger.info(f"Uploading file from {local_path} to {remote_file_path}")
            self.sftp.put(local_path, remote_file_path)
            logger.info(f"Successfully uploaded file to {remote_file_path}")
            return remote_file_path

        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            raise

    def get_pending_files(self) -> List[Path]:
        """
        Get list of CSV files waiting to be uploaded.

        Returns
        -------
        List[Path]
            List of CSV file paths in the source directory.
        """
        csv_files = sorted(self.source_dir.glob("*.csv"))
        logger.debug(f"Found {len(csv_files)} pending files")
        return csv_files

    def upload_pending_files(self) -> int:
        """
        Upload all pending CSV files from source directory to SFTP server.
        After successful upload, move files to archive directory.

        Returns
        -------
        int
            Number of files successfully uploaded.
        """
        pending_files = self.get_pending_files()

        if not pending_files:
            logger.debug("No pending files to upload")
            return 0

        uploaded_count = 0

        for file_path in pending_files:
            try:
                # Upload the file
                remote_file_path = self.upload_file(str(file_path))

                # Move to archive directory
                archive_path = self.archive_dir / file_path.name
                shutil.move(str(file_path), str(archive_path))
                logger.info(f"Moved {file_path.name} to archive")

                uploaded_count += 1

            except Exception as e:
                logger.error(f"Failed to upload {file_path.name}: {e}")
                # Continue with next file
                continue

        logger.info(
            f"Successfully uploaded {uploaded_count}/{len(pending_files)} files"
        )
        return uploaded_count

    def close(self):
        """Close the SFTP connection."""
        if self.sftp:
            self.sftp.close()
            logger.debug("SFTP session closed")

        if self.client:
            self.client.close()
            logger.info("SFTP connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
