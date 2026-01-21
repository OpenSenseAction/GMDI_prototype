"""
SFTP uploader module for the MNO simulator.

This module handles uploading CML data to an SFTP server.
"""

import paramiko
import io
import logging
import shutil
import re
import os
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
        password: Optional[str] = None,
        private_key_path: Optional[str] = None,
        known_hosts_path: Optional[str] = None,
        remote_path: str = "/upload",
        source_dir: str = "data_to_upload",
        archive_dir: str = "data_uploaded",
        connection_timeout: int = 30,
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
        password : str, optional
            SFTP password. Use either password or private_key_path.
        private_key_path : str, optional
            Path to SSH private key file. Preferred over password authentication.
        known_hosts_path : str, optional
            Path to known_hosts file for host key verification.
            If not provided, uses ~/.ssh/known_hosts.
        remote_path : str
            Remote directory path where files will be uploaded.
        source_dir : str, optional
            Local directory to read files from (default: "data_to_upload").
        archive_dir : str, optional
            Local directory to move uploaded files to (default: "data_uploaded").
        connection_timeout : int, optional
            Connection timeout in seconds (default: 30).
        """
        self.host = host
        self.port = port
        self.username = username
        self._password = password  # Private to discourage direct access
        self.private_key_path = private_key_path
        self.known_hosts_path = known_hosts_path or os.path.expanduser(
            "~/.ssh/known_hosts"
        )
        self.connection_timeout = connection_timeout

        # Validate remote path
        self.remote_path = self._validate_remote_path(remote_path)

        self.source_dir = Path(source_dir)
        self.archive_dir = Path(archive_dir)
        self.client: Optional[paramiko.SSHClient] = None
        self.sftp: Optional[paramiko.SFTPClient] = None

        # Create directories if they don't exist
        self.source_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Source directory: {self.source_dir}")
        logger.info(f"Archive directory: {self.archive_dir}")

    def _validate_remote_path(self, path: str) -> str:
        """
        Validate and sanitize remote path.

        Parameters
        ----------
        path : str
            Remote path to validate.

        Returns
        -------
        str
            Validated path.

        Raises
        ------
        ValueError
            If path contains invalid characters or patterns.
        """
        # Must be absolute path
        if not path.startswith("/"):
            raise ValueError("Remote path must be absolute (start with /)")

        # No path traversal attempts
        if ".." in path:
            raise ValueError("Remote path cannot contain '..' (path traversal)")

        # Only allow safe characters: alphanumeric, /, _, -, .
        if not re.match(r"^[a-zA-Z0-9/_.-]+$", path):
            raise ValueError("Remote path contains invalid characters")

        # Normalize path (remove duplicate slashes, etc.)
        normalized = os.path.normpath(path)

        # Ensure normalization didn't change the path (security check)
        if normalized != path.replace("//", "/"):
            logger.warning(f"Path normalized from {path} to {normalized}")

        return normalized

    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename to prevent path traversal.

        Parameters
        ----------
        filename : str
            Filename to sanitize.

        Returns
        -------
        str
            Sanitized filename.

        Raises
        ------
        ValueError
            If filename is invalid.
        """
        # Get just the filename, no directory components
        basename = os.path.basename(filename)

        # Check for path traversal attempts
        if basename != filename or "/" in filename or "\\" in filename:
            raise ValueError(f"Invalid filename: {filename}")

        # Only allow safe characters
        if not re.match(r"^[a-zA-Z0-9_.-]+$", basename):
            raise ValueError(f"Filename contains invalid characters: {basename}")

        # Prevent hidden files
        if basename.startswith("."):
            raise ValueError(f"Hidden files not allowed: {basename}")

        return basename

    def connect(self):
        """Establish SFTP connection with host key verification."""
        try:
            logger.info(
                f"Connecting to SFTP server {self.username}@{self.host}:{self.port}"
            )

            # Create SSH client
            self.client = paramiko.SSHClient()

            # Load host keys for verification
            try:
                self.client.load_host_keys(self.known_hosts_path)
                logger.debug(f"Loaded host keys from {self.known_hosts_path}")
            except FileNotFoundError:
                logger.warning(f"Known hosts file not found: {self.known_hosts_path}")
                logger.warning(
                    "Host key verification will fail unless AutoAddPolicy is used"
                )

            # Use strict host key checking (reject unknown hosts)
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())

            # Prepare authentication parameters
            connect_kwargs = {
                "hostname": self.host,
                "port": self.port,
                "username": self.username,
                "timeout": self.connection_timeout,
                "look_for_keys": False,  # Explicit control
            }

            # Use key-based auth if available, otherwise password
            if self.private_key_path:
                logger.info("Using SSH key authentication")
                try:
                    private_key = paramiko.RSAKey.from_private_key_file(
                        self.private_key_path
                    )
                    connect_kwargs["pkey"] = private_key
                except Exception as e:
                    logger.error(f"Failed to load private key: {e}")
                    raise ValueError(
                        f"Invalid private key file: {self.private_key_path}"
                    )
            elif self._password:
                logger.info(
                    "Using password authentication (consider switching to SSH keys)"
                )
                connect_kwargs["password"] = self._password
            else:
                raise ValueError("Either password or private_key_path must be provided")

            # Connect to the server
            self.client.connect(**connect_kwargs)

            # Clear password from memory after successful connection
            if self._password:
                self._password = None
                logger.debug("Password cleared from memory")

            # Open SFTP session
            self.sftp = self.client.open_sftp()

            # Ensure remote directory exists
            self._ensure_remote_directory()

            logger.info("SFTP connection established successfully")

        except paramiko.AuthenticationException as e:
            logger.error(f"Authentication failed: {e}")
            raise
        except paramiko.SSHException as e:
            logger.error(f"SSH error: {e}")
            raise
        except OSError as e:
            logger.error(f"Network error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to SFTP server: {e}")
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

        # Sanitize filename
        safe_filename = self._sanitize_filename(filename)

        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        # Upload file
        remote_file_path = f"{self.remote_path}/{safe_filename}"

        try:
            logger.info(f"Uploading DataFrame to: {safe_filename}")

            # Write CSV data to remote file
            with self.sftp.file(remote_file_path, "w") as remote_file:
                remote_file.write(csv_data)

            logger.info(f"Successfully uploaded {len(df)} rows")
            return remote_file_path

        except paramiko.SSHException as e:
            logger.error(f"SSH error during upload: {e}")
            raise
        except OSError as e:
            logger.error(f"File operation error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during upload: {e}")
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

        # Extract and sanitize filename
        if remote_filename is None:
            remote_filename = Path(local_path).name

        safe_filename = self._sanitize_filename(remote_filename)
        remote_file_path = f"{self.remote_path}/{safe_filename}"

        try:
            logger.info(f"Uploading file: {safe_filename}")
            self.sftp.put(local_path, remote_file_path)
            logger.info(f"Successfully uploaded file")
            return remote_file_path

        except FileNotFoundError as e:
            logger.error(f"Local file not found: {local_path}")
            raise
        except paramiko.SSHException as e:
            logger.error(f"SSH error during upload: {e}")
            raise
        except OSError as e:
            logger.error(f"File operation error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during upload: {e}")
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

            except ValueError as e:
                logger.error(f"Validation error for {file_path.name}: {e}")
                continue
            except paramiko.SSHException as e:
                logger.error(f"SSH error uploading {file_path.name}: {e}")
                continue
            except OSError as e:
                logger.error(f"File operation error for {file_path.name}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error uploading {file_path.name}: {e}")
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
