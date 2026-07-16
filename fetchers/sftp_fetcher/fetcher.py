"""SFTP Fetcher - Poll external SFTP servers and write files to incoming directory."""

import fnmatch
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import paramiko

from fetchers.shared.config import load_config, resolve_env
from fetchers.shared.incoming_writer import atomic_write
from fetchers.shared.polling import run_poll_loop
from fetchers.shared.state import FetcherState

logger = logging.getLogger(__name__)


class SFTPFetcher:
    """Polls an external SFTP server and downloads new files."""

    def __init__(self, config: dict):
        """Initialize fetcher with configuration.
        
        Args:
            config: Source configuration dictionary with keys:
                - name: Source identifier
                - host: SFTP hostname
                - port: SFTP port (default 22)
                - username: Username for authentication
                - private_key_env: Env var name containing path to private key
                - password_env: Env var name containing password (alternative to key)
                - remote_path: Remote directory to poll
                - file_glob: Glob pattern for files to download (e.g., "*.csv")
                - after_download: What to do after download (leave|delete|move)
                - poll_interval_seconds: How often to poll
                - archive_subdir: Subdirectory name for moved files (default: "done")
        """
        self.name = config['name']
        self.host = config['host']
        self.port = config.get('port', 22)
        self.username = config['username']
        self.remote_path = config['remote_path']
        self.file_glob = config.get('file_glob', '*')
        self.after_download = config.get('after_download', 'leave')
        self.poll_interval = config.get('poll_interval_seconds', 60)
        self.archive_subdir = config.get('archive_subdir', 'done')
        
        # Authentication: prefer SSH key over password
        self.private_key_path = None
        self.password = None
        
        if 'private_key_env' in config:
            self.private_key_path = resolve_env(config['private_key_env'])
        elif 'password_env' in config:
            self.password = resolve_env(config['password_env'])
        else:
            raise ValueError(
                f"Source {self.name}: must specify either 'private_key_env' or 'password_env'"
            )
        
        # State management for tracking seen files
        state_dir = Path(os.environ.get('STATE_DIR', '/app/state'))
        self.state = FetcherState(state_dir / f"{self.name}_state.json")
        
        # Incoming directory where files are written
        self.incoming_dir = Path(os.environ.get('INCOMING_DIR', '/app/incoming'))
        
        # SFTP client
        self.sftp: Optional[paramiko.SFTPClient] = None
        self.transport: Optional[paramiko.Transport] = None

    def connect(self) -> None:
        """Establish SFTP connection."""
        logger.info(f"Connecting to SFTP {self.username}@{self.host}:{self.port}")
        
        self.transport = paramiko.Transport((self.host, self.port))
        
        # Load SSH key or use password
        if self.private_key_path:
            key = paramiko.RSAKey.from_private_key_file(self.private_key_path)
            self.transport.connect(username=self.username, pkey=key)
            logger.info(f"Authenticated with SSH key: {self.private_key_path}")
        else:
            self.transport.connect(username=self.username, password=self.password)
            logger.info("Authenticated with password")
        
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        logger.info(f"SFTP connection established")

    def disconnect(self) -> None:
        """Close SFTP connection."""
        if self.sftp:
            self.sftp.close()
            self.sftp = None
        if self.transport:
            self.transport.close()
            self.transport = None
        logger.info("SFTP connection closed")

    def list_remote_files(self) -> List[Dict[str, Any]]:
        """List files in remote directory matching glob pattern."""
        if not self.sftp:
            self.connect()
        
        try:
            entries = self.sftp.listdir_attr(self.remote_path)
        except FileNotFoundError:
            logger.error(f"Remote path {self.remote_path} does not exist")
            return []
        
        files = []
        for entry in entries:
            if fnmatch.fnmatch(entry.filename, self.file_glob):
                # Skip directories
                if not entry.st_mode & 0o40000:  # not S_IFDIR
                    files.append({
                        'filename': entry.filename,
                        'size': entry.st_size,
                        'mtime': entry.st_mtime,
                    })
        
        logger.info(f"Found {len(files)} remote files matching '{self.file_glob}'")
        return files
    
    def download_file(self, remote_filename: str) -> bytes:
        """Download file content from remote SFTP."""
        if not self.sftp:
            self.connect()
        
        remote_filepath = f"{self.remote_path}/{remote_filename}"
        logger.info(f"Downloading {remote_filepath}")
        
        with self.sftp.open(remote_filepath, 'rb') as remote_file:
            return remote_file.read()

    def delete_remote_file(self, filename: str) -> None:
        """Delete file from remote SFTP after download."""
        if not self.sftp:
            self.connect()
        
        remote_filepath = f"{self.remote_path}/{filename}"
        logger.info(f"Deleting remote file {remote_filepath}")
        self.sftp.remove(remote_filepath)

    def move_remote_file(self, filename: str, target_dir: str = "done") -> None:
        """Move file to done/ subdirectory on remote SFTP."""
        if not self.sftp:
            self.connect()
        
        remote_filepath = f"{self.remote_path}/{filename}"
        target_filepath = f"{self.remote_path}/{target_dir}/{filename}"
        
        logger.info(f"Moving {remote_filepath} to {target_filepath}")
        
        # Ensure target directory exists
        try:
            self.sftp.stat(f"{self.remote_path}/{target_dir}")
        except FileNotFoundError:
            logger.info(f"Creating remote directory {target_dir}")
            self.sftp.mkdir(f"{self.remote_path}/{target_dir}")
        
        self.sftp.rename(remote_filepath, target_filepath)

    def poll(self) -> int:
        """Single polling iteration: check for new files and download them.

        Returns:
            Number of files downloaded.
        """
        downloaded_count = 0

        if not self.sftp:
            self.connect()

        remote_files = self.list_remote_files()

        for file_info in remote_files:
            filename = file_info['filename']
            mtime = str(file_info['mtime'])

            if self.state.is_seen(self.name, filename, mtime):
                logger.debug(f"Skipping already-seen file: {filename}")
                continue

            try:
                content = self.download_file(filename)
                final_path = atomic_write(self.incoming_dir, filename, content)
                logger.info(f"Wrote {final_path} ({len(content)} bytes)")

                self.state.mark_seen(self.name, filename, mtime)

                if self.after_download == 'delete':
                    self.delete_remote_file(filename)
                elif self.after_download == 'move':
                    self.move_remote_file(filename, self.archive_subdir)

                downloaded_count += 1

            except Exception as e:
                logger.error(f"Failed to download {filename}: {e}")
                # Don't update state — will retry on next poll

        return downloaded_count

    def run(self) -> None:
        """Run the fetcher in a continuous loop."""
        logger.info(f"Starting SFTP fetcher for source: {self.name}")
        run_poll_loop(self.poll, interval_s=self.poll_interval)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='SFTP Fetcher')
    parser.add_argument(
        '--config', 
        default='config.yml',
        help='Path to config file'
    )
    parser.add_argument(
        '--source',
        required=True,
        help='Name of source to fetch from (must match config)'
    )
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Find source configuration
    sources = config.get('sources', [])
    source_config = None
    for src in sources:
        if src['name'] == args.source:
            source_config = src
            break
    
    if not source_config:
        raise ValueError(f"Source '{args.source}' not found in config")
    
    # Create and run fetcher
    fetcher = SFTPFetcher(source_config)
    
    try:
        fetcher.run()
    finally:
        fetcher.disconnect()


if __name__ == '__main__':
    main()
