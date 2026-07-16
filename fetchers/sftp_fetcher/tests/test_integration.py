"""Integration tests with real in-process SFTP server."""

import os
import time
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch

# Try to import pytest-sftpserver; skip tests if not available
try:
    from pytest_sftpserver.sftp import SFTPTestServer
    HAS_SFTP_SERVER = True
except ImportError:
    HAS_SFTP_SERVER = False


pytestmark = pytest.mark.skipif(
    not HAS_SFTP_SERVER,
    reason="pytest-sftpserver not installed (pip install pytest-sftpserver)"
)


class TestSFTPFetcherIntegration:
    """Integration tests with real SFTP server."""
    
    def test_download_and_write(self, tmp_path, base_config):
        """Test complete flow: upload to SFTP → fetcher downloads → writes to incoming."""
        # Setup directories
        state_dir = tmp_path / "state"
        incoming_dir = tmp_path / "incoming"
        sftp_root = tmp_path / "sftp_root"
        remote_dir = sftp_root / "outgoing" / "cml"
        remote_dir.mkdir(parents=True)
        
        state_dir.mkdir()
        incoming_dir.mkdir()
        
        # Create a test file on the mock SFTP server
        test_file = remote_dir / "test_data.csv"
        test_content = b"time,cml_id,tsl,rsl\n2024-01-01,test,25.5,-45.2\n"
        test_file.write_bytes(test_content)
        
        # Configure fetcher
        config = base_config.copy()
        config['host'] = '127.0.0.1'
        config['remote_path'] = str(remote_dir)
        config['private_key_env'] = None  # Will be mocked
        
        os.environ['STATE_DIR'] = str(state_dir)
        os.environ['INCOMING_DIR'] = str(incoming_dir)
        os.environ['TEST_PASSWORD'] = 'testpass'
        config['password_env'] = 'TEST_PASSWORD'
        
        # Start mock SFTP server
        with SFTPTestServer(address=('127.0.0.1', 0)) as server:
            config['host'] = server.host
            config['port'] = server.port
            
            from fetchers.sftp_fetcher.fetcher import SFTPFetcher
            
            fetcher = SFTPFetcher(config)
            
            try:
                count = fetcher.poll()
                
                assert count == 1
                assert (incoming_dir / "test_data.csv").exists()
                assert (incoming_dir / "test_data.csv").read_bytes() == test_content
                
                # Verify state was updated
                assert fetcher.state.is_seen("test_source", "test_data.csv", str(test_file.stat().st_mtime))
                
            finally:
                fetcher.disconnect()
    
    def test_skip_already_downloaded(self, tmp_path, base_config):
        """Test that already-downloaded files are skipped on second poll."""
        state_dir = tmp_path / "state"
        incoming_dir = tmp_path / "incoming"
        sftp_root = tmp_path / "sftp_root"
        remote_dir = sftp_root / "outgoing" / "cml"
        remote_dir.mkdir(parents=True)
        
        state_dir.mkdir()
        incoming_dir.mkdir()
        
        test_file = remote_dir / "data.csv"
        test_content = b"test,data\n1,2\n"
        test_file.write_bytes(test_content)
        
        config = base_config.copy()
        config['password_env'] = 'TEST_PASSWORD'
        os.environ['STATE_DIR'] = str(state_dir)
        os.environ['INCOMING_DIR'] = str(incoming_dir)
        os.environ['TEST_PASSWORD'] = 'testpass'
        
        with SFTPTestServer(address=('127.0.0.1', 0)) as server:
            config['host'] = server.host
            config['port'] = server.port
            
            from fetchers.sftp_fetcher.fetcher import SFTPFetcher
            
            fetcher = SFTPFetcher(config)
            
            try:
                # First poll - should download
                count1 = fetcher.poll()
                assert count1 == 1
                
                # Second poll - should skip
                count2 = fetcher.poll()
                assert count2 == 0
                
            finally:
                fetcher.disconnect()
    
    def test_move_after_download(self, tmp_path, base_config):
        """Test that files are moved after download when configured."""
        state_dir = tmp_path / "state"
        incoming_dir = tmp_path / "incoming"
        sftp_root = tmp_path / "sftp_root"
        remote_dir = sftp_root / "outgoing" / "cml"
        archive_dir = sftp_root / "outgoing" / "cml" / "done"
        remote_dir.mkdir(parents=True)
        
        state_dir.mkdir()
        incoming_dir.mkdir()
        
        test_file = remote_dir / "move_test.csv"
        test_content = b"move,test\n"
        test_file.write_bytes(test_content)
        
        config = base_config.copy()
        config['after_download'] = 'move'
        config['archive_subdir'] = 'done'
        config['password_env'] = 'TEST_PASSWORD'
        os.environ['STATE_DIR'] = str(state_dir)
        os.environ['INCOMING_DIR'] = str(incoming_dir)
        os.environ['TEST_PASSWORD'] = 'testpass'
        
        with SFTPTestServer(address=('127.0.0.1', 0)) as server:
            config['host'] = server.host
            config['port'] = server.port
            
            from fetchers.sftp_fetcher.fetcher import SFTPFetcher
            
            fetcher = SFTPFetcher(config)
            
            try:
                count = fetcher.poll()
                assert count == 1
                
                # File should be moved, not in original location
                assert not test_file.exists()
                assert (archive_dir / "move_test.csv").exists()
                
            finally:
                fetcher.disconnect()
