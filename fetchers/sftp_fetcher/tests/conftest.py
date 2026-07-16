"""Fixtures for SFTP fetcher tests."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch


@pytest.fixture
def tmp_dirs(tmp_path):
    """Create temporary directories for testing."""
    state_dir = tmp_path / "state"
    incoming_dir = tmp_path / "incoming"
    state_dir.mkdir()
    incoming_dir.mkdir()
    return {
        "state": state_dir,
        "incoming": incoming_dir,
    }


@pytest.fixture
def base_config(tmp_dirs):
    """Base configuration dictionary for SFTPFetcher."""
    return {
        "name": "test_source",
        "host": "sftp.example.com",
        "port": 22,
        "username": "test_user",
        "private_key_env": "TEST_SSH_KEY",
        "remote_path": "/outgoing/cml",
        "poll_interval_seconds": 60,
        "file_glob": "*.csv",
        "after_download": "leave",
        "archive_subdir": "done",
    }


@pytest.fixture
def patched_paramiko():
    """Patch paramiko.Transport and SFTPClient."""
    # Create mock SFTP client
    mock_sftp = MagicMock()
    
    # Mock stat to simulate directory existence check
    def stat_side_effect(path):
        if "done" in path or "archive" in path:
            raise FileNotFoundError(f"Directory {path} not found")
        return MagicMock()
    
    mock_sftp.stat.side_effect = stat_side_effect
    
    # Mock file operations
    mock_file = MagicMock()
    mock_file.__enter__ = Mock(return_value=mock_file)
    mock_file.__exit__ = Mock(return_value=False)
    mock_file.read.return_value = b"test,content\n1,2,3"
    mock_sftp.open.return_value = mock_file
    
    # Create mock transport
    mock_transport = MagicMock()
    
    # Mock SFTPClient.from_transport to return our mock
    with patch('fetchers.sftp_fetcher.fetcher.paramiko.Transport') as mock_transport_class, \
         patch('fetchers.sftp_fetcher.fetcher.paramiko.RSAKey') as mock_rsa_key, \
         patch('fetchers.sftp_fetcher.fetcher.paramiko.SFTPClient') as mock_sftp_client_class:
        
        mock_transport_class.return_value = mock_transport
        mock_rsa_key.from_private_key_file.return_value = MagicMock()
        mock_sftp_client_class.from_transport.return_value = mock_sftp
        
        yield {
            'transport_class': mock_transport_class,
            'transport': mock_transport,
            'sftp': mock_sftp,
            'rsa_key': mock_rsa_key,
            'sftp_client_class': mock_sftp_client_class,
        }
