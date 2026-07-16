"""Unit tests for SFTPFetcher class."""

import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock

from fetchers.sftp_fetcher.fetcher import SFTPFetcher


class TestSFTPFetcherInit:
    """Test SFTPFetcher initialization."""
    
    def test_init_with_ssh_key(self, base_config, tmp_dirs, monkeypatch):
        """Test initialization with SSH key authentication."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        fetcher = SFTPFetcher(base_config)
        
        assert fetcher.name == "test_source"
        assert fetcher.host == "sftp.example.com"
        assert fetcher.port == 22
        assert fetcher.username == "test_user"
        assert fetcher.private_key_path == "/path/to/key"
        assert fetcher.password is None
    
    def test_init_with_password(self, base_config, tmp_dirs, monkeypatch):
        """Test initialization with password authentication."""
        # Remove private_key_env from config
        config = base_config.copy()
        del config['private_key_env']
        config['password_env'] = "TEST_PASSWORD"
        
        monkeypatch.setenv("TEST_PASSWORD", "secret123")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        fetcher = SFTPFetcher(config)
        
        assert fetcher.password == "secret123"
        assert fetcher.private_key_path is None
    
    def test_init_no_auth_raises(self, base_config, tmp_dirs, monkeypatch):
        """Test that missing auth raises ValueError."""
        config = base_config.copy()
        del config['private_key_env']
        
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        with pytest.raises(ValueError, match="must specify either"):
            SFTPFetcher(config)
    
    def test_init_default_values(self, base_config, tmp_dirs, monkeypatch):
        """Test default values for optional config fields."""
        config = {
            "name": "test",
            "host": "sftp.test.com",
            "username": "user",
            "private_key_env": "KEY",
            "remote_path": "/data",
        }
        
        monkeypatch.setenv("KEY", "/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        fetcher = SFTPFetcher(config)
        
        assert fetcher.port == 22
        assert fetcher.file_glob == "*"
        assert fetcher.after_download == "leave"
        assert fetcher.poll_interval == 60
        assert fetcher.archive_subdir == "done"


class TestConnectDisconnect:
    """Test connect and disconnect methods."""
    
    def test_connect_with_ssh_key(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test connection using SSH key."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        fetcher = SFTPFetcher(base_config)
        fetcher.connect()
        
        patched_paramiko['transport_class'].assert_called_once_with(("sftp.example.com", 22))
        patched_paramiko['rsa_key'].from_private_key_file.assert_called_once_with("/path/to/key")
        assert fetcher.sftp is not None
    
    def test_connect_with_password(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test connection using password."""
        config = base_config.copy()
        del config['private_key_env']
        config['password_env'] = "TEST_PASSWORD"
        
        monkeypatch.setenv("TEST_PASSWORD", "secret123")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        fetcher = SFTPFetcher(config)
        fetcher.connect()
        
        patched_paramiko['transport'].connect.assert_called_once_with(
            username="test_user", 
            password="secret123"
        )
    
    def test_disconnect(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test disconnect closes connections."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        fetcher = SFTPFetcher(base_config)
        fetcher.connect()
        fetcher.disconnect()
        
        patched_paramiko['sftp'].close.assert_called_once()
        patched_paramiko['transport'].close.assert_called_once()
        assert fetcher.sftp is None


class TestListRemoteFiles:
    """Test list_remote_files method."""
    
    def test_list_files_success(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test listing files with glob filter."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        # Mock directory entries
        mock_entry1 = MagicMock()
        mock_entry1.filename = "data.csv"
        mock_entry1.st_size = 1024
        mock_entry1.st_mtime = 1234567890
        mock_entry1.st_mode = 0o100644  # Regular file
        
        mock_entry2 = MagicMock()
        mock_entry2.filename = "metadata.csv"
        mock_entry2.st_size = 512
        mock_entry2.st_mtime = 1234567891
        mock_entry2.st_mode = 0o100644
        
        mock_dir = MagicMock()
        mock_dir.st_mode = 0o040755  # Directory
        mock_dir.filename = "subdir"
        
        patched_paramiko['sftp'].listdir_attr.return_value = [mock_entry1, mock_entry2, mock_dir]
        
        fetcher = SFTPFetcher(base_config)
        files = fetcher.list_remote_files()
        
        assert len(files) == 2
        assert files[0]['filename'] == "data.csv"
        assert files[0]['size'] == 1024
        assert 'mtime' in files[0]
    
    def test_list_files_glob_filter(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test that glob filter works correctly."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        base_config['file_glob'] = "*.json"
        
        mock_csv = MagicMock()
        mock_csv.filename = "data.csv"
        mock_csv.st_mode = 0o100644
        
        mock_json = MagicMock()
        mock_json.filename = "config.json"
        mock_json.st_mode = 0o100644
        
        patched_paramiko['sftp'].listdir_attr.return_value = [mock_csv, mock_json]
        
        fetcher = SFTPFetcher(base_config)
        files = fetcher.list_remote_files()
        
        assert len(files) == 1
        assert files[0]['filename'] == "config.json"
    
    def test_list_files_remote_missing(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test handling of missing remote directory."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        patched_paramiko['sftp'].listdir_attr.side_effect = FileNotFoundError()
        
        fetcher = SFTPFetcher(base_config)
        files = fetcher.list_remote_files()
        
        assert files == []


class TestFileOperations:
    """Test download, delete, and move operations."""
    
    def test_download_file(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test downloading a file."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        fetcher = SFTPFetcher(base_config)
        content = fetcher.download_file("test.csv")
        
        patched_paramiko['sftp'].open.assert_called_once_with("/outgoing/cml/test.csv", 'rb')
        assert content == b"test,content\n1,2,3"
    
    def test_delete_remote_file(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test deleting a remote file."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        fetcher = SFTPFetcher(base_config)
        fetcher.delete_remote_file("old.csv")
        
        patched_paramiko['sftp'].remove.assert_called_once_with("/outgoing/cml/old.csv")
    
    def test_move_remote_file_dir_exists(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test moving file when target directory exists."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        # Simulate directory already exists
        patched_paramiko['sftp'].stat.return_value = MagicMock()
        
        fetcher = SFTPFetcher(base_config)
        fetcher.move_remote_file("file.csv", "archive")
        
        patched_paramiko['sftp'].rename.assert_called_once_with(
            "/outgoing/cml/file.csv", 
            "/outgoing/cml/archive/file.csv"
        )
    
    def test_move_remote_file_dir_missing(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test moving file creates target directory if missing."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        fetcher = SFTPFetcher(base_config)
        fetcher.move_remote_file("file.csv", "done")
        
        patched_paramiko['sftp'].mkdir.assert_called_once_with("/outgoing/cml/done")
        patched_paramiko['sftp'].rename.assert_called_once()


class TestPoll:
    """Test poll method - the core business logic."""
    
    def test_poll_new_files(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test polling downloads new files."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        # Mock a new file
        mock_entry = MagicMock()
        mock_entry.filename = "new_data.csv"
        mock_entry.st_size = 2048
        mock_entry.st_mtime = 1234567890
        mock_entry.st_mode = 0o100644
        patched_paramiko['sftp'].listdir_attr.return_value = [mock_entry]
        
        fetcher = SFTPFetcher(base_config)
        count = fetcher.poll()
        
        assert count == 1
        # Verify state was updated
        assert fetcher.state.is_seen("test_source", "new_data.csv", "1234567890")
        # Verify file was written
        assert (tmp_dirs["incoming"] / "new_data.csv").exists()
    
    def test_poll_skip_seen_files(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test that already-seen files are skipped."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        mock_entry = MagicMock()
        mock_entry.filename = "seen.csv"
        mock_entry.st_size = 1024
        mock_entry.st_mtime = 1234567890
        mock_entry.st_mode = 0o100644
        patched_paramiko['sftp'].listdir_attr.return_value = [mock_entry]
        
        fetcher = SFTPFetcher(base_config)
        
        # Mark as seen
        fetcher.state.mark_seen("test_source", "seen.csv", "1234567890")
        
        count = fetcher.poll()
        
        assert count == 0
        patched_paramiko['sftp'].open.assert_not_called()
    
    def test_poll_after_download_delete(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test poll deletes file after download."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        base_config['after_download'] = 'delete'
        
        mock_entry = MagicMock()
        mock_entry.filename = "temp.csv"
        mock_entry.st_size = 512
        mock_entry.st_mtime = 1234567890
        mock_entry.st_mode = 0o100644
        patched_paramiko['sftp'].listdir_attr.return_value = [mock_entry]
        
        fetcher = SFTPFetcher(base_config)
        fetcher.poll()
        
        patched_paramiko['sftp'].remove.assert_called_once_with("/outgoing/cml/temp.csv")
    
    def test_poll_after_download_move(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test poll moves file after download."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        base_config['after_download'] = 'move'
        base_config['archive_subdir'] = "archived"
        
        mock_entry = MagicMock()
        mock_entry.filename = "processed.csv"
        mock_entry.st_size = 1024
        mock_entry.st_mtime = 1234567890
        mock_entry.st_mode = 0o100644
        patched_paramiko['sftp'].listdir_attr.return_value = [mock_entry]
        
        fetcher = SFTPFetcher(base_config)
        fetcher.poll()
        
        patched_paramiko['sftp'].rename.assert_called_once_with(
            "/outgoing/cml/processed.csv",
            "/outgoing/cml/archived/processed.csv"
        )
    
    def test_poll_download_failure_continues(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test that download failure doesn't stop processing other files."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        # Two files: first fails, second succeeds
        mock_fail = MagicMock()
        mock_fail.filename = "fail.csv"
        mock_fail.st_mode = 0o100644
        
        mock_success = MagicMock()
        mock_success.filename = "success.csv"
        mock_success.st_size = 1024
        mock_success.st_mtime = 1234567890
        mock_success.st_mode = 0o100644
        
        patched_paramiko['sftp'].listdir_attr.return_value = [mock_fail, mock_success]
        
        # Fail only on first call (for fail.csv), succeed on second (success.csv)
        call_count = [0]
        def open_side_effect(path, mode):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Download failed")
            # Return a new mock for successful read
            mock_file = MagicMock()
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=False)
            mock_file.read.return_value = b"success,data"
            return mock_file
        
        patched_paramiko['sftp'].open.side_effect = open_side_effect
        
        fetcher = SFTPFetcher(base_config)
        count = fetcher.poll()
        
        # Should have downloaded only the successful file
        assert count == 1
        # Failed file should not be marked as seen
        assert not fetcher.state.is_seen("test_source", "fail.csv", "1234567890")
    
    def test_poll_reconnect_if_needed(self, base_config, tmp_dirs, monkeypatch, patched_paramiko):
        """Test that poll reconnects if sftp is None."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        mock_entry = MagicMock()
        mock_entry.filename = "test.csv"
        mock_entry.st_size = 100
        mock_entry.st_mtime = 1234567890
        mock_entry.st_mode = 0o100644
        patched_paramiko['sftp'].listdir_attr.return_value = [mock_entry]
        
        fetcher = SFTPFetcher(base_config)
        # Force sftp to be None
        fetcher.sftp = None
        
        fetcher.poll()
        
        # Should have reconnected
        assert fetcher.sftp is not None
