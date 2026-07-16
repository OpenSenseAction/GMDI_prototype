"""Unit tests for main entry point."""

import pytest
from unittest.mock import patch, MagicMock
from io import StringIO

from fetchers.sftp_fetcher.main import main


class TestMain:
    """Test main() function."""
    
    def test_main_source_found(self, base_config, tmp_dirs, monkeypatch):
        """Test main runs when source is found."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        # Create a temporary config file
        import tempfile
        import yaml
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump({"sources": [base_config]}, f)
            config_path = f.name
        
        try:
            with patch('sys.argv', ['main', '--config', config_path, '--source', 'test_source']), \
                 patch('fetchers.sftp_fetcher.fetcher.SFTPFetcher') as mock_fetcher_class:
                
                mock_fetcher = MagicMock()
                mock_fetcher_class.return_value = mock_fetcher
                
                # Mock run_poll_loop to exit immediately
                with patch('fetchers.sftp_fetcher.fetcher.run_poll_loop') as mock_loop:
                    main()
                    
                    mock_fetcher_class.assert_called_once()
                    mock_fetcher.run.assert_called_once()
                    mock_fetcher.disconnect.assert_called_once()
        finally:
            import os
            os.unlink(config_path)
    
    def test_main_source_not_found(self, base_config, tmp_dirs, monkeypatch):
        """Test main raises error when source not found."""
        monkeypatch.setenv("TEST_SSH_KEY", "/path/to/key")
        monkeypatch.setenv("STATE_DIR", str(tmp_dirs["state"]))
        monkeypatch.setenv("INCOMING_DIR", str(tmp_dirs["incoming"]))
        
        import tempfile
        import yaml
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump({"sources": [{"name": "other_source"}]}, f)
            config_path = f.name
        
        try:
            with patch('sys.argv', ['main', '--config', config_path, '--source', 'test_source']):
                with pytest.raises(ValueError, match="Source 'test_source' not found"):
                    main()
        finally:
            import os
            os.unlink(config_path)
