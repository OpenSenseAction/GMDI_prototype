"""Tests for main.py authentication validation logic."""

import sys
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import yaml

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def temp_config_dir():
    """Create temporary directory for config files."""
    tmp_dir = tempfile.mkdtemp()
    yield tmp_dir
    shutil.rmtree(tmp_dir)


@pytest.fixture
def base_config():
    """Base configuration for testing."""
    return {
        "data_source": {
            "netcdf_file": "/app/example_data/test.nc",
            "loop_duration_seconds": 3600,
        },
        "generator": {
            "generation_frequency_seconds": 60,
            "output_dir": "data_to_upload",
        },
        "sftp": {
            "enabled": True,
            "host": "localhost",
            "port": 22,
            "username": "test_user",
            "remote_path": "/upload",
            "upload_frequency_seconds": 60,
            "connection_timeout": 30,
        },
        "file_management": {
            "source_dir": "data_to_upload",
            "archive_dir": "data_uploaded",
        },
    }


class TestAuthenticationValidation:
    """Test authentication method validation in main.py."""

    @patch("main.CMLDataGenerator")
    @patch("main.SFTPUploader")
    def test_password_auth_only(
        self, mock_sftp, mock_generator, base_config, temp_config_dir
    ):
        """Test that password-only authentication works."""
        # Write config file
        config_file = Path(temp_config_dir) / "config.yml"
        config = base_config.copy()
        config["sftp"]["private_key_path"] = None
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Set password in environment
        os.environ["SFTP_PASSWORD"] = "test_password"

        # Mock SFTP uploader
        mock_sftp_instance = MagicMock()
        mock_sftp.return_value = mock_sftp_instance

        # Import and run main (with mocked components)
        with patch("main.load_config", return_value=config):
            with patch("main.time.sleep", side_effect=KeyboardInterrupt):
                # This should not raise an error
                import main

                try:
                    main.main()
                except KeyboardInterrupt:
                    pass

                # Verify SFTPUploader was created with password
                mock_sftp.assert_called_once()
                call_kwargs = mock_sftp.call_args[1]
                assert call_kwargs["password"] == "test_password"
                assert call_kwargs["private_key_path"] is None

        # Clean up
        del os.environ["SFTP_PASSWORD"]

    @patch("main.CMLDataGenerator")
    @patch("main.SFTPUploader")
    def test_ssh_key_auth_only(
        self, mock_sftp, mock_generator, base_config, temp_config_dir
    ):
        """Test that SSH key-only authentication works."""
        # Create a fake key file
        key_file = Path(temp_config_dir) / "test_key"
        key_file.write_text("fake key")

        # Write config file
        config_file = Path(temp_config_dir) / "config.yml"
        config = base_config.copy()
        config["sftp"]["private_key_path"] = str(key_file)
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Ensure no password in environment
        os.environ.pop("SFTP_PASSWORD", None)

        # Mock SFTP uploader
        mock_sftp_instance = MagicMock()
        mock_sftp.return_value = mock_sftp_instance

        # Import and run main
        with patch("main.load_config", return_value=config):
            with patch("main.time.sleep", side_effect=KeyboardInterrupt):
                import main

                try:
                    main.main()
                except KeyboardInterrupt:
                    pass

                # Verify SFTPUploader was created with key path
                mock_sftp.assert_called_once()
                call_kwargs = mock_sftp.call_args[1]
                assert call_kwargs["password"] is None
                assert call_kwargs["private_key_path"] == str(key_file)

    @patch("main.CMLDataGenerator")
    @patch("main.logger")
    def test_both_auth_methods_configured(
        self, mock_logger, mock_generator, base_config, temp_config_dir
    ):
        """Test that both auth methods configured triggers error."""
        # Create a fake key file
        key_file = Path(temp_config_dir) / "test_key"
        key_file.write_text("fake key")

        # Write config file
        config_file = Path(temp_config_dir) / "config.yml"
        config = base_config.copy()
        config["sftp"]["private_key_path"] = str(key_file)
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Set password in environment
        os.environ["SFTP_PASSWORD"] = "test_password"

        # Import and run main
        with patch("main.load_config", return_value=config):
            with patch("main.time.sleep", side_effect=KeyboardInterrupt):
                import main

                try:
                    main.main()
                except KeyboardInterrupt:
                    pass

                # Verify error was logged
                error_calls = [
                    call for call in mock_logger.error.call_args_list if call[0]
                ]
                assert any(
                    "Multiple SFTP authentication methods" in str(call)
                    for call in error_calls
                )

        # Clean up
        del os.environ["SFTP_PASSWORD"]

    @patch("main.CMLDataGenerator")
    @patch("main.logger")
    def test_no_auth_method_configured(
        self, mock_logger, mock_generator, base_config, temp_config_dir
    ):
        """Test that no auth method configured triggers warning."""
        # Write config file
        config_file = Path(temp_config_dir) / "config.yml"
        config = base_config.copy()
        config["sftp"]["private_key_path"] = None
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Ensure no password in environment
        os.environ.pop("SFTP_PASSWORD", None)

        # Import and run main
        with patch("main.load_config", return_value=config):
            with patch("main.time.sleep", side_effect=KeyboardInterrupt):
                import main

                try:
                    main.main()
                except KeyboardInterrupt:
                    pass

                # Verify warning was logged
                warning_calls = [
                    call for call in mock_logger.warning.call_args_list if call[0]
                ]
                assert any(
                    "No SFTP authentication method" in str(call)
                    for call in warning_calls
                )

    @patch("main.CMLDataGenerator")
    @patch("main.SFTPUploader")
    def test_expanduser_on_key_path(
        self, mock_sftp, mock_generator, base_config, temp_config_dir
    ):
        """Test that ~ in key path is expanded."""
        # Write config file with ~ in path
        config_file = Path(temp_config_dir) / "config.yml"
        config = base_config.copy()
        config["sftp"]["private_key_path"] = "~/.ssh/test_key"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Ensure no password in environment
        os.environ.pop("SFTP_PASSWORD", None)

        # Mock SFTP uploader
        mock_sftp_instance = MagicMock()
        mock_sftp.return_value = mock_sftp_instance

        # Import and run main
        with patch("main.load_config", return_value=config):
            with patch("main.time.sleep", side_effect=KeyboardInterrupt):
                import main

                try:
                    main.main()
                except KeyboardInterrupt:
                    pass

                # Verify path was expanded
                mock_sftp.assert_called_once()
                call_kwargs = mock_sftp.call_args[1]
                assert call_kwargs["private_key_path"] is not None
                assert not call_kwargs["private_key_path"].startswith("~")
                assert call_kwargs["private_key_path"].startswith(
                    os.path.expanduser("~")
                )

    @patch("main.CMLDataGenerator")
    @patch("main.SFTPUploader")
    def test_expanduser_on_known_hosts_path(
        self, mock_sftp, mock_generator, base_config, temp_config_dir
    ):
        """Test that ~ in known_hosts path is expanded."""
        # Write config file with ~ in path
        config_file = Path(temp_config_dir) / "config.yml"
        config = base_config.copy()
        config["sftp"]["known_hosts_path"] = "~/.ssh/known_hosts"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Set password in environment
        os.environ["SFTP_PASSWORD"] = "test_password"

        # Mock SFTP uploader
        mock_sftp_instance = MagicMock()
        mock_sftp.return_value = mock_sftp_instance

        # Import and run main
        with patch("main.load_config", return_value=config):
            with patch("main.time.sleep", side_effect=KeyboardInterrupt):
                import main

                try:
                    main.main()
                except KeyboardInterrupt:
                    pass

                # Verify path was expanded
                mock_sftp.assert_called_once()
                call_kwargs = mock_sftp.call_args[1]
                assert call_kwargs["known_hosts_path"] is not None
                assert not call_kwargs["known_hosts_path"].startswith("~")
                assert call_kwargs["known_hosts_path"].startswith(
                    os.path.expanduser("~")
                )

        # Clean up
        del os.environ["SFTP_PASSWORD"]
