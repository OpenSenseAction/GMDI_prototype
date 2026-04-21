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


class TestEnvVarOverrides:
    """Test that SFTP env var overrides are applied to config before use."""

    @patch("main.ensure_netcdf_file")
    @patch("main.CMLDataGenerator")
    @patch("main.SFTPUploader")
    def test_sftp_env_vars_override_config(
        self, mock_sftp, mock_generator, mock_ensure, base_config
    ):
        """All SFTP_* env vars override the corresponding config.yml values."""
        config = {k: dict(v) if isinstance(v, dict) else v for k, v in base_config.items()}
        # Start with no key so auth is via the env-var override
        config["sftp"].pop("private_key_path", None)

        mock_sftp_instance = MagicMock()
        mock_sftp.return_value = mock_sftp_instance

        env_overrides = {
            "SFTP_HOST": "env-host",
            "SFTP_PORT": "2222",
            "SFTP_USERNAME": "env-user",
            "SFTP_REMOTE_PATH": "/env/path",
            "SFTP_PRIVATE_KEY_PATH": "/env/key",
            "SFTP_KNOWN_HOSTS_PATH": "/env/known_hosts",
        }
        os.environ.pop("SFTP_PASSWORD", None)

        import main

        with patch("main.load_config", return_value=config):
            with patch.dict(os.environ, env_overrides, clear=False):
                with patch("main.time.sleep", side_effect=KeyboardInterrupt):
                    try:
                        main.main()
                    except KeyboardInterrupt:
                        pass

        mock_sftp.assert_called_once()
        call_kwargs = mock_sftp.call_args[1]
        assert call_kwargs["host"] == "env-host"
        assert call_kwargs["port"] == 2222
        assert call_kwargs["username"] == "env-user"
        assert call_kwargs["remote_path"] == "/env/path"
        assert call_kwargs["private_key_path"] == "/env/key"
        assert call_kwargs["known_hosts_path"] == "/env/known_hosts"

    @patch("main.ensure_netcdf_file")
    @patch("main.CMLDataGenerator")
    @patch("main.logger")
    def test_sftp_use_ssh_key_false_removes_key_path(
        self, mock_logger, mock_generator, mock_ensure, base_config
    ):
        """SFTP_USE_SSH_KEY=false removes private_key_path from config."""
        config = {k: dict(v) if isinstance(v, dict) else v for k, v in base_config.items()}
        config["sftp"]["private_key_path"] = "/original/key"
        os.environ.pop("SFTP_PASSWORD", None)

        import main

        with patch("main.load_config", return_value=config):
            with patch.dict(os.environ, {"SFTP_USE_SSH_KEY": "false"}, clear=False):
                with patch("main.time.sleep", side_effect=KeyboardInterrupt):
                    try:
                        main.main()
                    except KeyboardInterrupt:
                        pass

        # With key removed and no password, SFTP should warn about no auth method
        warning_calls = [c for c in mock_logger.warning.call_args_list if c[0]]
        assert any("No SFTP authentication method" in str(c) for c in warning_calls)
