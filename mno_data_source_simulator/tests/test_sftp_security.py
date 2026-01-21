"""Security-focused tests for SFTP uploader."""

import tempfile
import shutil
from pathlib import Path
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock
import paramiko

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from sftp_uploader import SFTPUploader


@pytest.fixture
def test_dirs():
    """Create temporary directories for testing."""
    tmp_base = tempfile.mkdtemp()
    source_dir = Path(tmp_base) / "data_to_upload"
    archive_dir = Path(tmp_base) / "data_uploaded"

    source_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    yield {
        "base": tmp_base,
        "source": str(source_dir),
        "archive": str(archive_dir),
    }

    shutil.rmtree(tmp_base)


class TestPathValidation:
    """Test remote path validation security."""

    def test_valid_absolute_path(self, test_dirs):
        """Test that valid absolute paths are accepted."""
        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload/data",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )
        assert uploader.remote_path == "/upload/data"

    def test_reject_relative_path(self, test_dirs):
        """Test that relative paths are rejected."""
        with pytest.raises(ValueError, match="must be absolute"):
            SFTPUploader(
                host="localhost",
                port=22,
                username="test",
                password="pass",
                remote_path="upload/data",  # No leading /
                source_dir=test_dirs["source"],
                archive_dir=test_dirs["archive"],
            )

    def test_reject_path_traversal(self, test_dirs):
        """Test that path traversal attempts are rejected."""
        with pytest.raises(ValueError, match="path traversal"):
            SFTPUploader(
                host="localhost",
                port=22,
                username="test",
                password="pass",
                remote_path="/upload/../../../etc",
                source_dir=test_dirs["source"],
                archive_dir=test_dirs["archive"],
            )

    def test_reject_invalid_characters(self, test_dirs):
        """Test that paths with invalid characters are rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            SFTPUploader(
                host="localhost",
                port=22,
                username="test",
                password="pass",
                remote_path="/upload/data;rm -rf /",
                source_dir=test_dirs["source"],
                archive_dir=test_dirs["archive"],
            )

    def test_path_normalization(self, test_dirs):
        """Test that paths with double slashes are normalized."""
        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload//data",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )
        # Should normalize double slashes
        assert uploader.remote_path == "/upload/data"


class TestFilenameValidation:
    """Test filename sanitization security."""

    def test_valid_filename(self, test_dirs):
        """Test that valid filenames are accepted."""
        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )
        result = uploader._sanitize_filename("data_file.csv")
        assert result == "data_file.csv"

    def test_reject_path_in_filename(self, test_dirs):
        """Test that filenames with path separators are rejected."""
        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )
        with pytest.raises(ValueError, match="Invalid filename"):
            uploader._sanitize_filename("../etc/passwd")

        with pytest.raises(ValueError, match="Invalid filename"):
            uploader._sanitize_filename("subdir/file.csv")

    def test_reject_hidden_files(self, test_dirs):
        """Test that hidden files (starting with .) are rejected."""
        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )
        with pytest.raises(ValueError, match="Hidden files not allowed"):
            uploader._sanitize_filename(".hidden_file.csv")

    def test_reject_special_characters(self, test_dirs):
        """Test that filenames with special characters are rejected."""
        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )
        with pytest.raises(ValueError, match="invalid characters"):
            uploader._sanitize_filename("file;rm -rf.csv")


class TestAuthenticationMethods:
    """Test authentication method handling."""

    @patch("paramiko.SSHClient")
    def test_ssh_key_authentication(self, mock_ssh, test_dirs):
        """Test SSH key authentication is used when configured."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client
        mock_sftp = MagicMock()
        mock_client.open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError()

        # Create a temporary key file
        key_file = Path(test_dirs["base"]) / "test_key"
        key_file.write_text("fake key content")

        with patch("paramiko.RSAKey.from_private_key_file") as mock_key:
            mock_pkey = MagicMock()
            mock_key.return_value = mock_pkey

            uploader = SFTPUploader(
                host="localhost",
                port=22,
                username="test",
                private_key_path=str(key_file),
                remote_path="/upload",
                source_dir=test_dirs["source"],
                archive_dir=test_dirs["archive"],
            )

            uploader.connect()

            # Verify key was loaded
            mock_key.assert_called_once_with(str(key_file))

            # Verify connection used the key, not password
            call_kwargs = mock_client.connect.call_args[1]
            assert "pkey" in call_kwargs
            assert call_kwargs["pkey"] == mock_pkey
            assert "password" not in call_kwargs

            uploader.close()

    @patch("paramiko.SSHClient")
    def test_password_authentication(self, mock_ssh, test_dirs):
        """Test password authentication is used when configured."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client
        mock_sftp = MagicMock()
        mock_client.open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError()

        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="secret_pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )

        uploader.connect()

        # Verify connection used password
        call_kwargs = mock_client.connect.call_args[1]
        assert "password" in call_kwargs
        assert call_kwargs["password"] == "secret_pass"
        assert "pkey" not in call_kwargs

        # Verify password was cleared after connection
        assert uploader._password is None

        uploader.close()

    def test_no_authentication_method(self, test_dirs):
        """Test that initialization fails without any auth method."""
        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            # No password or private_key_path
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )

        with pytest.raises(ValueError, match="Either password or private_key_path"):
            uploader.connect()

    @patch("paramiko.SSHClient")
    def test_invalid_private_key_file(self, mock_ssh, test_dirs):
        """Test that invalid private key files are rejected."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client

        with patch(
            "paramiko.RSAKey.from_private_key_file",
            side_effect=Exception("Invalid key"),
        ):
            uploader = SFTPUploader(
                host="localhost",
                port=22,
                username="test",
                private_key_path="/invalid/key/path",
                remote_path="/upload",
                source_dir=test_dirs["source"],
                archive_dir=test_dirs["archive"],
            )

            with pytest.raises(ValueError, match="Invalid private key file"):
                uploader.connect()


class TestHostKeyVerification:
    """Test host key verification security."""

    @patch("paramiko.SSHClient")
    def test_reject_policy_used(self, mock_ssh, test_dirs):
        """Test that RejectPolicy is used for host key verification."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client
        mock_sftp = MagicMock()
        mock_client.open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError()

        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )

        uploader.connect()

        # Verify RejectPolicy was set
        policy_call = mock_client.set_missing_host_key_policy.call_args[0][0]
        assert isinstance(policy_call, paramiko.RejectPolicy)

        uploader.close()

    @patch("paramiko.SSHClient")
    def test_known_hosts_loaded(self, mock_ssh, test_dirs):
        """Test that known_hosts file is loaded."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client
        mock_sftp = MagicMock()
        mock_client.open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError()

        # Create a temporary known_hosts file
        known_hosts = Path(test_dirs["base"]) / "known_hosts"
        known_hosts.write_text("localhost ssh-rsa AAAA...")

        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            known_hosts_path=str(known_hosts),
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )

        uploader.connect()

        # Verify known_hosts was loaded
        mock_client.load_host_keys.assert_called_once_with(str(known_hosts))

        uploader.close()


class TestConnectionTimeout:
    """Test connection timeout configuration."""

    @patch("paramiko.SSHClient")
    def test_default_timeout(self, mock_ssh, test_dirs):
        """Test that default timeout is set."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client
        mock_sftp = MagicMock()
        mock_client.open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError()

        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )

        uploader.connect()

        # Verify timeout was set to default (30 seconds)
        call_kwargs = mock_client.connect.call_args[1]
        assert call_kwargs["timeout"] == 30

        uploader.close()

    @patch("paramiko.SSHClient")
    def test_custom_timeout(self, mock_ssh, test_dirs):
        """Test that custom timeout is used."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client
        mock_sftp = MagicMock()
        mock_client.open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError()

        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            connection_timeout=60,
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )

        uploader.connect()

        # Verify custom timeout was set
        call_kwargs = mock_client.connect.call_args[1]
        assert call_kwargs["timeout"] == 60

        uploader.close()


class TestExceptionHandling:
    """Test specific exception handling."""

    @patch("paramiko.SSHClient")
    def test_authentication_exception(self, mock_ssh, test_dirs):
        """Test that authentication failures are handled correctly."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client
        mock_client.connect.side_effect = paramiko.AuthenticationException(
            "Auth failed"
        )

        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="wrong_pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )

        with pytest.raises(paramiko.AuthenticationException):
            uploader.connect()

    @patch("paramiko.SSHClient")
    def test_ssh_exception(self, mock_ssh, test_dirs):
        """Test that SSH errors are handled correctly."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client
        mock_client.connect.side_effect = paramiko.SSHException("SSH error")

        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )

        with pytest.raises(paramiko.SSHException):
            uploader.connect()

    @patch("paramiko.SSHClient")
    def test_network_error(self, mock_ssh, test_dirs):
        """Test that network errors are handled correctly."""
        mock_client = MagicMock()
        mock_ssh.return_value = mock_client
        mock_client.connect.side_effect = OSError("Network unreachable")

        uploader = SFTPUploader(
            host="localhost",
            port=22,
            username="test",
            password="pass",
            remote_path="/upload",
            source_dir=test_dirs["source"],
            archive_dir=test_dirs["archive"],
        )

        with pytest.raises(OSError):
            uploader.connect()
