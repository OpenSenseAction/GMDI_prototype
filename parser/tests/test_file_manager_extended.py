"""Extended tests for FileManager edge cases."""

from pathlib import Path
import pytest
from unittest.mock import patch, Mock
from ..file_manager import FileManager


def test_archive_file_not_found():
    """Test archiving non-existent file raises FileNotFoundError."""
    fm = FileManager("/tmp/incoming", "/tmp/archived", "/tmp/quarantine")

    with pytest.raises(FileNotFoundError):
        fm.archive_file(Path("/nonexistent/file.csv"))


def test_quarantine_file_not_found(tmp_path):
    """Test quarantining non-existent file creates error note."""
    quarantine = tmp_path / "quarantine"
    fm = FileManager(str(tmp_path / "in"), str(tmp_path / "arch"), str(quarantine))

    result = fm.quarantine_file(Path("/nonexistent/missing.csv"), "File was missing")

    assert result.exists()
    assert result.name == "missing.csv.error.txt"
    content = result.read_text()
    assert "Original file not found" in content
    assert "File was missing" in content


def test_safe_move_fallback_to_copy(tmp_path):
    """Test _safe_move falls back to copy when move fails."""
    incoming = tmp_path / "incoming"
    archived = tmp_path / "archived"
    quarantine = tmp_path / "quarantine"
    incoming.mkdir()

    fm = FileManager(str(incoming), str(archived), str(quarantine))

    f = incoming / "test.csv"
    f.write_text("data")

    # Mock shutil.move to fail, copy2 to succeed
    with patch("parser.file_manager.shutil.move") as mock_move:
        mock_move.side_effect = OSError("Cross-device link")

        dest = fm.archive_file(f)

        assert dest.exists()
        # File should be copied since move failed
        mock_move.assert_called_once()


def test_safe_move_both_fail(tmp_path):
    """Test archive fails when both move and copy fail."""
    incoming = tmp_path / "incoming"
    archived = tmp_path / "archived"
    quarantine = tmp_path / "quarantine"
    incoming.mkdir()

    fm = FileManager(str(incoming), str(archived), str(quarantine))

    f = incoming / "test.csv"
    f.write_text("data")

    with patch("parser.file_manager.shutil.move") as mock_move:
        with patch("parser.file_manager.shutil.copy2") as mock_copy:
            mock_move.side_effect = OSError("Move failed")
            mock_copy.side_effect = OSError("Copy failed")

            with pytest.raises(RuntimeError, match="Failed to archive"):
                fm.archive_file(f)


def test_quarantine_creates_orphan_on_move_copy_failure(tmp_path):
    """Test quarantine creates orphan note when both move and copy fail."""
    incoming = tmp_path / "incoming"
    quarantine = tmp_path / "quarantine"
    incoming.mkdir()

    fm = FileManager(str(incoming), str(tmp_path / "arch"), str(quarantine))

    f = incoming / "test.csv"
    f.write_text("data")

    with patch("parser.file_manager.shutil.move") as mock_move:
        with patch("parser.file_manager.shutil.copy2") as mock_copy:
            mock_move.side_effect = OSError("Move failed")
            mock_copy.side_effect = OSError("Copy failed")

            result = fm.quarantine_file(f, "Parse error")

            # Should create .orphan error note
            error_file = quarantine / "test.csv.orphan.error.txt"
            assert error_file.exists()


def test_get_archived_path(tmp_path):
    """Test getting archived path without actually moving file."""
    fm = FileManager(str(tmp_path / "in"), str(tmp_path / "arch"), str(tmp_path / "q"))

    path = fm.get_archived_path(Path("test.csv"))

    assert "test.csv" in str(path)
    assert not path.exists()  # File not actually moved


def test_quarantine_error_note_contains_timestamp(tmp_path):
    """Test quarantine error note includes timestamp."""
    incoming = tmp_path / "incoming"
    quarantine = tmp_path / "quarantine"
    incoming.mkdir()

    fm = FileManager(str(incoming), str(tmp_path / "arch"), str(quarantine))

    f = incoming / "test.csv"
    f.write_text("data")

    fm.quarantine_file(f, "Test error message")

    error_file = quarantine / "test.csv.error.txt"
    content = error_file.read_text()

    assert "Quarantined at:" in content
    assert "Test error message" in content
    assert str(f) in content
