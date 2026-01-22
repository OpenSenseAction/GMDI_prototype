"""Basic tests for FileWatcher and FileUploadHandler."""

import tempfile
import shutil
import time
from pathlib import Path
import pytest
from ..file_watcher import FileWatcher, FileUploadHandler


def test_fileuploadhandler_triggers_callback(tmp_path):
    """Test that FileUploadHandler calls the callback for supported files."""
    called = {}

    def cb(filepath):
        called["path"] = filepath

    handler = FileUploadHandler(cb, [".csv"])
    # Simulate file creation event
    test_file = tmp_path / "test.csv"
    test_file.write_text("dummy")
    event = type("FakeEvent", (), {"is_directory": False, "src_path": str(test_file)})()
    handler.on_created(event)
    assert called["path"] == test_file


def test_filewatcher_start_stop(tmp_path):
    """Test FileWatcher can start and stop without error."""

    def cb(filepath):
        pass

    watcher = FileWatcher(str(tmp_path), cb, [".csv"])
    watcher.start()
    time.sleep(0.2)
    watcher.stop()


def test_fileuploadhandler_ignores_unsupported(tmp_path):
    """Test FileUploadHandler ignores unsupported file extensions."""
    called = False

    def cb(filepath):
        nonlocal called
        called = True

    handler = FileUploadHandler(cb, [".csv"])
    test_file = tmp_path / "test.txt"
    test_file.write_text("dummy")
    event = type("FakeEvent", (), {"is_directory": False, "src_path": str(test_file)})()
    handler.on_created(event)
    assert not called
