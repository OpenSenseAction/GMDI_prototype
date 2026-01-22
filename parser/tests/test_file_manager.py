from pathlib import Path
import os

from ..file_manager import FileManager


def test_archive_and_quarantine(tmp_path):
    incoming = tmp_path / "incoming"
    archived = tmp_path / "archived"
    quarantine = tmp_path / "quarantine"
    incoming.mkdir()

    fm = FileManager(str(incoming), str(archived), str(quarantine))

    # create a file to archive
    f = incoming / "testfile.csv"
    f.write_text("hello")

    archived_path = fm.archive_file(f)
    assert archived_path.exists()
    assert not f.exists()

    # create a file to quarantine
    f2 = incoming / "bad.csv"
    f2.write_text("bad")
    qpath = fm.quarantine_file(f2, "parse error")
    assert qpath.exists()
    errfile = quarantine / (qpath.name + ".error.txt")
    # error file should exist
    assert errfile.exists()
