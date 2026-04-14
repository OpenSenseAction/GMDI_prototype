"""Tests for process_rawdata_files_batch in service_logic."""

import logging
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from ..service_logic import process_rawdata_files_batch


@pytest.fixture
def mock_db_writer():
    db = MagicMock()
    db.write_rawdata.return_value = 10
    return db


@pytest.fixture
def mock_file_manager():
    return MagicMock()


def _make_raw_csv(tmp_path, name="raw_data.csv", rows=2):
    """Write a minimal valid rawdata CSV and return its Path."""
    lines = ["time,cml_id,sublink_id,tsl,rsl"]
    for i in range(rows):
        lines.append(f"2026-01-{i+1:02d} 10:00:00,CML_{i},A,{i}.0,-{i}.0")
    p = tmp_path / name
    p.write_text("\n".join(lines) + "\n")
    return p


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_single_batch_writes_and_archives(tmp_path, mock_db_writer, mock_file_manager):
    """All files in a single batch are written and archived."""
    files = [_make_raw_csv(tmp_path, f"raw_{i}.csv") for i in range(3)]

    process_rawdata_files_batch(files, mock_db_writer, mock_file_manager)

    mock_db_writer.connect.assert_called_once()
    mock_db_writer.write_rawdata.assert_called_once()
    combined = mock_db_writer.write_rawdata.call_args[0][0]
    assert isinstance(combined, pd.DataFrame)
    assert len(combined) == 6  # 3 files × 2 rows each

    assert mock_file_manager.archive_file.call_count == 3
    mock_file_manager.quarantine_file.assert_not_called()


def test_multiple_batches(tmp_path, mock_db_writer, mock_file_manager):
    """Files exceeding batch_size are split into multiple batches."""
    files = [_make_raw_csv(tmp_path, f"raw_{i}.csv") for i in range(5)]

    process_rawdata_files_batch(
        files, mock_db_writer, mock_file_manager, batch_size=2
    )

    # 5 files / batch_size=2 → 3 batches
    assert mock_db_writer.connect.call_count == 3
    assert mock_db_writer.write_rawdata.call_count == 3
    assert mock_file_manager.archive_file.call_count == 5


def test_empty_file_list_is_noop(mock_db_writer, mock_file_manager):
    """An empty file list should not touch the DB or file manager."""
    process_rawdata_files_batch([], mock_db_writer, mock_file_manager)

    mock_db_writer.connect.assert_not_called()
    mock_db_writer.write_rawdata.assert_not_called()
    mock_file_manager.archive_file.assert_not_called()
    mock_file_manager.quarantine_file.assert_not_called()


# ---------------------------------------------------------------------------
# Parse failures
# ---------------------------------------------------------------------------


def test_unparseable_file_is_quarantined(tmp_path, mock_db_writer, mock_file_manager):
    """A file that raises during parsing is quarantined; others proceed."""
    good = _make_raw_csv(tmp_path, "good.csv")
    bad = tmp_path / "bad.csv"
    bad.write_text("not,a,valid,csv\n???\n")

    with patch(
        "parser.service_logic.parse_rawdata_csv",
        side_effect=lambda p: (_ for _ in ()).throw(ValueError("bad csv"))
        if p == bad
        else pd.read_csv(p),
    ):
        process_rawdata_files_batch(
            [good, bad], mock_db_writer, mock_file_manager
        )

    mock_file_manager.quarantine_file.assert_called_once_with(
        bad, "Parse error during batch processing"
    )
    mock_file_manager.archive_file.assert_called_once_with(good)


def test_file_returning_none_is_quarantined(tmp_path, mock_db_writer, mock_file_manager):
    """A file whose parser returns None is quarantined."""
    f = _make_raw_csv(tmp_path, "raw.csv")

    with patch("parser.service_logic.parse_rawdata_csv", return_value=None):
        process_rawdata_files_batch([f], mock_db_writer, mock_file_manager)

    mock_file_manager.quarantine_file.assert_called_once_with(
        f, "Parse error during batch processing"
    )
    mock_db_writer.write_rawdata.assert_not_called()


def test_file_returning_empty_df_is_quarantined(tmp_path, mock_db_writer, mock_file_manager):
    """A file whose parser returns an empty DataFrame is quarantined."""
    f = _make_raw_csv(tmp_path, "raw.csv")

    with patch(
        "parser.service_logic.parse_rawdata_csv",
        return_value=pd.DataFrame(),
    ):
        process_rawdata_files_batch([f], mock_db_writer, mock_file_manager)

    mock_file_manager.quarantine_file.assert_called_once_with(
        f, "Parse error during batch processing"
    )
    mock_db_writer.write_rawdata.assert_not_called()


def test_all_files_fail_parse_skips_write(tmp_path, mock_db_writer, mock_file_manager):
    """When every file in a batch fails to parse, the DB write is skipped."""
    files = [_make_raw_csv(tmp_path, f"raw_{i}.csv") for i in range(3)]

    with patch("parser.service_logic.parse_rawdata_csv", return_value=None):
        process_rawdata_files_batch(files, mock_db_writer, mock_file_manager)

    mock_db_writer.write_rawdata.assert_not_called()
    assert mock_file_manager.quarantine_file.call_count == 3


# ---------------------------------------------------------------------------
# DB write failure
# ---------------------------------------------------------------------------


def test_db_write_failure_leaves_files_in_place(
    tmp_path, mock_db_writer, mock_file_manager
):
    """If the DB write raises, files are NOT archived (available for retry)."""
    files = [_make_raw_csv(tmp_path, f"raw_{i}.csv") for i in range(2)]
    mock_db_writer.write_rawdata.side_effect = Exception("DB down")

    process_rawdata_files_batch(files, mock_db_writer, mock_file_manager)

    mock_file_manager.archive_file.assert_not_called()
    mock_file_manager.quarantine_file.assert_not_called()
