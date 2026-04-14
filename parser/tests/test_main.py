"""Tests for process_existing_files in parser/main.py."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call


from ..main import process_existing_files


@pytest.fixture
def mock_db_writer():
    return MagicMock()


@pytest.fixture
def mock_file_manager():
    return MagicMock()


@pytest.fixture
def logger():
    return MagicMock()


def _write_csv(directory: Path, name: str) -> Path:
    p = directory / name
    p.write_text("time,cml_id,sublink_id,tsl,rsl\n2026-01-01 00:00:00,A,B,1.0,-1.0\n")
    return p


# ---------------------------------------------------------------------------
# Helpers to patch Config.INCOMING_DIR
# ---------------------------------------------------------------------------

def _run(tmp_path, db_writer, file_manager, logger):
    with patch("parser.main.Config.INCOMING_DIR", tmp_path):
        process_existing_files(db_writer, file_manager, logger)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_no_files_is_noop(tmp_path, mock_db_writer, mock_file_manager, logger):
    """Empty incoming directory returns early without calling anything."""
    _run(tmp_path, mock_db_writer, mock_file_manager, logger)

    mock_db_writer.connect.assert_not_called()
    mock_file_manager.archive_file.assert_not_called()


def test_data_files_only_use_batch_processing(
    tmp_path, mock_db_writer, mock_file_manager, logger
):
    """Data files are forwarded to process_rawdata_files_batch."""
    files = [_write_csv(tmp_path, f"raw_data_{i}.csv") for i in range(3)]

    with patch("parser.main.process_rawdata_files_batch") as mock_batch, \
         patch("parser.main.process_cml_file") as mock_single, \
         patch("parser.main.Config.INCOMING_DIR", tmp_path):
        process_existing_files(mock_db_writer, mock_file_manager, logger)

    mock_single.assert_not_called()
    mock_batch.assert_called_once()
    passed_files = mock_batch.call_args[0][0]
    assert set(passed_files) == set(files)


def test_metadata_files_only_use_individual_processing(
    tmp_path, mock_db_writer, mock_file_manager, logger
):
    """Metadata files are processed individually via process_cml_file."""
    meta1 = _write_csv(tmp_path, "metadata_links.csv")
    meta2 = _write_csv(tmp_path, "meta_extra.csv")

    with patch("parser.main.process_rawdata_files_batch") as mock_batch, \
         patch("parser.main.process_cml_file") as mock_single, \
         patch("parser.main.Config.INCOMING_DIR", tmp_path):
        process_existing_files(mock_db_writer, mock_file_manager, logger)

    assert mock_single.call_count == 2
    mock_batch.assert_not_called()


def test_mixed_files_routes_to_correct_handlers(
    tmp_path, mock_db_writer, mock_file_manager, logger
):
    """Metadata files go to process_cml_file; data files go to the batch function."""
    meta = _write_csv(tmp_path, "metadata_links.csv")
    data1 = _write_csv(tmp_path, "raw_data_001.csv")
    data2 = _write_csv(tmp_path, "raw_data_002.csv")

    with patch("parser.main.process_rawdata_files_batch") as mock_batch, \
         patch("parser.main.process_cml_file") as mock_single, \
         patch("parser.main.Config.INCOMING_DIR", tmp_path):
        process_existing_files(mock_db_writer, mock_file_manager, logger)

    mock_single.assert_called_once_with(meta, mock_db_writer, mock_file_manager, logger)
    mock_batch.assert_called_once()
    passed_files = mock_batch.call_args[0][0]
    assert set(passed_files) == {data1, data2}


def test_metadata_exception_is_swallowed(
    tmp_path, mock_db_writer, mock_file_manager, logger
):
    """An exception from process_cml_file does not abort processing of remaining files."""
    meta = _write_csv(tmp_path, "metadata_links.csv")
    data = _write_csv(tmp_path, "raw_data_001.csv")

    with patch("parser.main.process_cml_file", side_effect=Exception("DB down")) as mock_single, \
         patch("parser.main.process_rawdata_files_batch") as mock_batch, \
         patch("parser.main.Config.INCOMING_DIR", tmp_path):
        process_existing_files(mock_db_writer, mock_file_manager, logger)  # must not raise

    mock_single.assert_called_once()
    # Batch processing of data files still happens
    mock_batch.assert_called_once()
