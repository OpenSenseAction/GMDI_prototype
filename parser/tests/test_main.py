"""Tests for process_existing_files and main() wiring in parser/main.py."""

import threading
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call


from ..main import process_existing_files, main


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


def _run_stats_loop(tmp_path, mock_event, *, configure_db=None):
    """Run main(), then invoke the captured stats_loop closure under the same patches.

    Calling stats_loop() inside the with-block is critical: if called after
    the block exits, DBWriter and Config are unpatched, connect() fails against
    a real DB, and the retry loop hangs forever.

    Returns the MagicMock instance used as every DBWriter in the test.
    """
    captured = {}

    class CapturingThread(threading.Thread):
        def __init__(self, *args, target=None, name=None, **kwargs):
            super().__init__(*args, target=target, name=name, **kwargs)
            if name == "stats-refresh":
                captured["stats_loop"] = target

        def start(self):
            pass  # don't spawn real threads in unit tests

    mock_db = MagicMock()

    with patch("parser.main.threading.Thread", CapturingThread), \
         patch("parser.main.threading.Event", return_value=mock_event), \
         patch("parser.main.FileManager"), \
         patch("parser.main.FileWatcher"), \
         patch("parser.main.DBWriter", return_value=mock_db), \
         patch("parser.main.Config.PARSER_ENABLED", True), \
         patch("parser.main.Config.PROCESS_EXISTING_ON_STARTUP", False), \
         patch("parser.main.Config.DATABASE_URL", "postgresql://test"), \
         patch("parser.main.Config.USER_ID", "test_user"), \
         patch("parser.main.Config.STATS_REFRESH_INTERVAL", 60), \
         patch("parser.main.Config.INCOMING_DIR", tmp_path), \
         patch("parser.main.Config.ARCHIVED_DIR", tmp_path), \
         patch("parser.main.Config.QUARANTINE_DIR", tmp_path), \
         patch("parser.main.time.sleep", side_effect=KeyboardInterrupt):
        try:
            main()
        except (KeyboardInterrupt, SystemExit):
            pass
        if configure_db is not None:
            configure_db(mock_db)
        if "stats_loop" in captured:
            captured["stats_loop"]()

    return mock_db


def test_stats_loop_calls_refresh_windowed_stats_on_startup(tmp_path):
    """stats_loop calls refresh_windowed_stats once before entering the timer loop."""
    mock_event = MagicMock()
    mock_event.is_set.return_value = False  # connect loop: enter → connect succeeds → break
    mock_event.wait.return_value = True     # timer wait → loop exits immediately

    mock_db = _run_stats_loop(tmp_path, mock_event)

    mock_db.refresh_windowed_stats.assert_called_once()
    mock_db.close.assert_called()  # called by stats_loop (possibly also by main cleanup)


def test_stats_loop_initial_refresh_error_is_swallowed(tmp_path):
    """stats_loop swallows exceptions raised by the startup refresh_windowed_stats call."""
    mock_event = MagicMock()
    mock_event.is_set.return_value = False
    mock_event.wait.return_value = True

    def configure(db):
        db.refresh_windowed_stats.side_effect = Exception("stats error")

    _run_stats_loop(tmp_path, mock_event, configure_db=configure)  # must not raise


def test_stats_loop_calls_refresh_windowed_stats_in_timer_loop(tmp_path):
    """stats_loop calls refresh_windowed_stats again on each timer tick."""
    mock_event = MagicMock()
    mock_event.is_set.return_value = False
    # First timer wait → False (enter loop body), second → True (exit)
    mock_event.wait.side_effect = [False, True]

    mock_db = _run_stats_loop(tmp_path, mock_event)

    # startup call + 1 timer-loop call = 2 total
    assert mock_db.refresh_windowed_stats.call_count == 2


def test_stats_loop_creates_dbwriter_with_config_user_id(tmp_path):
    """stats_loop must pass user_id=Config.USER_ID to DBWriter.

    Regression test for the bug where DBWriter was instantiated without
    user_id in the stats background thread, causing it to silently default
    to 'demo_openmrg' and never compute stats for any other user.
    """
    captured = {}

    class CapturingThread(threading.Thread):
        """Intercepts Thread creation to capture the stats-refresh target."""
        def __init__(self, *args, target=None, name=None, **kwargs):
            super().__init__(*args, target=target, name=name, **kwargs)
            if name == "stats-refresh":
                captured["stats_loop"] = target

        def start(self):
            pass  # don't spawn real threads in unit tests

    with patch("parser.main.threading.Thread", CapturingThread), \
         patch("parser.main.FileManager"), \
         patch("parser.main.FileWatcher"), \
         patch("parser.main.DBWriter") as MockDBWriter, \
         patch("parser.main.Config.PARSER_ENABLED", True), \
         patch("parser.main.Config.PROCESS_EXISTING_ON_STARTUP", False), \
         patch("parser.main.Config.DATABASE_URL", "postgresql://test"), \
         patch("parser.main.Config.USER_ID", "ctu_cz_tmobile"), \
         patch("parser.main.Config.INCOMING_DIR", tmp_path), \
         patch("parser.main.Config.ARCHIVED_DIR", tmp_path), \
         patch("parser.main.Config.QUARANTINE_DIR", tmp_path), \
         patch("parser.main.time.sleep", side_effect=KeyboardInterrupt):
        try:
            main()
        except (KeyboardInterrupt, SystemExit):
            pass

    assert "stats_loop" in captured, "stats-refresh Thread was never created"

    # After main() exits its finally block sets stop_event, so calling
    # stats_loop() directly will create DBWriter then return immediately
    # (stop_event already set → while-loop body skipped).
    captured["stats_loop"]()

    for c in MockDBWriter.call_args_list:
        user_id = c.kwargs.get("user_id") or (c.args[1] if len(c.args) > 1 else None)
        assert user_id == "ctu_cz_tmobile", (
            f"DBWriter called without user_id=Config.USER_ID: {c}"
        )
