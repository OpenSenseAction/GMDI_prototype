"""
Core logic for processing CML data files, extracted from ParserService.
This module is designed for unit testing and reuse.
"""

from pathlib import Path
import logging
from typing import List
import pandas as pd
from .parsers.demo_csv_data.parse_raw import parse_rawdata_csv
from .parsers.demo_csv_data.parse_metadata import parse_metadata_csv


def process_rawdata_files_batch(
    filepaths: List[Path],
    db_writer,
    file_manager,
    logger=None,
    batch_size: int = 500,
) -> None:
    """Process a list of rawdata CSV files in batches.

    Instead of one commit per file, accumulates DataFrames across `batch_size`
    files and issues a single write + commit per batch.  This is orders of
    magnitude faster for large backlogs.

    Files that fail to parse are quarantined individually.  If the batch
    write fails the files remain in the incoming directory so a subsequent
    restart can retry them.
    """
    if logger is None:
        logger = logging.getLogger("parser.logic")

    total = len(filepaths)
    logger.info("Batch-processing %d data files (batch_size=%d)", total, batch_size)

    for batch_start in range(0, total, batch_size):
        batch = filepaths[batch_start : batch_start + batch_size]
        batch_num = batch_start // batch_size + 1

        dfs: List[pd.DataFrame] = []
        parsed_files: List[Path] = []
        failed_files: List[Path] = []

        for filepath in batch:
            try:
                df = parse_rawdata_csv(filepath)
                if df is not None and not df.empty:
                    dfs.append(df)
                    parsed_files.append(filepath)
                else:
                    failed_files.append(filepath)
            except Exception:
                logger.exception("Failed to parse %s, quarantining", filepath.name)
                failed_files.append(filepath)

        for filepath in failed_files:
            if filepath.exists():
                file_manager.quarantine_file(filepath, "Parse error during batch processing")

        if not dfs:
            logger.info("Batch %d: no parseable files, skipping write", batch_num)
            continue

        combined = pd.concat(dfs, ignore_index=True)
        try:
            db_writer.connect()
            rows = db_writer.write_rawdata(combined)
            logger.info(
                "Batch %d/%d: wrote %d rows from %d files",
                batch_num,
                -(-total // batch_size),
                rows,
                len(parsed_files),
            )
            for filepath in parsed_files:
                file_manager.archive_file(filepath)
        except Exception:
            logger.exception(
                "Batch %d write failed; %d files remain in incoming for retry",
                batch_num,
                len(parsed_files),
            )


def process_cml_file(filepath: Path, db_writer, file_manager, logger=None):
    """
    Process a CML data file (raw or metadata), write to DB, archive or quarantine as needed.
    Args:
        filepath (Path): Path to the file to process.
        db_writer: DBWriter instance (must have connect, write_metadata, write_rawdata, validate_rawdata_references).
        file_manager: FileManager instance (must have archive_file, quarantine_file).
        logger: Optional logger for logging (default: None).
    Returns:
        str: 'metadata', 'rawdata', or 'unsupported' for file type processed.
    Raises:
        Exception: If any error occurs during processing (file is quarantined).
    """
    if logger is None:
        logger = logging.getLogger("parser.logic")
    logger.info(f"Processing file: {filepath}")
    name = filepath.name.lower()
    try:
        db_writer.connect()
    except Exception as e:
        logger.exception("Failed to connect to DB")
        file_manager.quarantine_file(filepath, f"DB connection failed: {e}")
        raise

    try:
        if "meta" in name:
            df = parse_metadata_csv(filepath)
            rows = db_writer.write_metadata(df)
            logger.info(f"Wrote {rows} metadata rows from {filepath.name}")
            file_manager.archive_file(filepath)
            return "metadata"
        elif "raw" in name or "data" in name:
            df = parse_rawdata_csv(filepath)
            try:
                ok, missing = db_writer.validate_rawdata_references(df)
            except Exception:
                ok, missing = True, []
            rows = db_writer.write_rawdata(df)
            if not ok and missing:
                sample = missing[:10]
                logger.warning(
                    "Missing metadata for %d (cml_id, sublink_id) pairs; sample: %s",
                    len(missing),
                    sample,
                )
            logger.info(f"Wrote {rows} data rows from {filepath.name}")
            file_manager.archive_file(filepath)
            return "rawdata"
        else:
            file_manager.quarantine_file(
                filepath, f"Unsupported file type: {filepath.name}"
            )
            return "unsupported"
    except Exception as e:
        logger.exception("Error handling file")
        file_manager.quarantine_file(filepath, str(e))
        raise
