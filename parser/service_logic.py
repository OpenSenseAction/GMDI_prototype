"""
Core logic for processing CML data files, extracted from ParserService.
This module is designed for unit testing and reuse.
"""

from pathlib import Path
import logging
from .parsers.demo_csv_data.parse_raw import parse_rawdata_csv
from .parsers.demo_csv_data.parse_metadata import parse_metadata_csv


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
