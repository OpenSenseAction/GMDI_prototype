"""
Core logic for processing CML data files, extracted from ParserService.
This module is designed for unit testing and reuse.
"""

from dataclasses import dataclass
from pathlib import Path
import logging
from typing import Callable, Dict, List, Optional
import pandas as pd
from .parsers.demo_csv_data.parse_raw import parse_rawdata_csv
from .parsers.demo_csv_data.parse_metadata import parse_metadata_csv


@dataclass
class ParserBundle:
    """Encapsulates the three parser behaviours that vary across deployments."""

    parse_rawdata: Callable[[Path], Optional[pd.DataFrame]]
    parse_metadata: Callable[[Path], Optional[pd.DataFrame]]
    is_metadata_file: Callable[[str], bool]
    is_rawdata_file: Callable[[str], bool]


def _make_default_bundle() -> ParserBundle:
    """Return a ParserBundle backed by the demo_csv_data parsers.

    Module-level names are resolved at call time so that unit tests can still
    patch ``parser.service_logic.parse_rawdata_csv`` /
    ``parser.service_logic.parse_metadata_csv`` and have the patch take effect.
    """
    return ParserBundle(
        parse_rawdata=parse_rawdata_csv,
        parse_metadata=parse_metadata_csv,
        is_metadata_file=lambda name: "meta" in name,
        is_rawdata_file=lambda name: "raw" in name or "data" in name,
    )


def load_parser(parser_type: str, csv_config: Optional[dict] = None) -> ParserBundle:
    """Return a :class:`ParserBundle` for the requested *parser_type*.

    Args:
        parser_type: ``"demo_csv_data"`` (the built-in parser) or
            ``"csv_generic"`` (the config-driven parser).
        csv_config: Config dict used only when *parser_type* is
            ``"csv_generic"``.  Keys:

            * ``read_csv_kwargs`` — forwarded to ``pd.read_csv``
            * ``rawdata_columns`` — ``{src_col: canonical_col}`` rename map
            * ``metadata_columns`` — same for metadata files
            * ``timezone`` — IANA tz; naive timestamps are localised then
              converted to UTC
            * ``metadata_filename_keyword`` — substring that identifies
              metadata files (default: ``"meta"``)
            * ``rawdata_filename_keyword`` — substring that identifies rawdata
              files (default: anything that is not a metadata file)
    """
    if parser_type in ("demo_csv_data", "openmrg", "orange_cameroun"):
        # Legacy aliases kept for backward compatibility
        return _make_default_bundle()

    if parser_type == "csv_generic":
        config = csv_config or {}
        from .parsers.csv_generic.parse_raw import (
            parse_rawdata_csv as _parse_raw,
        )
        from .parsers.csv_generic.parse_metadata import (
            parse_metadata_csv as _parse_meta,
        )

        meta_kw = config.get("metadata_filename_keyword", "meta")
        raw_kw = config.get("rawdata_filename_keyword")

        def _is_metadata(name: str) -> bool:
            return meta_kw.lower() in name.lower()

        def _is_rawdata(name: str) -> bool:
            if raw_kw:
                return raw_kw.lower() in name.lower()
            return not _is_metadata(name)

        return ParserBundle(
            parse_rawdata=lambda fp: _parse_raw(fp, config),
            parse_metadata=lambda fp: _parse_meta(fp, config),
            is_metadata_file=_is_metadata,
            is_rawdata_file=_is_rawdata,
        )

    raise ValueError(f"Unknown parser_type: {parser_type!r}")


def process_rawdata_files_batch(
    filepaths: List[Path],
    db_writer,
    file_manager,
    logger=None,
    batch_size: int = 500,
    parser: Optional[ParserBundle] = None,
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
    if parser is None:
        parser = _make_default_bundle()

    total = len(filepaths)
    logger.info("Batch-processing %d data files (batch_size=%d)", total, batch_size)

    for batch_start in range(0, total, batch_size):
        batch = filepaths[batch_start : batch_start + batch_size]
        batch_num = batch_start // batch_size + 1

        dfs: List[pd.DataFrame] = []
        parsed_files: List[Path] = []
        failed_files: List[Path] = []
        file_row_counts: Dict[Path, int] = {}

        for filepath in batch:
            try:
                df = parser.parse_rawdata(filepath)
                if df is not None and not df.empty:
                    dfs.append(df)
                    file_row_counts[filepath] = len(df)
                    parsed_files.append(filepath)
                else:
                    failed_files.append(filepath)
            except Exception:
                logger.exception("Failed to parse %s, quarantining", filepath.name)
                failed_files.append(filepath)

        for filepath in failed_files:
            if filepath.exists():
                file_manager.quarantine_file(
                    filepath, "Parse error during batch processing"
                )
            db_writer.log_file_event(
                filepath.name,
                "quarantined",
                error_message="Parse error during batch processing",
            )

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
                db_writer.log_file_event(
                    filepath.name,
                    "archived",
                    rows_written=file_row_counts.get(filepath),
                )
        except Exception:
            logger.exception(
                "Batch %d write failed; %d files remain in incoming for retry",
                batch_num,
                len(parsed_files),
            )


def process_cml_file(
    filepath: Path,
    db_writer,
    file_manager,
    logger=None,
    parser: Optional[ParserBundle] = None,
):
    """
    Process a CML data file (raw or metadata), write to DB, archive or quarantine as needed.
    Args:
        filepath (Path): Path to the file to process.
        db_writer: DBWriter instance (must have connect, write_metadata, write_rawdata, validate_rawdata_references).
        file_manager: FileManager instance (must have archive_file, quarantine_file).
        logger: Optional logger for logging (default: None).
        parser: ParserBundle selecting which parse functions and filename
            classification rules to use.  Defaults to the demo_csv_data parser.
    Returns:
        str: 'metadata', 'rawdata', or 'unsupported' for file type processed.
    Raises:
        Exception: If any error occurs during processing (file is quarantined).
    """
    if logger is None:
        logger = logging.getLogger("parser.logic")
    if parser is None:
        parser = _make_default_bundle()
    logger.info(f"Processing file: {filepath}")
    name = filepath.name.lower()
    try:
        db_writer.connect()
    except Exception as e:
        logger.exception("Failed to connect to DB")
        error_msg = f"DB connection failed: {e}"
        file_manager.quarantine_file(filepath, error_msg)
        db_writer.log_file_event(filepath.name, "quarantined", error_message=error_msg)
        raise

    try:
        if parser.is_metadata_file(name):
            df = parser.parse_metadata(filepath)
            rows = db_writer.write_metadata(df)
            logger.info(f"Wrote {rows} metadata rows from {filepath.name}")
            file_manager.archive_file(filepath)
            db_writer.log_file_event(filepath.name, "archived", rows_written=rows)
            return "metadata"
        elif parser.is_rawdata_file(name):
            df = parser.parse_rawdata(filepath)
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
            db_writer.log_file_event(filepath.name, "archived", rows_written=rows)
            return "rawdata"
        else:
            error_msg = f"Unsupported file type: {filepath.name}"
            file_manager.quarantine_file(filepath, error_msg)
            db_writer.log_file_event(
                filepath.name, "quarantined", error_message=error_msg
            )
            return "unsupported"
    except Exception as e:
        logger.exception("Error handling file")
        file_manager.quarantine_file(filepath, str(e))
        db_writer.log_file_event(filepath.name, "quarantined", error_message=str(e))
        raise
