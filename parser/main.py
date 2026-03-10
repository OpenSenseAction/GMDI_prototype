"""Parser service entrypoint and orchestration.

This module wires together the FileWatcher, DBWriter and FileManager to implement the parser service. It is intentionally lightweight and delegates parsing logic to function-based parsers in `parsers/demo_csv_data/`.
"""

import os
import time
import logging
import threading
from pathlib import Path

from .file_watcher import FileWatcher
from .file_manager import FileManager
from .db_writer import DBWriter
from .service_logic import process_cml_file


class Config:
    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://myuser:mypassword@database:5432/mydatabase"
    )
    INCOMING_DIR = Path(os.getenv("PARSER_INCOMING_DIR", "data/incoming"))
    ARCHIVED_DIR = Path(os.getenv("PARSER_ARCHIVED_DIR", "data/archived"))
    QUARANTINE_DIR = Path(os.getenv("PARSER_QUARANTINE_DIR", "data/quarantine"))
    PARSER_ENABLED = os.getenv("PARSER_ENABLED", "True").lower() in ("1", "true", "yes")
    PROCESS_EXISTING_ON_STARTUP = os.getenv(
        "PROCESS_EXISTING_ON_STARTUP", "True"
    ).lower() in ("1", "true", "yes")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    # How often (seconds) to recalculate aggregate CML stats in the background
    STATS_REFRESH_INTERVAL = int(os.getenv("STATS_REFRESH_INTERVAL", "60"))


def setup_logging():
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def process_existing_files(db_writer, file_manager, logger):
    incoming = list(Config.INCOMING_DIR.glob("*"))
    for f in incoming:
        if f.is_file() and f.suffix.lower() in {".csv"}:
            try:
                process_cml_file(f, db_writer, file_manager, logger)
            except Exception:
                pass


def main():
    setup_logging()
    logger = logging.getLogger("parser.service")
    file_manager = FileManager(
        str(Config.INCOMING_DIR),
        str(Config.ARCHIVED_DIR),
        str(Config.QUARANTINE_DIR),
    )
    db_writer = DBWriter(Config.DATABASE_URL)

    logger.info("Starting parser service")
    Config.INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    Config.ARCHIVED_DIR.mkdir(parents=True, exist_ok=True)
    Config.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    if not Config.PARSER_ENABLED:
        logger.warning("Parser is disabled via configuration. Exiting.")
        return

    try:
        db_writer.connect()
    except Exception:
        logger.exception("Unable to connect to DB at startup")

    if Config.PROCESS_EXISTING_ON_STARTUP:
        process_existing_files(db_writer, file_manager, logger)

    def on_new_file(filepath):
        try:
            process_cml_file(filepath, db_writer, file_manager, logger)
        except Exception:
            pass

    watcher = FileWatcher(
        str(Config.INCOMING_DIR),
        on_new_file,
        {".csv"},
    )
    watcher.start()

    # Background thread: refresh cml_stats on a slow timer so it never
    # blocks file processing.
    stop_event = threading.Event()

    def stats_loop():
        # Use a separate DBWriter connection so stats queries don't contend
        # with the insert connection.
        stats_db = DBWriter(Config.DATABASE_URL)
        try:
            stats_db.connect()
        except Exception:
            logger.exception("Stats thread: could not connect to DB")
            return
        while not stop_event.wait(Config.STATS_REFRESH_INTERVAL):
            try:
                stats_db.refresh_stats()
            except Exception:
                logger.exception("Stats thread: refresh_stats failed")
        stats_db.close()

    stats_thread = threading.Thread(target=stats_loop, daemon=True, name="stats-refresh")
    stats_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down parser service")
    finally:
        stop_event.set()
        watcher.stop()
        db_writer.close()


if __name__ == "__main__":
    main()
