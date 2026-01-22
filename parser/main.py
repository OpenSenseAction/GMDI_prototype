"""Parser service entrypoint and orchestration.

This module wires together the FileWatcher, DBWriter and FileManager to implement the parser service. It is intentionally lightweight and delegates parsing logic to function-based parsers in `parsers/demo_csv_data/`.
"""

import os
import time
import logging
from pathlib import Path
from parser.file_watcher import FileWatcher
from parser.file_manager import FileManager
from parser.db_writer import DBWriter
from parser.service_logic import process_cml_file


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

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down parser service")
    finally:
        watcher.stop()
        db_writer.close()


if __name__ == "__main__":
    main()
