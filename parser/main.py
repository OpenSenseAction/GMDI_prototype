"""Parser service entrypoint and orchestration.

This module wires together the ParserRegistry, FileWatcher, DBWriter and
FileManager to implement the parser service. It is intentionally
lightweight and delegates parsing logic to parser implementations in
`parsers/`.
"""

import sys
import os
import time
import logging
from pathlib import Path
from typing import Optional

from parser.parsers.parser_registry import ParserRegistry
from parser.file_watcher import FileWatcher
from parser.file_manager import FileManager
from parser.db_writer import DBWriter


class Config:
    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://myuser:mypassword@database:5432/mydatabase"
    )
    # Fallbacks to simple defaults; can be overridden via env vars at container level
    INCOMING_DIR = Path("/app/data/incoming")
    ARCHIVED_DIR = Path("/app/data/archived")
    QUARANTINE_DIR = Path("/app/data/quarantine")
    PARSER_ENABLED = True
    PROCESS_EXISTING_ON_STARTUP = True
    LOG_LEVEL = "INFO"


def setup_logging():
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


class ParserService:
    def __init__(self):
        setup_logging()
        self.logger = logging.getLogger("parser.service")
        self.registry = ParserRegistry()
        self.file_manager = FileManager(
            str(Config.INCOMING_DIR),
            str(Config.ARCHIVED_DIR),
            str(Config.QUARANTINE_DIR),
        )
        self.db_writer = DBWriter(Config.DATABASE_URL)
        self.watcher: Optional[FileWatcher] = None

    def process_file(self, filepath: Path):
        self.logger.info(f"Processing file: {filepath}")
        parser = self.registry.get_parser(filepath)
        if not parser:
            err = f"No parser available for {filepath.name}"
            self.logger.error(err)
            self.file_manager.quarantine_file(filepath, err)
            return

        df, parse_error = parser.parse(filepath)
        if parse_error:
            self.logger.error(f"Parse error for {filepath.name}: {parse_error}")
            self.file_manager.quarantine_file(filepath, parse_error)
            return

        file_type = parser.get_file_type()
        try:
            self.db_writer.connect()
        except Exception as e:
            self.logger.exception("Failed to connect to DB")
            self.file_manager.quarantine_file(filepath, f"DB connection failed: {e}")
            return

        try:
            if file_type == "metadata":
                rows = self.db_writer.write_metadata(df)
                self.logger.info(f"Wrote {rows} metadata rows from {filepath.name}")
            elif file_type == "rawdata":
                ok, missing = self.db_writer.validate_rawdata_references(df)
                if not ok:
                    self.file_manager.quarantine_file(
                        filepath, f"Missing metadata for CML IDs: {missing}"
                    )
                    return
                rows = self.db_writer.write_rawdata(df)
                self.logger.info(f"Wrote {rows} data rows from {filepath.name}")
            else:
                self.file_manager.quarantine_file(
                    filepath, f"Unsupported file type: {file_type}"
                )
                return

            self.file_manager.archive_file(filepath)

        except Exception as e:
            self.logger.exception("Error handling file")
            try:
                self.file_manager.quarantine_file(filepath, str(e))
            except Exception:
                self.logger.exception("Failed to quarantine after error")

    def process_existing_files(self):
        incoming = list(Config.INCOMING_DIR.glob("*"))
        for f in incoming:
            if (
                f.is_file()
                and f.suffix.lower() in self.registry.get_supported_extensions()
            ):
                self.process_file(f)

    def start(self):
        self.logger.info("Starting parser service")
        Config.INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        Config.ARCHIVED_DIR.mkdir(parents=True, exist_ok=True)
        Config.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

        if not Config.PARSER_ENABLED:
            self.logger.warning("Parser is disabled via configuration. Exiting.")
            return

        try:
            self.db_writer.connect()
        except Exception:
            self.logger.exception("Unable to connect to DB at startup")

        if Config.PROCESS_EXISTING_ON_STARTUP:
            self.process_existing_files()

        self.watcher = FileWatcher(
            str(Config.INCOMING_DIR),
            self.process_file,
            self.registry.get_supported_extensions(),
        )
        self.watcher.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Shutting down parser service")
        finally:
            if self.watcher:
                self.watcher.stop()
            self.db_writer.close()


def main():
    svc = ParserService()
    svc.start()


if __name__ == "__main__":
    main()
