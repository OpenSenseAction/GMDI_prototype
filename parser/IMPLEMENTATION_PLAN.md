# Parser Service Implementation Plan (Option 4: Hybrid File Watcher)

**Date:** 2026-01-22  
**Status:** Planning  
**Target:** Implement event-driven parser service for CML data ingestion

---

## Overview

Implement a lightweight, event-driven parser service that:
- Watches for new files uploaded via SFTP
- Parses CSV files (raw data and metadata) and writes to PostgreSQL/TimescaleDB
- Moves successfully parsed files to archive directory
- Moves failed files to quarantine directory
- Supports extensibility for future file formats (NetCDF, HDF5)
- Can be disabled for testing environments

---

## Architecture

### Current Data Flow
```
MNO Simulator → SFTP Server → /uploads/
                                  ↓
                         Webserver (read-only access)
```

### New Data Flow
```
MNO Simulator → SFTP Server → /uploads/ (incoming)
                                  ↓ (watchdog file event)
                              Parser Service
                                  ├─ Parse & Validate
                                  ├─ Write to Database
                                  ├─ Success → /archived/YYYY-MM-DD/
                                  └─ Failure → /quarantine/
```

### Directory Structure
```
/app/data/incoming/          # SFTP uploads (shared volume: sftp_uploads)
/app/data/archived/          # Successfully parsed files (by date)
/app/data/quarantine/        # Failed parsing attempts
```

---

## File Structure

### New/Modified Files

```
parser/
├── main.py                      # MODIFY: Entry point with file watcher
├── requirements.txt             # MODIFY: Add dependencies
├── Dockerfile                   # MODIFY: Update if needed
├── parsers/                     # NEW directory
│   ├── __init__.py             # Parser exports
│   ├── base_parser.py          # Abstract base class
│   ├── csv_rawdata_parser.py   # CML time series CSV parser
│   ├── csv_metadata_parser.py  # CML metadata CSV parser
│   └── parser_registry.py      # File pattern → Parser mapping
├── file_watcher.py             # NEW: Watchdog-based file monitor
├── file_manager.py             # NEW: Archive/quarantine operations
├── db_writer.py                # NEW: Database operations
└── config.py                   # NEW: Configuration management

tests/
└── parser/                     # NEW directory
    ├── test_csv_parsers.py
    ├── test_file_manager.py
    ├── test_db_writer.py
    └── fixtures/
        ├── valid_cml_data.csv
        ├── valid_cml_metadata.csv
        ├── invalid_data.csv
        └── sample_with_nulls.csv
```

---

## Implementation Steps

### Phase 1: Database Operations (`db_writer.py`)

**Purpose:** Centralize all database write operations with validation.

**Key Functions:**
```python
class DBWriter:
    def __init__(self, db_url: str)
    def connect(self) -> None
    def close(self) -> None
    
    # Metadata operations
    def write_metadata(self, df: pd.DataFrame) -> int
    def metadata_exists(self, cml_id: str) -> bool
    def get_existing_metadata_ids(self) -> set[str]
    
    # Raw data operations  
    def write_rawdata(self, df: pd.DataFrame) -> int
    def validate_rawdata_references(self, df: pd.DataFrame) -> tuple[bool, list[str]]
    
    # Utilities
    def execute_query(self, query: str, params: tuple) -> Any
```

**Validation Rules:**
- Metadata: `cml_id` must be unique (handle ON CONFLICT)
- Raw data: `cml_id` must exist in `cml_metadata` table
- All coordinates must be valid floats
- Timestamps must be parseable
- Handle NULL values appropriately (RSL/TSL can be NULL)

**Error Handling:**
- Catch `psycopg2.IntegrityError` for duplicate metadata
- Catch `psycopg2.DataError` for invalid data types
- Return detailed error messages for logging

---

### Phase 2: File Management (`file_manager.py`)

**Purpose:** Handle file movement with atomic operations and date-based archiving.

**Key Functions:**
```python
class FileManager:
    def __init__(self, incoming_dir: str, archived_dir: str, quarantine_dir: str)
    
    def archive_file(self, filepath: Path) -> Path
        """Move file to archived/YYYY-MM-DD/ directory"""
        
    def quarantine_file(self, filepath: Path, error: str) -> Path
        """Move file to quarantine with error metadata"""
        
    def create_error_metadata(self, filepath: Path, error: str) -> None
        """Create .error.txt file with failure details"""
        
    def get_archived_path(self, filepath: Path) -> Path
        """Generate archive path with date subfolder"""
        
    def is_valid_file(self, filepath: Path) -> bool
        """Check if file should be processed (extension, size, etc.)"""
```

**Archive Structure:**
```
archived/
├── 2026-01-22/
│   ├── cml_data_20260122_093038.csv
│   └── cml_metadata_20260122_100000.csv
└── 2026-01-23/
    └── cml_data_20260123_080000.csv

quarantine/
├── bad_data_20260122_120000.csv
├── bad_data_20260122_120000.csv.error.txt  # Contains error details
└── corrupt_file.csv
```

**Atomic Operations:**
- Use `shutil.move()` for atomic file moves (same filesystem)
- Create directories with `exist_ok=True`
- Handle permission errors gracefully

---

### Phase 3: Parser Base Class (`parsers/base_parser.py`)

**Purpose:** Define interface for all parser implementations.

**Abstract Base Class:**
```python
from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple

class BaseParser(ABC):
    """Abstract base class for all file parsers."""
    
    @abstractmethod
    def can_parse(self, filepath: Path) -> bool:
        """Check if this parser can handle the file."""
        pass
    
    @abstractmethod
    def parse(self, filepath: Path) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Parse file and return DataFrame and error message.
        
        Returns:
            (DataFrame, None) on success
            (None, error_message) on failure
        """
        pass
    
    @abstractmethod
    def get_file_type(self) -> str:
        """Return file type identifier (e.g., 'rawdata', 'metadata')"""
        pass
    
    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate parsed DataFrame structure."""
        pass
```

**Common Validation:**
- Check required columns exist
- Validate data types
- Check for empty DataFrames
- Validate value ranges (e.g., lat/lon bounds)

---

### Phase 4: CSV Parsers

#### A. Raw Data Parser (`parsers/csv_rawdata_parser.py`)

**Expected CSV Format:**
```csv
time,cml_id,sublink_id,tsl,rsl
2026-01-20 09:30:38.196389,10001,sublink_1,1.0,-46.0
2026-01-20 09:30:38.196389,10002,sublink_1,0.0,-41.0
```

**Implementation:**
```python
class CSVRawDataParser(BaseParser):
    REQUIRED_COLUMNS = ['time', 'cml_id', 'sublink_id', 'tsl', 'rsl']
    FILE_PATTERN = r'^cml_data_.*\.csv$'
    
    def can_parse(self, filepath: Path) -> bool:
        return re.match(self.FILE_PATTERN, filepath.name) is not None
    
    def parse(self, filepath: Path) -> Tuple[pd.DataFrame, Optional[str]]:
        try:
            df = pd.read_csv(filepath)
            
            # Validate columns
            if not all(col in df.columns for col in self.REQUIRED_COLUMNS):
                return None, f"Missing required columns. Expected: {self.REQUIRED_COLUMNS}"
            
            # Parse timestamps
            df['time'] = pd.to_datetime(df['time'])
            
            # Convert cml_id to string
            df['cml_id'] = df['cml_id'].astype(str)
            
            # Handle nulls in tsl/rsl (they are allowed)
            df['tsl'] = pd.to_numeric(df['tsl'], errors='coerce')
            df['rsl'] = pd.to_numeric(df['rsl'], errors='coerce')
            
            # Validate
            is_valid, error = self.validate_dataframe(df)
            if not is_valid:
                return None, error
            
            return df, None
            
        except Exception as e:
            return None, f"Parse error: {str(e)}"
    
    def get_file_type(self) -> str:
        return 'rawdata'
    
    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        if df.empty:
            return False, "Empty DataFrame"
        
        if df['time'].isna().any():
            return False, "Invalid timestamps found"
        
        if df['cml_id'].isna().any():
            return False, "Missing cml_id values"
            
        return True, None
```

#### B. Metadata Parser (`parsers/csv_metadata_parser.py`)

**Expected CSV Format:**
```csv
cml_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat
10001,13.3888,52.5170,13.4050,52.5200
10002,13.3500,52.5100,13.3600,52.5150
```

**Implementation:**
```python
class CSVMetadataParser(BaseParser):
    REQUIRED_COLUMNS = ['cml_id', 'site_0_lon', 'site_0_lat', 'site_1_lon', 'site_1_lat']
    FILE_PATTERN = r'^cml_metadata_.*\.csv$'
    
    def can_parse(self, filepath: Path) -> bool:
        return re.match(self.FILE_PATTERN, filepath.name) is not None
    
    def parse(self, filepath: Path) -> Tuple[pd.DataFrame, Optional[str]]:
        try:
            df = pd.read_csv(filepath)
            
            # Validate columns
            if not all(col in df.columns for col in self.REQUIRED_COLUMNS):
                return None, f"Missing required columns. Expected: {self.REQUIRED_COLUMNS}"
            
            # Convert cml_id to string
            df['cml_id'] = df['cml_id'].astype(str)
            
            # Parse coordinates as floats
            for col in ['site_0_lon', 'site_0_lat', 'site_1_lon', 'site_1_lat']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate
            is_valid, error = self.validate_dataframe(df)
            if not is_valid:
                return None, error
            
            return df, None
            
        except Exception as e:
            return None, f"Parse error: {str(e)}"
    
    def get_file_type(self) -> str:
        return 'metadata'
    
    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        if df.empty:
            return False, "Empty DataFrame"
        
        if df['cml_id'].isna().any():
            return False, "Missing cml_id values"
        
        # Validate coordinate ranges
        if not df['site_0_lon'].between(-180, 180).all():
            return False, "Invalid longitude values in site_0_lon"
        if not df['site_0_lat'].between(-90, 90).all():
            return False, "Invalid latitude values in site_0_lat"
        if not df['site_1_lon'].between(-180, 180).all():
            return False, "Invalid longitude values in site_1_lon"
        if not df['site_1_lat'].between(-90, 90).all():
            return False, "Invalid latitude values in site_1_lat"
            
        return True, None
```

---

### Phase 5: Parser Registry (`parsers/parser_registry.py`)

**Purpose:** Map file patterns to appropriate parsers.

**Implementation:**
```python
from typing import List, Optional
from pathlib import Path
import logging

from .base_parser import BaseParser
from .csv_rawdata_parser import CSVRawDataParser
from .csv_metadata_parser import CSVMetadataParser

logger = logging.getLogger(__name__)

class ParserRegistry:
    """Registry for mapping files to appropriate parsers."""
    
    def __init__(self):
        self.parsers: List[BaseParser] = [
            CSVRawDataParser(),
            CSVMetadataParser(),
            # Future parsers can be added here:
            # NetCDFRawDataParser(),
            # NetCDFMetadataParser(),
        ]
    
    def get_parser(self, filepath: Path) -> Optional[BaseParser]:
        """
        Find appropriate parser for given file.
        
        Returns:
            Parser instance if found, None otherwise
        """
        for parser in self.parsers:
            if parser.can_parse(filepath):
                logger.debug(f"Matched {filepath.name} to {parser.__class__.__name__}")
                return parser
        
        logger.warning(f"No parser found for {filepath.name}")
        return None
    
    def get_supported_extensions(self) -> List[str]:
        """Return list of supported file extensions."""
        return ['.csv', '.nc', '.h5', '.hdf5']  # Can be dynamic in future
```

**Usage:**
```python
registry = ParserRegistry()
parser = registry.get_parser(Path("cml_data_20260122.csv"))
if parser:
    df, error = parser.parse(filepath)
```

---

### Phase 6: File Watcher (`file_watcher.py`)

**Purpose:** Monitor directory for new files using watchdog library.

**Implementation:**
```python
import time
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent

logger = logging.getLogger(__name__)

class FileUploadHandler(FileSystemEventHandler):
    """Handle file creation events."""
    
    def __init__(self, callback, supported_extensions):
        super().__init__()
        self.callback = callback
        self.supported_extensions = supported_extensions
        self.processing = set()  # Track files being processed
    
    def on_created(self, event: FileCreatedEvent):
        """Called when a file is created."""
        if event.is_directory:
            return
        
        filepath = Path(event.src_path)
        
        # Check if supported extension
        if filepath.suffix not in self.supported_extensions:
            logger.debug(f"Ignoring unsupported file: {filepath.name}")
            return
        
        # Avoid processing same file twice
        if str(filepath) in self.processing:
            logger.debug(f"Already processing: {filepath.name}")
            return
        
        # Wait for file to be fully written (SFTP might still be writing)
        self._wait_for_file_ready(filepath)
        
        # Mark as processing
        self.processing.add(str(filepath))
        
        try:
            logger.info(f"New file detected: {filepath.name}")
            self.callback(filepath)
        finally:
            self.processing.discard(str(filepath))
    
    def _wait_for_file_ready(self, filepath: Path, timeout: int = 10):
        """
        Wait for file to be fully written by checking size stability.
        
        Args:
            filepath: Path to file
            timeout: Maximum seconds to wait
        """
        if not filepath.exists():
            return
        
        start_time = time.time()
        last_size = -1
        
        while time.time() - start_time < timeout:
            try:
                current_size = filepath.stat().st_size
                
                if current_size == last_size and current_size > 0:
                    # Size hasn't changed, file is ready
                    logger.debug(f"File ready: {filepath.name} ({current_size} bytes)")
                    return
                
                last_size = current_size
                time.sleep(0.5)  # Check every 500ms
                
            except OSError:
                # File might be temporarily inaccessible
                time.sleep(0.5)
        
        logger.warning(f"Timeout waiting for file to stabilize: {filepath.name}")


class FileWatcher:
    """Watch directory for new files."""
    
    def __init__(self, watch_dir: str, callback, supported_extensions):
        self.watch_dir = Path(watch_dir)
        self.callback = callback
        self.supported_extensions = supported_extensions
        self.observer = None
    
    def start(self):
        """Start watching directory."""
        if not self.watch_dir.exists():
            raise ValueError(f"Watch directory does not exist: {self.watch_dir}")
        
        event_handler = FileUploadHandler(self.callback, self.supported_extensions)
        self.observer = Observer()
        self.observer.schedule(event_handler, str(self.watch_dir), recursive=False)
        self.observer.start()
        
        logger.info(f"Started watching: {self.watch_dir}")
    
    def stop(self):
        """Stop watching directory."""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            logger.info("Stopped file watcher")
```

---

### Phase 7: Configuration (`config.py`)

**Purpose:** Centralize configuration with environment variable support.

**Implementation:**
```python
import os
from pathlib import Path
from typing import Optional

class Config:
    """Parser service configuration."""
    
    # Database
    DATABASE_URL: str = os.getenv(
        'DATABASE_URL',
        'postgresql://myuser:mypassword@database:5432/mydatabase'
    )
    
    # Directories
    INCOMING_DIR: Path = Path(os.getenv('INCOMING_DIR', '/app/data/incoming'))
    ARCHIVED_DIR: Path = Path(os.getenv('ARCHIVED_DIR', '/app/data/archived'))
    QUARANTINE_DIR: Path = Path(os.getenv('QUARANTINE_DIR', '/app/data/quarantine'))
    
    # Parser behavior
    PARSER_ENABLED: bool = os.getenv('PARSER_ENABLED', 'true').lower() == 'true'
    PROCESS_EXISTING_ON_STARTUP: bool = os.getenv('PROCESS_EXISTING_ON_STARTUP', 'true').lower() == 'true'
    
    # File watching
    FILE_STABILITY_TIMEOUT: int = int(os.getenv('FILE_STABILITY_TIMEOUT', '10'))
    
    # Database operations
    DB_BATCH_SIZE: int = int(os.getenv('DB_BATCH_SIZE', '10000'))
    DB_TIMEOUT: int = int(os.getenv('DB_TIMEOUT', '30'))
    
    # Logging
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def create_directories(cls):
        """Create required directories if they don't exist."""
        for directory in [cls.INCOMING_DIR, cls.ARCHIVED_DIR, cls.QUARANTINE_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def validate(cls):
        """Validate configuration."""
        if not cls.DATABASE_URL:
            raise ValueError("DATABASE_URL must be set")
        
        # Ensure directories are accessible
        try:
            cls.create_directories()
        except Exception as e:
            raise ValueError(f"Cannot create directories: {e}")
```

---

### Phase 8: Main Entry Point (`main.py`)

**Purpose:** Orchestrate all components and handle startup/shutdown.

**Implementation:**
```python
import sys
import time
import logging
from pathlib import Path
from typing import Optional

from config import Config
from parsers.parser_registry import ParserRegistry
from file_watcher import FileWatcher
from file_manager import FileManager
from db_writer import DBWriter

# Configure logging
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class ParserService:
    """Main parser service orchestrator."""
    
    def __init__(self):
        self.config = Config
        self.parser_registry = ParserRegistry()
        self.file_manager = FileManager(
            incoming_dir=str(Config.INCOMING_DIR),
            archived_dir=str(Config.ARCHIVED_DIR),
            quarantine_dir=str(Config.QUARANTINE_DIR)
        )
        self.db_writer = DBWriter(Config.DATABASE_URL)
        self.file_watcher: Optional[FileWatcher] = None
    
    def process_file(self, filepath: Path):
        """
        Process a single file: parse, validate, write to DB, archive/quarantine.
        
        Args:
            filepath: Path to file to process
        """
        logger.info(f"Processing: {filepath.name}")
        
        try:
            # Find appropriate parser
            parser = self.parser_registry.get_parser(filepath)
            if not parser:
                error = f"No parser available for {filepath.name}"
                logger.error(error)
                self.file_manager.quarantine_file(filepath, error)
                return
            
            # Parse file
            df, parse_error = parser.parse(filepath)
            if parse_error:
                logger.error(f"Parse failed for {filepath.name}: {parse_error}")
                self.file_manager.quarantine_file(filepath, parse_error)
                return
            
            # Write to database based on file type
            file_type = parser.get_file_type()
            
            try:
                if file_type == 'metadata':
                    rows_written = self.db_writer.write_metadata(df)
                    logger.info(f"Wrote {rows_written} metadata records from {filepath.name}")
                
                elif file_type == 'rawdata':
                    # Validate that metadata exists for all cml_ids
                    is_valid, missing_ids = self.db_writer.validate_rawdata_references(df)
                    if not is_valid:
                        error = f"Missing metadata for CML IDs: {missing_ids}"
                        logger.error(error)
                        self.file_manager.quarantine_file(filepath, error)
                        return
                    
                    rows_written = self.db_writer.write_rawdata(df)
                    logger.info(f"Wrote {rows_written} data records from {filepath.name}")
                
                else:
                    error = f"Unknown file type: {file_type}"
                    logger.error(error)
                    self.file_manager.quarantine_file(filepath, error)
                    return
                
                # Success - archive file
                archived_path = self.file_manager.archive_file(filepath)
                logger.info(f"Archived: {filepath.name} → {archived_path}")
                
            except Exception as db_error:
                error = f"Database error: {str(db_error)}"
                logger.error(error, exc_info=True)
                self.file_manager.quarantine_file(filepath, error)
                return
        
        except Exception as e:
            error = f"Unexpected error: {str(e)}"
            logger.error(error, exc_info=True)
            try:
                self.file_manager.quarantine_file(filepath, error)
            except Exception as quarantine_error:
                logger.critical(f"Failed to quarantine file: {quarantine_error}")
    
    def process_existing_files(self):
        """Process any files that already exist in incoming directory."""
        logger.info("Checking for existing files...")
        
        incoming_files = list(Config.INCOMING_DIR.glob('*'))
        file_count = len([f for f in incoming_files if f.is_file()])
        
        if file_count == 0:
            logger.info("No existing files to process")
            return
        
        logger.info(f"Found {file_count} existing files")
        
        for filepath in incoming_files:
            if filepath.is_file():
                # Check if it's a supported file type
                if filepath.suffix in self.parser_registry.get_supported_extensions():
                    self.process_file(filepath)
                else:
                    logger.debug(f"Skipping unsupported file: {filepath.name}")
    
    def start(self):
        """Start the parser service."""
        logger.info("=" * 60)
        logger.info("Starting Parser Service")
        logger.info("=" * 60)
        
        # Validate configuration
        try:
            Config.validate()
            logger.info(f"Incoming directory: {Config.INCOMING_DIR}")
            logger.info(f"Archive directory: {Config.ARCHIVED_DIR}")
            logger.info(f"Quarantine directory: {Config.QUARANTINE_DIR}")
        except Exception as e:
            logger.critical(f"Configuration validation failed: {e}")
            sys.exit(1)
        
        # Check if parser is enabled
        if not Config.PARSER_ENABLED:
            logger.warning("Parser is DISABLED (PARSER_ENABLED=false)")
            logger.info("Service will run but not process files")
            # Keep container running but do nothing
            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                logger.info("Shutting down (parser was disabled)")
            return
        
        # Connect to database
        try:
            self.db_writer.connect()
            logger.info("Connected to database")
        except Exception as e:
            logger.critical(f"Database connection failed: {e}")
            sys.exit(1)
        
        # Process existing files on startup (if enabled)
        if Config.PROCESS_EXISTING_ON_STARTUP:
            try:
                self.process_existing_files()
            except Exception as e:
                logger.error(f"Error processing existing files: {e}")
        
        # Start file watcher
        try:
            supported_extensions = self.parser_registry.get_supported_extensions()
            self.file_watcher = FileWatcher(
                watch_dir=str(Config.INCOMING_DIR),
                callback=self.process_file,
                supported_extensions=supported_extensions
            )
            self.file_watcher.start()
            
            logger.info("Parser service started successfully")
            logger.info("Watching for new files...")
            
            # Keep running
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.critical(f"Fatal error: {e}", exc_info=True)
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Clean shutdown of all components."""
        logger.info("Shutting down parser service...")
        
        if self.file_watcher:
            self.file_watcher.stop()
        
        if self.db_writer:
            self.db_writer.close()
        
        logger.info("Parser service stopped")


def main():
    """Entry point."""
    service = ParserService()
    service.start()


if __name__ == '__main__':
    main()
```

---

### Phase 9: Update Dependencies (`requirements.txt`)

**Add Required Packages:**
```txt
# Existing dependencies (keep these)
requests
psycopg2-binary
xarray
netCDF4
pandas
numpy

# New dependencies for parser service
watchdog>=3.0.0       # File system monitoring
python-dateutil>=2.8.0  # Date parsing utilities
```

---

### Phase 10: Update Docker Configuration

#### A. Update `docker-compose.yml`

**Add Volume Mounts for Parser:**
```yaml
parser:
  build: ./parser
  depends_on:
    - database
    - sftp_receiver
  environment:
    - DATABASE_URL=postgresql://myuser:mypassword@database:5432/mydatabase
    - PARSER_ENABLED=true
    - PROCESS_EXISTING_ON_STARTUP=true
    - LOG_LEVEL=INFO
  volumes:
    - sftp_uploads:/app/data/incoming:ro  # Read-only access to SFTP uploads
    - parser_archived:/app/data/archived
    - parser_quarantine:/app/data/quarantine

volumes:
  sftp_uploads:
  parser_archived:    # NEW
  parser_quarantine:  # NEW
  # ... other volumes
```

#### B. Update `parser/Dockerfile` (if needed)

**Current Dockerfile should work, but verify:**
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create data directories
RUN mkdir -p /app/data/incoming /app/data/archived /app/data/quarantine

CMD ["python", "main.py"]
```

---

### Phase 11: Testing Strategy

#### A. Unit Tests (`tests/parser/test_csv_parsers.py`)

**Test Cases:**
```python
import pytest
import pandas as pd
from pathlib import Path
from parser.parsers.csv_rawdata_parser import CSVRawDataParser
from parser.parsers.csv_metadata_parser import CSVMetadataParser

class TestCSVRawDataParser:
    def test_can_parse_valid_filename(self):
        parser = CSVRawDataParser()
        assert parser.can_parse(Path("cml_data_20260122.csv"))
        assert not parser.can_parse(Path("cml_metadata_20260122.csv"))
    
    def test_parse_valid_file(self, tmp_path):
        # Create test CSV
        csv_content = """time,cml_id,sublink_id,tsl,rsl
2026-01-22 10:00:00,10001,sublink_1,1.0,-46.0
2026-01-22 10:01:00,10002,sublink_1,0.0,-41.0"""
        
        test_file = tmp_path / "cml_data_test.csv"
        test_file.write_text(csv_content)
        
        parser = CSVRawDataParser()
        df, error = parser.parse(test_file)
        
        assert error is None
        assert df is not None
        assert len(df) == 2
        assert df['cml_id'].iloc[0] == '10001'
    
    def test_parse_with_nulls(self, tmp_path):
        csv_content = """time,cml_id,sublink_id,tsl,rsl
2026-01-22 10:00:00,10001,sublink_1,,
2026-01-22 10:01:00,10002,sublink_1,1.0,-41.0"""
        
        test_file = tmp_path / "cml_data_nulls.csv"
        test_file.write_text(csv_content)
        
        parser = CSVRawDataParser()
        df, error = parser.parse(test_file)
        
        assert error is None
        assert pd.isna(df['tsl'].iloc[0])
        assert pd.isna(df['rsl'].iloc[0])
    
    def test_parse_missing_columns(self, tmp_path):
        csv_content = """time,cml_id
2026-01-22 10:00:00,10001"""
        
        test_file = tmp_path / "cml_data_bad.csv"
        test_file.write_text(csv_content)
        
        parser = CSVRawDataParser()
        df, error = parser.parse(test_file)
        
        assert df is None
        assert "Missing required columns" in error

class TestCSVMetadataParser:
    def test_can_parse_valid_filename(self):
        parser = CSVMetadataParser()
        assert parser.can_parse(Path("cml_metadata_20260122.csv"))
        assert not parser.can_parse(Path("cml_data_20260122.csv"))
    
    def test_parse_valid_file(self, tmp_path):
        csv_content = """cml_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat
10001,13.3888,52.5170,13.4050,52.5200
10002,13.3500,52.5100,13.3600,52.5150"""
        
        test_file = tmp_path / "cml_metadata_test.csv"
        test_file.write_text(csv_content)
        
        parser = CSVMetadataParser()
        df, error = parser.parse(test_file)
        
        assert error is None
        assert df is not None
        assert len(df) == 2
    
    def test_parse_invalid_coordinates(self, tmp_path):
        csv_content = """cml_id,site_0_lon,site_0_lat,site_1_lon,site_1_lat
10001,200.0,52.5170,13.4050,52.5200"""  # Invalid longitude
        
        test_file = tmp_path / "cml_metadata_bad.csv"
        test_file.write_text(csv_content)
        
        parser = CSVMetadataParser()
        df, error = parser.parse(test_file)
        
        assert df is None
        assert "longitude" in error.lower()
```

#### B. Integration Tests

**Test with Docker Compose:**
```yaml
# docker-compose.test.yml
services:
  database:
    # ... same as main compose
  
  parser:
    build: ./parser
    depends_on:
      - database
    environment:
      - PARSER_ENABLED=true
      - DATABASE_URL=postgresql://myuser:mypassword@database:5432/mydatabase
    volumes:
      - ./tests/parser/fixtures:/app/data/incoming
      - test_archived:/app/data/archived
      - test_quarantine:/app/data/quarantine

volumes:
  test_archived:
  test_quarantine:
```

**Run Tests:**
```bash
# Start test environment
docker compose -f docker-compose.test.yml up -d

# Check that files were processed
docker compose -f docker-compose.test.yml exec parser ls -la /app/data/archived
docker compose -f docker-compose.test.yml exec parser ls -la /app/data/quarantine

# Query database
docker compose -f docker-compose.test.yml exec database psql -U myuser -d mydatabase -c "SELECT COUNT(*) FROM cml_data;"

# Cleanup
docker compose -f docker-compose.test.yml down -v
```

---

## Database Schema Considerations

### Current Schema
```sql
CREATE TABLE cml_data (
    time TIMESTAMPTZ NOT NULL,
    cml_id TEXT NOT NULL,
    sublink_id TEXT NOT NULL,
    rsl REAL,
    tsl REAL
);

CREATE TABLE cml_metadata (
    cml_id TEXT PRIMARY KEY,
    site_0_lon REAL,
    site_0_lat REAL,
    site_1_lon REAL,
    site_1_lat REAL
);
```

### Recommended Additions

**Add foreign key constraint** (optional but recommended):
```sql
-- Add to database/init.sql
ALTER TABLE cml_data 
ADD CONSTRAINT fk_cml_metadata 
FOREIGN KEY (cml_id) REFERENCES cml_metadata(cml_id);
```

**Add processing metadata table** (optional):
```sql
CREATE TABLE file_processing_log (
    id SERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    file_type TEXT,  -- 'rawdata' or 'metadata'
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    status TEXT,  -- 'success' or 'failed'
    rows_processed INTEGER,
    error_message TEXT,
    archived_path TEXT
);
```

This allows tracking of all processed files for auditing.

---

## Migration from Current State

### Current State
- SFTP uploads go to shared volume `sftp_uploads`
- Webserver has read-only access to uploads
- Parser container exists but is not implemented

### Migration Steps

1. **Implement parser code** (Phases 1-8)
2. **Add volume mounts** to docker-compose.yml
3. **Deploy** with `docker compose up -d --build parser`
4. **Monitor logs**: `docker compose logs -f parser`
5. **Verify processing**: 
   - Check archived files: `docker compose exec parser ls /app/data/archived`
   - Check database: `docker compose exec database psql ...`

### Rollback Plan
If parser has issues:
```bash
# Disable parser without rebuilding
docker compose up -d parser -e PARSER_ENABLED=false

# Or stop parser entirely
docker compose stop parser
```

Files remain in incoming directory and can be reprocessed after fix.

---

## Error Handling Scenarios

### Scenario 1: Database Connection Lost
- **Behavior**: Parser logs error and moves file to quarantine
- **Recovery**: Fix DB, move files from quarantine back to incoming

### Scenario 2: Malformed CSV
- **Behavior**: Parse error logged, file moved to quarantine with .error.txt
- **Recovery**: Fix CSV format, move back to incoming

### Scenario 3: Missing Metadata Reference
- **Behavior**: Raw data file quarantined (metadata doesn't exist for CML ID)
- **Recovery**: Upload metadata file first, then move raw data back to incoming

### Scenario 4: Duplicate Metadata
- **Behavior**: Use `ON CONFLICT` to update existing metadata or skip
- **Recovery**: None needed (idempotent)

### Scenario 5: Watchdog Crashes
- **Behavior**: Parser service restarts, processes existing files on startup
- **Recovery**: Automatic via Docker restart policy

---

## Performance Considerations

### Batch Size
- Process DataFrames in batches of 10,000 rows (configurable via `DB_BATCH_SIZE`)
- Commit transaction after each batch

### File Size Limits
- Reasonable limit: 500 MB per file (same as webserver upload limit)
- Large files handled via chunked reading with pandas `chunksize` parameter

### Concurrent Processing
- Current implementation processes files sequentially (simple, safe)
- Future enhancement: Thread pool for parallel file processing

### Database Connection Pooling
- For now: Single connection per parser instance
- Future: Use connection pool (e.g., psycopg2.pool) for better performance

---

## Monitoring and Observability

### Logging
- **INFO**: File processing events (received, parsed, archived)
- **WARNING**: Unsupported files, slow file writes
- **ERROR**: Parse failures, DB errors
- **CRITICAL**: Service startup failures

### Metrics to Track
- Files processed per hour
- Parse success/failure rate
- Average parse time per file
- Database write time
- Quarantine rate

### Health Check Endpoint (Future Enhancement)
```python
# Add to main.py
from flask import Flask, jsonify

health_app = Flask(__name__)

@health_app.route('/health')
def health():
    return jsonify({
        'status': 'healthy',
        'parser_enabled': Config.PARSER_ENABLED,
        'database_connected': db_writer.is_connected(),
        'watching': file_watcher.is_running()
    })

# Run on separate thread
```

---

## Future Enhancements

### 1. NetCDF Parser
```python
class NetCDFRawDataParser(BaseParser):
    FILE_PATTERN = r'^.*\.nc$'
    
    def parse(self, filepath: Path):
        ds = xr.open_dataset(filepath)
        df = get_dataframe_from_cml_dataset(ds)
        return df, None
```

### 2. Metadata Extraction from Raw Data Files
If metadata is embedded in raw data files (e.g., NetCDF), extract and update metadata table automatically.

### 3. Data Quality Checks
- Validate realistic value ranges (e.g., RSL should be negative)
- Flag outliers for review
- Add data quality scores to database

### 4. Notification System
- Email alerts on repeated parse failures
- Slack/webhook notifications for quarantined files

### 5. Web Dashboard Integration
- Add parser status to webserver landing page
- Show recent uploads and processing status
- Display quarantined files with errors

---

## Testing Checklist

Before considering implementation complete:

- [ ] Unit tests pass for all parsers
- [ ] File manager correctly archives files with date folders
- [ ] File manager creates error metadata in quarantine
- [ ] Database writer handles duplicate metadata gracefully
- [ ] Database writer validates foreign key references
- [ ] File watcher detects new files within 1 second
- [ ] Existing files processed on startup
- [ ] Parser can be disabled via environment variable
- [ ] Logs are informative and at correct levels
- [ ] Docker volumes persist data correctly
- [ ] Integration test runs end-to-end successfully
- [ ] Quarantined files can be reprocessed after moving back
- [ ] Service recovers from database connection loss
- [ ] Service handles malformed CSV files gracefully

---

## Summary

This implementation plan provides a **complete, production-ready parser service** that:

✅ Uses event-driven file watching (no polling delay)  
✅ Supports extensible parser architecture (easy to add formats)  
✅ Separates metadata and raw data parsing with validation  
✅ Archives successfully parsed files by date  
✅ Quarantines failed files with error details  
✅ Can be disabled for testing environments  
✅ Provides comprehensive error handling  
✅ Includes detailed logging for debugging  
✅ Is testable at unit and integration levels  

**Estimated Implementation Time:** 2-3 days for experienced developer

**Priority Order:**
1. Database operations (foundational)
2. File management (critical for safety)
3. Parsers (core functionality)
4. File watcher (automation)
5. Main orchestration (tie it together)
6. Testing (validation)
