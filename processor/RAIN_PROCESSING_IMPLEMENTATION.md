# Rain Rate Processing Implementation Plan

## Overview

This document describes the implementation of continuous rain rate processing from CML attenuation data. The processor runs as a polling service that:

1. Reads configuration from YAML file (which users can be enabled, processing frequency, data window)
2. Maintains state (last processed timestamp) in a JSON file
3. Polls the database at configurable intervals
4. Fetches raw CML data (RSL, TSL) and metadata for a sliding time window
5. Applies pluggable processing workflows (different algorithms for different MNO providers)
6. Writes results (rain rates and intermediate products) to a new database table

**Key Design Decisions:**
- Configuration via YAML (not database) for simplicity and version control
- State tracking via JSON file for persistence across restarts
- Starts processing from "now" when first enabled (no automatic backfill)
- Pluggable workflow architecture to support MNO-specific variants
- Fixed interface between data fetching and processing algorithms
- Processing workflows use `xarray.Dataset` / `xarray.DataArray` as the main in-memory format, because this matches typical `pycomlink` and `poligrain` example workflows better than `pandas`

**Feasibility:**
- Yes, this is feasible.
- The database read/write layer can still use SQL and optionally `pandas` internally for convenience, but the workflow interface should expose `xarray` objects.
- This is a good fit because CML processing is naturally multi-dimensional (`time`, `cml_id`, `sublink_id`) and many scientific processing examples already assume `xarray`.
- The main extra work is careful conversion between SQL table rows and a well-defined `xarray.Dataset` structure.

---

## Short Step-by-Step Implementation Guide

This section is intended as a quick guide for an implementing agent. Each step points to the section below where the details are defined.

1. **Create the database target table for processed rain data**  
    See: **Database Schema**

2. **Create the YAML configuration file for enabling/disabling processing and selecting workflow variants**  
    See: **Configuration Structure**

3. **Create the JSON state file handling so the processor remembers the last processed timestamp per user**  
    See: **State File Structure** and **`state_manager.py`**

4. **Implement the database read/write layer for raw CML data, metadata, and processed rain output**  
    See: **`data_interface.py`**

5. **Implement one canonical conversion from database rows to `xarray.Dataset` and back**  
    See: **`dataset_builder.py`**

6. **Implement the workflow interface and register workflow variants**  
    See: **`workflows/base.py`**, **`workflows/default.py`**, **`workflows/openmrg_basic.py`**, and **`registry.py`**

7. **Implement the continuous polling service**  
    See: **`main.py`**

8. **Add notebook-based validation that imports the exact same workflow code as the continuous processor**  
    See: **Notebook-Based Validation Workflow**

9. **Prepare the website integration target so processed rain data can later be shown in the same layout as the real-time page**  
    See: **Planned Website Integration**

10. **Install dependencies, update Docker configuration, and follow the implementation/testing checklist**  
     See: **Docker Configuration**, **Requirements**, **Implementation Checklist**, and **Testing Strategy**

**Recommended implementation order:**
- first make the processor run end-to-end with a minimal workflow
- then validate it in the notebook
- then improve the scientific workflow
- only after that enable continuous processing for selected users

---

## Database Schema

### New Table: `cml_rain_data`

This table stores the processed rain rate estimates and intermediate products.

```sql
CREATE TABLE cml_rain_data (
    time TIMESTAMPTZ NOT NULL,
    cml_id TEXT NOT NULL,
    sublink_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    tl REAL,              -- Total loss (TSL - RSL)
    wet BOOLEAN,          -- Wet/dry classification
    baseline REAL,        -- Baseline attenuation
    waa REAL,             -- Wet antenna attenuation
    a_rain REAL,          -- Rain-induced path attenuation
    r REAL,               -- Rain rate estimate (mm/h)
    PRIMARY KEY (time, cml_id, sublink_id, user_id)
);

-- Convert to hypertable (TimescaleDB)
SELECT create_hypertable('cml_rain_data', 'time');

-- Enable compression (same strategy as cml_data)
ALTER TABLE cml_rain_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id, cml_id'
);

-- Add compression policy (compress chunks older than 7 days)
SELECT add_compression_policy('cml_rain_data', INTERVAL '7 days');

-- Add Row-Level Security (RLS) for multi-user isolation
ALTER TABLE cml_rain_data ENABLE ROW LEVEL SECURITY;

CREATE POLICY cml_rain_data_user_policy ON cml_rain_data
    USING (user_id = current_user);

-- Grant permissions to user roles
-- Note: Assumes user roles already exist (e.g., demo_openmrg, demo_orange_cameroun)
-- and webserver_role exists for admin access
GRANT SELECT, INSERT ON cml_rain_data TO webserver_role;
-- Individual user grants will be added per user (same pattern as cml_data)

-- Create security-barrier view for safe access
CREATE VIEW cml_rain_data_secure WITH (security_barrier) AS
    SELECT * FROM cml_rain_data
    WHERE user_id = current_user;

GRANT SELECT ON cml_rain_data_secure TO webserver_role;
```

**Migration File:** Create `database/migrations/010_add_rain_data_table.sql` with the above SQL.

**Important Notes:**
- The table follows the same multi-user pattern as `cml_data` (user_id column + RLS)
- Compression is enabled to save space (rain data can be voluminous)
- `segmentby` includes `user_id, cml_id` for efficient compression and queries
- RLS ensures users only see their own data
- The security-barrier view provides safe access without bypassing RLS
- Even if workflows use `xarray`, the persisted format remains a normal relational table. The conversion back from `xarray` to rows must be explicit and validated.

---

## Configuration Structure

### YAML Configuration File

**Location:** `processor/config/rain_processing.yml`

**Structure:**
```yaml
# Global settings for the rain processing service
global:
  # How often to reload this YAML file (seconds)
  config_reload_interval_seconds: 60
  
  # Default poll interval if not specified per user (seconds)
  default_poll_interval_seconds: 900  # 15 minutes
  
  # Default data window if not specified per user (minutes)
  default_data_window_minutes: 90

# Per-user configuration
users:
  demo_openmrg:
    enabled: true                          # Enable/disable processing for this user
    processing_variant: default            # Which workflow to use (maps to processor/workflows/<variant>.py)
    poll_interval_seconds: 900             # Process every 15 minutes
    data_window_minutes: 90                # Fetch 90 minutes of data for context
  
  demo_orange_cameroun:
    enabled: false                         # Disabled by default
    processing_variant: orange_cameroun_v1
    poll_interval_seconds: 60              # Process every 1 minute
    data_window_minutes: 120               # Fetch 120 minutes of data

# To add a new user, simply add an entry under 'users'
# The processor will automatically pick it up on next config reload
```

**Validation Rules:**
- `enabled` must be boolean
- `processing_variant` must exist in `processor/workflows/` directory
- `poll_interval_seconds` must be positive integer (recommend >= 60)
- `data_window_minutes` must be positive integer, typically >= `poll_interval_seconds/60`

**Pitfall Warning:**
- YAML is whitespace-sensitive. Use consistent indentation (2 spaces recommended).
- User IDs in YAML must match `user_id` values in the database exactly.
- If a user is enabled but their workflow variant doesn't exist, processing will fail with error log.

---

## State File Structure

### JSON State File

**Location:** `processor/data/state/rain_processing_state.json`

**Structure:**
```json
{
  "demo_openmrg": {
    "last_processed_time": "2026-06-19T10:30:00.000000Z"
  },
  "demo_orange_cameroun": {
    "last_processed_time": "2026-06-19T11:45:00.000000Z"
  }
}
```

**Initialization:**
- On first run, if a user has no entry in the state file, initialize `last_processed_time` to `datetime.utcnow().isoformat() + 'Z'`
- This ensures no backfill happens on first enable (starts from "now")

**Persistence:**
- File is written after each successful processing run
- Use atomic write pattern: write to temp file, then rename (to avoid corruption on crash)
- File must be stored in a Docker volume to persist across container restarts

**Pitfall Warning:**
- Timestamps must be in ISO 8601 format with timezone (UTC recommended)
- Use file locking to prevent concurrent writes (though only one processor instance should run)
- If state file is deleted/corrupted, system resets to "now" for all users (safe default)

---

## Python Module Structure

### Directory Layout

```
processor/
├── main.py                      # Entry point, main polling loop
├── config_loader.py             # YAML configuration loading and validation
├── state_manager.py             # JSON state file read/write with locking
├── data_interface.py            # Database interface for reading/writing CML data
├── dataset_builder.py           # Converts SQL query results into canonical xarray datasets
├── registry.py                  # Maps processing_variant names to workflow classes
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Container definition
├── config/                      # Configuration files (mounted volume)
│   └── rain_processing.yml
├── data/                        # State persistence (mounted volume)
│   └── state/
│       └── rain_processing_state.json
└── workflows/                   # Pluggable processing implementations
    ├── __init__.py
    ├── base.py                  # Abstract base class defining interface
    ├── default.py               # Default xarray-based implementation using pycomlink
    ├── openmrg_basic.py         # Basic OpenMRG-style workflow outline
    └── orange_cameroun_v1.py    # Example MNO-specific variant
```

---

## Module Specifications

### 1. `config_loader.py`

**Purpose:** Load and validate YAML configuration, with periodic reloading.

**Functions:**

```python
import yaml
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

class RainProcessingConfig:
    """
    Loads and manages rain processing configuration from YAML.
    Supports hot-reloading without service restart.
    """
    
    def __init__(self, config_path: str = "/app/config/rain_processing.yml"):
        """
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._last_loaded: datetime = None
        self.load()
    
    def load(self) -> None:
        """
        Load configuration from YAML file.
        Validates structure and raises ValueError if invalid.
        
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If YAML is malformed
            ValueError: If required fields are missing or invalid
        """
        # Implementation notes:
        # 1. Read YAML file
        # 2. Validate 'global' and 'users' sections exist
        # 3. Validate each user entry has required fields
        # 4. Set self._config and self._last_loaded
        pass
    
    def should_reload(self, reload_interval_seconds: int) -> bool:
        """
        Check if enough time has passed to reload config.
        
        Args:
            reload_interval_seconds: Time between reloads
        
        Returns:
            True if config should be reloaded
        """
        # Implementation: Check if (now - _last_loaded) >= reload_interval_seconds
        pass
    
    def get_global_config(self) -> Dict[str, Any]:
        """Get global configuration section."""
        return self._config.get('global', {})
    
    def get_user_config(self, user_id: str) -> Dict[str, Any]:
        """
        Get configuration for a specific user.
        
        Args:
            user_id: User identifier
        
        Returns:
            User config dict, or None if user not in config
        """
        return self._config.get('users', {}).get(user_id)
    
    def get_enabled_users(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all users where enabled=true.
        
        Returns:
            Dict mapping user_id to user config
        """
        users = self._config.get('users', {})
        return {uid: cfg for uid, cfg in users.items() if cfg.get('enabled', False)}
```

**Pitfall Warnings:**
- YAML parsing can raise various exceptions. Wrap in try/except and log clearly.
- Missing config file should be fatal error (service can't run without config).
- Invalid YAML should log error and keep previous valid config (don't crash mid-operation).
- File permissions: config file should be readable by the container user.

---

### 2. `state_manager.py`

**Purpose:** Manage persistent state (last processed timestamps) in JSON file.

**Functions:**

```python
import json
import fcntl
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict

class StateManager:
    """
    Manages persistent state for rain processing service.
    Uses file locking to ensure atomic updates.
    """
    
    def __init__(self, state_path: str = "/app/data/state/rain_processing_state.json"):
        """
        Args:
            state_path: Path to JSON state file
        """
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize empty state file if it doesn't exist
        if not self.state_path.exists():
            self._write_state({})
    
    def get_last_processed_time(self, user_id: str) -> Optional[datetime]:
        """
        Get the last processed timestamp for a user.
        
        Args:
            user_id: User identifier
        
        Returns:
            Last processed datetime (UTC), or None if user has no state
        """
        # Implementation notes:
        # 1. Read state file with shared lock
        # 2. Parse timestamp string to datetime
        # 3. Return None if user not found
        pass
    
    def update_last_processed_time(self, user_id: str, timestamp: datetime) -> None:
        """
        Update the last processed timestamp for a user.
        Uses atomic write pattern: write to temp file, then rename.
        
        Args:
            user_id: User identifier
            timestamp: New last processed time (should be UTC)
        """
        # Implementation notes:
        # 1. Read current state with exclusive lock
        # 2. Update user's timestamp
        # 3. Write to temp file
        # 4. Rename temp file over original (atomic on POSIX)
        # 5. Release lock
        pass
    
    def initialize_user(self, user_id: str, timestamp: Optional[datetime] = None) -> None:
        """
        Initialize state for a user if not already present.
        Defaults to current time (UTC) to avoid backfill.
        
        Args:
            user_id: User identifier
            timestamp: Initial timestamp (defaults to now)
        """
        if self.get_last_processed_time(user_id) is None:
            if timestamp is None:
                timestamp = datetime.utcnow()
            self.update_last_processed_time(user_id, timestamp)
    
    def _read_state(self) -> Dict:
        """Read state file with file locking."""
        # Use fcntl.flock(fd, fcntl.LOCK_SH) for shared lock
        # Return empty dict if file doesn't exist or is empty
        pass
    
    def _write_state(self, state: Dict) -> None:
        """Write state file atomically with file locking."""
        # Use fcntl.flock(fd, fcntl.LOCK_EX) for exclusive lock
        # Atomic write: write to .tmp file, then os.rename()
        pass
```

**Pitfall Warnings:**
- **File locking is critical** if multiple processes might access the file (though only one processor should run).
- Always use UTC for timestamps to avoid timezone confusion.
- State file corruption: If JSON parsing fails, log error and consider it empty (start fresh).
- Atomic writes prevent partial writes on crash: always write to temp file then rename.
- The state directory must be a mounted Docker volume or state won't persist across restarts.

---

### 3. `data_interface.py`

**Purpose:** Fixed interface for reading raw CML data and metadata, and writing processed rain data.

**Functions:**

```python
import pandas as pd
import psycopg2
from datetime import datetime
from typing import List, Optional

class CMLDataInterface:
    """
    Database interface for CML data operations.
    Provides fixed API for workflows to read/write data.
    """
    
    def __init__(self, database_url: str):
        """
        Args:
            database_url: PostgreSQL connection string
        """
        self.database_url = database_url
    
    def fetch_raw_cml_data_rows(
        self,
        user_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> pd.DataFrame:
        """
        Fetch raw CML data (RSL, TSL) for a time window as tabular rows.
        This is an internal helper for later conversion to xarray.
        
        Args:
            user_id: User identifier
            start_time: Window start (inclusive)
            end_time: Window end (inclusive)
        
        Returns:
            DataFrame with columns: time, cml_id, sublink_id, user_id, rsl, tsl
            Sorted by time ascending
            Empty DataFrame if no data found
        """
        # Implementation notes:
        # 1. Connect to database
        # 2. Query: SELECT time, cml_id, sublink_id, user_id, rsl, tsl 
        #           FROM cml_data 
        #           WHERE user_id = %s AND time >= %s AND time <= %s
        #           ORDER BY time ASC
        # 3. Return as pandas DataFrame
        # 4. Handle connection errors gracefully (log and raise)
        pass
    
    def fetch_cml_metadata_rows(
        self,
        user_id: str,
        cml_ids: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fetch CML metadata (coordinates, frequency, length, polarization) as tabular rows.
        
        Args:
            user_id: User identifier
            cml_ids: List of CML IDs to fetch (None = fetch all for user)
        
        Returns:
            DataFrame with columns: cml_id, sublink_id, site_0_lon, site_0_lat,
                                   site_1_lon, site_1_lat, frequency, polarization, length
            Empty DataFrame if no metadata found
        """
        # Implementation notes:
        # 1. Query cml_metadata table
        # 2. Filter by user_id and optionally cml_ids
        # 3. Return all metadata fields
        # 4. Handle case where metadata might be missing (return empty df, don't crash)
        pass
    
    def write_rain_data(self, rain_df: pd.DataFrame) -> int:
        """
        Write processed rain data to cml_rain_data table.
        
        Args:
            rain_df: DataFrame with columns: time, cml_id, sublink_id, user_id,
                     tl, wet, baseline, waa, a_rain, r
        
        Returns:
            Number of rows written
        
        Raises:
            ValueError: If required columns are missing
            psycopg2.Error: On database errors
        """
        # Implementation notes:
        # 1. Validate required columns exist
        # 2. Use batch INSERT with ON CONFLICT DO UPDATE (upsert)
        #    to handle duplicate timestamps gracefully
        # 3. Consider using COPY for performance if DataFrame is large (>1000 rows)
        # 4. Commit transaction
        # 5. Return row count
        pass
    
    def close(self):
        """Close database connection."""
        pass
```

**Pitfall Warnings:**
- **SQL injection**: Always use parameterized queries (`%s` placeholders), never string formatting.
- **Connection management**: Consider using connection pooling or context managers for clean resource handling.
- **Empty results**: Workflows must handle empty DataFrames gracefully (no data in time window).
- **Timezone awareness**: Database stores `TIMESTAMPTZ` (timezone-aware). Ensure datetime objects are UTC.
- **NULL handling**: RSL/TSL can be NULL in database. Workflows must handle NaN values in pandas.
- **ON CONFLICT**: Use `ON CONFLICT (time, cml_id, sublink_id, user_id) DO NOTHING` or `DO UPDATE` to avoid errors on re-processing same time window.

### 4. `dataset_builder.py`

**Purpose:** Convert SQL query results into a canonical `xarray.Dataset` structure for workflows.

This module is important because the less capable agent may otherwise mix ad-hoc `xarray` layouts between workflows. The layout must be fixed early and reused everywhere.

**Canonical dataset design:**
- Dimensions: `time`, `cml_id`, `sublink_id`
- Data variables from raw data: `rsl`, `tsl`
- Coordinate or data variables from metadata: `frequency`, `polarization`, `length`, `site_0_lon`, `site_0_lat`, `site_1_lon`, `site_1_lat`
- Scalar or attribute: `user_id`

**Important design note:**
- Some providers may effectively have one `sublink_id` per `cml_id`, others may have multiple.
- To keep the interface stable, always keep `sublink_id` as an explicit dimension, even if it has length 1 for some providers.

**Functions:**

```python
import pandas as pd
import xarray as xr

def build_cml_dataset(
    raw_rows: pd.DataFrame,
    metadata_rows: pd.DataFrame,
) -> xr.Dataset:
    """
    Build the canonical xarray dataset used by all workflows.

    Args:
        raw_rows: DataFrame with columns [time, cml_id, sublink_id, user_id, rsl, tsl]
        metadata_rows: DataFrame with columns [cml_id, sublink_id, site_0_lon, site_0_lat,
                       site_1_lon, site_1_lat, frequency, polarization, length]

    Returns:
        xr.Dataset with dimensions [time, cml_id, sublink_id]
        and variables [rsl, tsl, frequency, polarization, length, ...]
    """
    # Implementation notes:
    # 1. Validate required columns exist
    # 2. Ensure time is timezone-aware UTC and sorted
    # 3. Pivot raw rows into a regular xarray structure
    # 4. Attach metadata in a way that broadcasts correctly over time
    # 5. Store user_id in attrs if it is constant for the dataset
    pass


def flatten_rain_dataset(rain_ds: xr.Dataset) -> pd.DataFrame:
    """
    Convert processed xarray dataset back to tabular rows for DB writing.

    Expected variables in rain_ds:
        tl, wet, baseline, waa, a_rain, r
    """
    # Implementation notes:
    # 1. Convert dataset to DataFrame
    # 2. Reset index to columns
    # 3. Drop rows where all output variables are NaN
    # 4. Re-add user_id from attrs if needed
    pass
```

**Pitfall Warnings:**
- `xarray` does not magically solve irregular tabular data. The conversion step must define exactly how rows map to dimensions.
- Duplicate `(time, cml_id, sublink_id, user_id)` rows must be resolved before building the dataset.
- Metadata variables may be scalar per link, while raw variables vary over time. Broadcasting must be deliberate.
- String metadata like `polarization` may need special handling in `xarray`.
- If the agent uses inconsistent dimension order across workflows, later merging and flattening will become error-prone.

---

### 5. `workflows/base.py`

**Purpose:** Abstract base class defining the interface for all processing workflows.

**Code:**

```python
from abc import ABC, abstractmethod
import xarray as xr
from datetime import datetime

class BaseRainWorkflow(ABC):
    """
    Abstract base class for rain rate processing workflows.
    All workflow variants must inherit from this class and implement process().
    """
    
    @abstractmethod
    def process(
        self,
        cml_ds: xr.Dataset,
        window_start: datetime,
        window_end: datetime
    ) -> xr.Dataset:
        """
        Process raw CML data to estimate rain rates.
        
        Args:
                cml_ds: Canonical xarray dataset with dimensions [time, cml_id, sublink_id]
                    and variables including [rsl, tsl] plus metadata variables.
                    May contain NaN values and missing metadata.
            
            window_start: Start of processing window (for context)
            window_end: End of processing window
        
        Returns:
            xr.Dataset with dimensions [time, cml_id, sublink_id] and variables:
            - tl: total loss (TSL - RSL)
            - wet: boolean wet/dry classification
            - baseline: baseline attenuation level
            - waa: wet antenna attenuation estimate
            - a_rain: rain-induced attenuation
            - r: rain rate estimate (mm/h)
            
            The dataset should preserve coordinates and carry `user_id` in attrs.
            All fields can be NULL/NaN if processing fails for a timestamp.
        
        Notes:
            - Implementations should be robust to missing metadata
            - Should handle gaps in time series gracefully
            - Should not raise exceptions on bad data (log warnings, return partial results)
                        - Processing time window may be larger than output window (for temporal context)
                        - Output should usually be trimmed to the target interval that should be persisted,
                            even if a larger context window was used internally
        """
        pass
    
    def get_name(self) -> str:
        """Return workflow name for logging."""
        return self.__class__.__name__
```

**Pitfall Warnings:**
- **Interface compliance**: All workflow implementations MUST accept the same input signature and return the same output structure.
- **User ID**: Workflows must preserve `user_id`, preferably in `cml_ds.attrs['user_id']` and later in flattened output rows.
- **Error handling**: Workflows should catch internal errors and return partial results rather than crashing the entire processor.
- **Missing metadata**: Not all CMLs may have metadata (e.g., new CMLs not yet in metadata table). Skip or use defaults.
- **Temporal context**: `raw_data` may span longer than `window_start` to `window_end` to provide context for algorithms (e.g., baseline estimation needs history).

---

### 6. `workflows/default.py`

**Purpose:** Default implementation using `xarray` plus `pycomlink`-style processing.

**Skeleton:**

```python
from .base import BaseRainWorkflow
import xarray as xr
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DefaultRainWorkflow(BaseRainWorkflow):
    """
    Default rain rate processing workflow using xarray and pycomlink.
    
    Processing steps:
    1. Calculate total loss (TL = TSL - RSL)
    2. Wet/dry classification
    3. Baseline estimation
    4. Wet antenna attenuation (WAA) correction
    5. Rain-induced attenuation estimation
    6. Rain rate retrieval using power-law relationship
    """
    
    def process(
        self,
        cml_ds: xr.Dataset,
        window_start: datetime,
        window_end: datetime
    ) -> xr.Dataset:
        """
        Process CML data to rain rates using pycomlink algorithms.
        
        See BaseRainWorkflow.process() for parameter/return documentation.
        """
        logger.info("Processing CML dataset with default workflow")
        
        # TODO: Implementation details:
        # 1. Work directly on cml_ds with dimensions [time, cml_id, sublink_id]
        # 2. Calculate TL = TSL - RSL
        # 3. Apply pycomlink wet/dry classification (e.g., Schleiss algorithm)
        # 4. Estimate baseline (e.g., rolling window minimum during dry periods)
        # 5. Estimate WAA (e.g., constant or time-variable)
        # 6. Calculate A_rain = TL - baseline - WAA
        # 7. Convert A_rain to rain rate using frequency, polarization, length
        #    R = a * (A_rain / L)^b  where a, b depend on frequency/polarization
        # 8. Trim output to the target persistence interval if needed
        # 9. Return xr.Dataset with required variables
        
        # Placeholder return (implementation needed)
        out = xr.Dataset(coords=cml_ds.coords)
        out['tl'] = cml_ds['tsl'] - cml_ds['rsl']
        out['wet'] = xr.full_like(out['tl'], fill_value=False, dtype=bool)
        out['baseline'] = xr.full_like(out['tl'], fill_value=float('nan'))
        out['waa'] = xr.full_like(out['tl'], fill_value=float('nan'))
        out['a_rain'] = xr.full_like(out['tl'], fill_value=float('nan'))
        out['r'] = xr.full_like(out['tl'], fill_value=float('nan'))
        out.attrs['user_id'] = cml_ds.attrs.get('user_id')
        return out
```

**Pitfall Warnings:**
- **pycomlink API**: Ensure compatibility with installed pycomlink version. API may change between versions.
- **Parameter tuning**: Default parameters (e.g., wet/dry thresholds, power-law coefficients) may need tuning per MNO.
- **Frequency/polarization**: Power-law parameters (a, b) depend on frequency and polarization. Missing metadata means can't compute accurate rain rate.
- **Length units**: Ensure consistent units (typically km for length, GHz for frequency, mm/h for rain rate).
- **Time series alignment**: pycomlink may expect regularly-spaced time series. Irregular data needs interpolation or special handling.
- **xarray broadcasting**: Operations may silently broadcast over dimensions. The agent must verify shapes after each step.
- **Boolean arrays with NaN**: `wet` may need nullable handling if classification is unavailable for some timestamps.

### 7. `workflows/openmrg_basic.py`

**Purpose:** A basic workflow that follows the style of OpenMRG / pycomlink examples more closely.

This workflow is not meant to be the final scientific implementation. It is a structured starting point for a less capable agent.

**Suggested behavior for the first implementation:**
1. Build `tl = tsl - rsl`
2. Optionally resample or regularize time if required by the chosen pycomlink functions
3. Run a simple wet/dry classification on `tl`
4. Estimate a baseline from dry periods or a rolling dry reference
5. Estimate `waa` with a simple method or placeholder
6. Compute `a_rain = tl - baseline - waa`
7. Compute `r` from `a_rain`, `length`, `frequency`, and `polarization`
8. Return all intermediate variables in one `xarray.Dataset`

**Skeleton:**

```python
from .base import BaseRainWorkflow
import xarray as xr
import logging

logger = logging.getLogger(__name__)


class OpenMRGBasicWorkflow(BaseRainWorkflow):
    """
    Basic xarray-based workflow inspired by OpenMRG / pycomlink examples.
    """

    def process(self, cml_ds: xr.Dataset, window_start, window_end) -> xr.Dataset:
        out = xr.Dataset(coords=cml_ds.coords)

        # Step 1: total loss
        out['tl'] = cml_ds['tsl'] - cml_ds['rsl']

        # Step 2: wet/dry classification
        # Replace with actual pycomlink-based method once selected.
        out['wet'] = xr.full_like(out['tl'], False, dtype=bool)

        # Step 3: baseline estimation
        out['baseline'] = out['tl'].rolling(time=12, min_periods=1).min()

        # Step 4: wet antenna attenuation
        out['waa'] = xr.zeros_like(out['tl'])

        # Step 5: rain-induced attenuation
        out['a_rain'] = out['tl'] - out['baseline'] - out['waa']

        # Step 6: rain rate retrieval
        # Placeholder only. Real implementation must use frequency/polarization-dependent coefficients.
        out['r'] = xr.where(out['a_rain'] > 0, out['a_rain'], 0.0)

        out = out.sel(time=slice(window_start, window_end))
        out.attrs['user_id'] = cml_ds.attrs.get('user_id')
        return out
```

**Important note:**
- The above `r` computation is only a placeholder and is not scientifically correct.
- It is included only as a minimal end-to-end implementation target for the first coding pass.
- The real implementation should replace the placeholder steps with actual `pycomlink` / `poligrain` functions once the exact method is chosen.

**Pitfall Warnings:**
- A rolling minimum baseline is only a crude starting point.
- Some pycomlink examples assume a specific sampling interval. If DB data are irregular, resampling may be required before processing.
- If the agent resamples, it must document the chosen interval and aggregation rule.

---

### 8. `registry.py`

**Purpose:** Map processing variant names to workflow classes.

**Code:**

```python
from typing import Dict, Type
from .workflows.base import BaseRainWorkflow
from .workflows.default import DefaultRainWorkflow
from .workflows.openmrg_basic import OpenMRGBasicWorkflow
# Import other workflows as they're added
# from .workflows.orange_cameroun_v1 import OrangeCamerounV1Workflow

class WorkflowRegistry:
    """
    Registry mapping processing variant names to workflow classes.
    """
    
    _registry: Dict[str, Type[BaseRainWorkflow]] = {
        'default': DefaultRainWorkflow,
        'openmrg_basic': OpenMRGBasicWorkflow,
        # 'orange_cameroun_v1': OrangeCamerounV1Workflow,
    }
    
    @classmethod
    def get_workflow(cls, variant_name: str) -> BaseRainWorkflow:
        """
        Get a workflow instance by variant name.
        
        Args:
            variant_name: Name from config (e.g., 'default')
        
        Returns:
            Instance of the workflow class
        
        Raises:
            ValueError: If variant_name not in registry
        """
        workflow_class = cls._registry.get(variant_name)
        if workflow_class is None:
            raise ValueError(
                f"Unknown processing variant: {variant_name}. "
                f"Available: {list(cls._registry.keys())}"
            )
        return workflow_class()
    
    @classmethod
    def register(cls, variant_name: str, workflow_class: Type[BaseRainWorkflow]):
        """
        Register a new workflow variant.
        
        Args:
            variant_name: Name to use in config
            workflow_class: Class inheriting from BaseRainWorkflow
        """
        cls._registry[variant_name] = workflow_class
    
    @classmethod
    def list_variants(cls) -> list:
        """Return list of registered variant names."""
        return list(cls._registry.keys())
```

**Pitfall Warnings:**
- **Import errors**: If a workflow file has syntax errors or missing dependencies, the import will fail and crash the service.
  Consider dynamic imports with try/except if you want graceful degradation.
- **Name conflicts**: Variant names must be unique. Use descriptive names (e.g., `openmrg_v1`, not just `openmrg`).

---

### 9. `main.py`

**Purpose:** Main entry point with polling loop.

**Skeleton:**

```python
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from config_loader import RainProcessingConfig
from state_manager import StateManager
from data_interface import CMLDataInterface
from dataset_builder import build_cml_dataset, flatten_rain_dataset
from registry import WorkflowRegistry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Main polling loop for rain rate processing.
    
    Flow:
    1. Load configuration (with periodic reload)
    2. For each enabled user:
       a. Check if time to process (based on poll_interval and last_processed_time)
       b. If yes:
          - Fetch raw data for time window
          - Fetch metadata
          - Run workflow
          - Write results
          - Update state
    3. Sleep and repeat
    """
    
    # Get database URL from environment
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)
    
    # Initialize components
    config = RainProcessingConfig()
    state_manager = StateManager()
    data_interface = CMLDataInterface(database_url)
    
    logger.info("Rain processing service started")
    logger.info(f"Available workflows: {WorkflowRegistry.list_variants()}")
    
    # Main loop
    while True:
        try:
            # Reload config if needed
            global_config = config.get_global_config()
            reload_interval = global_config.get('config_reload_interval_seconds', 60)
            if config.should_reload(reload_interval):
                logger.info("Reloading configuration")
                config.load()
            
            # Process enabled users
            enabled_users = config.get_enabled_users()
            logger.debug(f"Enabled users: {list(enabled_users.keys())}")
            
            for user_id, user_config in enabled_users.items():
                try:
                    process_user_if_ready(
                        user_id,
                        user_config,
                        state_manager,
                        data_interface
                    )
                except Exception as e:
                    logger.error(f"Error processing user {user_id}: {e}", exc_info=True)
                    # Continue to next user (don't let one user's error stop others)
            
            # Sleep before next iteration
            # Use a short sleep to allow responsive config reloading
            time.sleep(10)
            
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            time.sleep(30)  # Back off on unexpected errors

def process_user_if_ready(
    user_id: str,
    user_config: dict,
    state_manager: StateManager,
    data_interface: CMLDataInterface
):
    """
    Check if user is ready for processing and run if so.
    
    Args:
        user_id: User identifier
        user_config: User configuration dict from YAML
        state_manager: State manager instance
        data_interface: Data interface instance
    """
    # Get last processed time from state
    last_processed = state_manager.get_last_processed_time(user_id)
    
    # Initialize if first run
    if last_processed is None:
        logger.info(f"Initializing state for new user: {user_id}")
        state_manager.initialize_user(user_id)
        return  # Don't process on first initialization (starts from now)
    
    # Check if enough time has passed
    now = datetime.utcnow()
    poll_interval_seconds = user_config.get('poll_interval_seconds', 900)
    time_since_last = (now - last_processed).total_seconds()
    
    if time_since_last < poll_interval_seconds:
        logger.debug(
            f"User {user_id}: {time_since_last:.0f}s since last processing "
            f"(need {poll_interval_seconds}s)"
        )
        return
    
    # Ready to process
    logger.info(f"Processing user: {user_id}")
    
    # Define time window
    window_end = now
    window_minutes = user_config.get('data_window_minutes', 90)
    window_start = window_end - timedelta(minutes=window_minutes)
    
    logger.info(
        f"User {user_id}: fetching data from {window_start} to {window_end} "
        f"({window_minutes} minutes)"
    )
    
    # Fetch data
    raw_rows = data_interface.fetch_raw_cml_data_rows(user_id, window_start, window_end)
    if raw_rows.empty:
        logger.warning(f"User {user_id}: no raw data in time window, skipping")
        # Still update state to avoid repeated processing attempts
        state_manager.update_last_processed_time(user_id, window_end)
        return
    
    cml_ids = raw_rows['cml_id'].unique().tolist()
    metadata_rows = data_interface.fetch_cml_metadata_rows(user_id, cml_ids)
    cml_ds = build_cml_dataset(raw_rows, metadata_rows)
    
    logger.info(
        f"User {user_id}: fetched {len(raw_rows)} raw data points, "
        f"{len(metadata_rows)} metadata records"
    )
    
    # Run workflow
    variant_name = user_config.get('processing_variant', 'default')
    try:
        workflow = WorkflowRegistry.get_workflow(variant_name)
        logger.info(f"User {user_id}: running workflow '{variant_name}'")
        
        rain_ds = workflow.process(cml_ds, window_start, window_end)
        rain_data = flatten_rain_dataset(rain_ds)
        
        if rain_data.empty:
            logger.warning(f"User {user_id}: workflow produced no output")
        else:
            logger.info(f"User {user_id}: workflow produced {len(rain_data)} results")
            rows_written = data_interface.write_rain_data(rain_data)
            logger.info(f"User {user_id}: wrote {rows_written} rows to database")
        
    except ValueError as e:
        logger.error(f"User {user_id}: invalid workflow variant '{variant_name}': {e}")
        return  # Don't update state on config error
    except Exception as e:
        logger.error(f"User {user_id}: workflow processing failed: {e}", exc_info=True)
        # Consider whether to update state on processing failure
        # For now, don't update to retry on next iteration
        return
    
    # Update state on success
    state_manager.update_last_processed_time(user_id, window_end)
    logger.info(f"User {user_id}: processing complete, state updated")

if __name__ == "__main__":
    main()
```

**Pitfall Warnings:**
- **Infinite loops**: Ensure the main loop has proper error handling to avoid crash-restart cycles.
- **Database connections**: Consider connection pooling or reconnection logic for long-running processes.
- **Memory leaks**: If processing large volumes, ensure DataFrames are freed after processing (Python GC usually handles this).
- **xarray size growth**: Converting sparse tabular data into dense arrays can increase memory usage. The agent should test realistic window sizes.
- **Clock drift**: Use UTC consistently. Container clock should sync with host.
- **Graceful shutdown**: Handle SIGTERM/SIGINT to close database connections cleanly.
- **Logging levels**: Use appropriate levels (DEBUG, INFO, WARNING, ERROR) for operational visibility.

---

## Docker Configuration

### Dockerfile

Update `processor/Dockerfile`:

```dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY *.py ./
COPY workflows/ ./workflows/

# Create directories for config and state
RUN mkdir -p /app/config /app/data/state

# Set environment variables
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
```

### docker-compose.yml

Update the processor service:

```yaml
processor:
  build: ./processor
  restart: unless-stopped  # Changed from default (restarts on failure)
  depends_on:
    database:
      condition: service_healthy
  environment:
    - DATABASE_URL=postgresql://myuser:mypassword@database:5432/mydatabase
  volumes:
    - ./processor/config:/app/config:ro           # Config file (read-only)
    - processor_state:/app/data/state             # State persistence
  # Remove ports if not needed (no HTTP API currently)
```

Add volume at bottom of file:

```yaml
volumes:
  # ... existing volumes ...
  processor_state:
```

**Pitfall Warnings:**
- **Volume permissions**: Ensure the container user has write access to `/app/data/state`.
- **Read-only config**: Mount config as `:ro` to prevent accidental modification from container.
- **Restart policy**: Use `unless-stopped` to auto-restart on crashes, but not if manually stopped.
- **Health checks**: Consider adding health check (e.g., timestamp of last successful processing).

---

## Requirements

Update `processor/requirements.txt`:

```
psycopg2-binary>=2.9.0
pandas>=1.5.0
xarray>=2023.0.0
pyyaml>=6.0
pycomlink>=0.4.0
poligrain
```

**Pitfall Warnings:**
- **Version pinning**: Consider pinning exact versions for reproducibility (e.g., `pandas==1.5.3`).
- **pycomlink dependencies**: May pull in scipy, numpy (large images). Consider Alpine base image if size matters.
- **Compatibility**: Test that pycomlink works with Python 3.10+ (check their docs).
- **xarray backend assumptions**: The agent should not assume NetCDF I/O is needed in the processor. `xarray` is used as an in-memory data model here.

---

## Implementation Checklist

### Phase 1: Database and Infrastructure
- [ ] Create migration file `database/migrations/010_add_rain_data_table.sql`
- [ ] Apply migration to database
- [ ] Create `processor/config/rain_processing.yml` with initial config
- [ ] Update `processor/requirements.txt`
- [ ] Update `processor/Dockerfile`
- [ ] Update `docker-compose.yml` (processor service + volume)

### Phase 2: Core Modules
- [ ] Implement `config_loader.py` with validation
- [ ] Implement `state_manager.py` with file locking
- [ ] Implement `data_interface.py` (read/write methods)
- [ ] Implement `dataset_builder.py` with one canonical xarray layout
- [ ] Create `workflows/base.py` abstract class
- [ ] Implement `registry.py`

### Phase 3: Processing Workflow
- [ ] Implement `workflows/default.py` (can start with placeholder that just calculates TL in xarray)
- [ ] Implement `workflows/openmrg_basic.py` as the first end-to-end xarray workflow
- [ ] Enhance `default.py` / `openmrg_basic.py` with actual pycomlink and poligrain algorithms (iterative)

### Phase 4: Main Service
- [ ] Implement `main.py` polling loop
- [ ] Add comprehensive logging
- [ ] Test end-to-end with one user

### Phase 5: Testing and Deployment
- [ ] Test config hot-reload
- [ ] Test state persistence across restarts
- [ ] Test with multiple users
- [ ] Test error handling (missing data, bad workflow name, etc.)
- [ ] Deploy and monitor

---

## Testing Strategy

### Unit Tests
- `config_loader.py`: Test YAML parsing, validation, reload logic
- `state_manager.py`: Test file locking, atomic writes, initialization
- `data_interface.py`: Test with mock database or test database
- Workflow classes: Test with synthetic input data

### Integration Tests
- End-to-end: Enable user in config, verify data written to `cml_rain_data`
- Test with empty database (no raw data)
- Test with missing metadata
- Test state file corruption recovery

### Operational Tests
- Config hot-reload (edit YAML while running)
- Container restart (verify state persists)
- Database connection loss (verify reconnection)
- Invalid workflow variant (verify error handling)

---

## Operational Notes

### Monitoring

**Key metrics to track:**
- Processing lag: `now() - last_processed_time` per user
- Processing duration per user
- Rows processed per run
- Error rate (failed processing attempts)

**Logging strategy:**
- INFO: Normal operations (processing started/completed, rows written)
- WARNING: Recoverable issues (no data, missing metadata)
- ERROR: Failures (workflow errors, database errors)

### Troubleshooting

**Processor not running:**
- Check logs: `docker logs gmdi_prototype-processor-1`
- Verify DATABASE_URL is set correctly
- Check config file exists and is valid YAML

**User not processing:**
- Check `enabled: true` in config
- Check state file for `last_processed_time`
- Verify `poll_interval_seconds` has passed
- Check for errors in logs for that specific user

**No output data:**
- Verify raw data exists in `cml_data` for the user
- Check time window (might be too narrow or offset)
- Check workflow logs for errors
- Verify metadata exists

**Reset processing for a user:**
```bash
# Edit state file (in volume or container)
# Remove user's entry or set last_processed_time to desired timestamp
# Or delete entire state file to reset all users to "now"
```

### Performance Tuning

**If processing is slow:**
- Reduce `data_window_minutes` (less data to fetch)
- Increase `poll_interval_seconds` (process less frequently)
- Optimize workflow algorithms (profiling needed)
- Add database indexes on `cml_data(user_id, time)`

**If processing lags behind real-time:**
- Decrease `poll_interval_seconds`
- Increase resources (CPU, memory) for processor container
- Consider parallel processing (one processor per user)

---

## Future Enhancements

**Potential additions (not in current scope):**

1. **Backfill support**: Add flag in config to process historical data
2. **Parallel processing**: Run multiple workflow instances concurrently
3. **Real-time streaming**: Switch from polling to event-driven (LISTEN/NOTIFY)
4. **Quality control**: Add QC flags to output data
5. **Aggregated views**: Create materialized view for hourly/daily rain rates
6. **Web API**: Add HTTP endpoint to trigger/configure processing
7. **Metrics endpoint**: Expose Prometheus metrics for monitoring
8. **Workflow versioning**: Track which workflow version processed each record

---

## Notebook-Based Validation Workflow

Before enabling continuous processing for a user, it should be possible to test whether the selected workflow produces sensible results for a chosen day or time window.

The recommended approach is a **Jupyter notebook working example** that imports and runs the **exact same workflow code** used by the continuous processor.

This is important: the notebook must **not** reimplement the processing logic separately. It should call the same modules as the service, otherwise the notebook and the service may diverge over time.

### Goal

Provide a reproducible, inspectable workflow for:
- selecting one user
- selecting one day or arbitrary time window
- loading raw data and metadata from the database
- building the canonical `xarray.Dataset`
- running one configured workflow variant
- plotting intermediate and final outputs
- visually checking whether the processing looks plausible before enabling continuous mode

### Recommended Notebook File

Create a notebook such as:

`notebooks/rain_processing_validation.ipynb`

This notebook should be treated as a **working example** and a **manual validation tool**.

### Required Reuse of Production Code

The notebook should import the same modules used by the processor service:

- `processor.config_loader`
- `processor.data_interface`
- `processor.dataset_builder`
- `processor.registry`
- workflow modules from `processor.workflows`

The notebook should **not** contain copied versions of:
- SQL queries
- dataset conversion logic
- workflow logic

Instead, it should call the production functions directly.

### Suggested Notebook Flow

#### 1. Setup and imports

The notebook should:
- import `os`, `datetime`, `matplotlib`, `xarray`
- import the production processor modules
- read `DATABASE_URL`

#### 2. User-selectable parameters

The notebook should expose a small parameter section near the top, for example:

```python
USER_ID = "demo_openmrg"
WORKFLOW_VARIANT = "openmrg_basic"
START_TIME = "2026-01-20T00:00:00Z"
END_TIME = "2026-01-21T00:00:00Z"
PLOT_CML_ID = None
PLOT_SUBLINK_ID = None
```

This keeps the notebook easy to reuse.

#### 3. Load raw rows and metadata

Use `CMLDataInterface` methods to fetch:
- raw CML rows from `cml_data`
- metadata rows from `cml_metadata`

#### 4. Build canonical xarray dataset

Use `build_cml_dataset()` from `processor.dataset_builder`.

This ensures the notebook uses the same in-memory structure as the service.

#### 5. Load workflow from registry

Use `WorkflowRegistry.get_workflow(WORKFLOW_VARIANT)`.

This is important because it guarantees the notebook runs the same workflow variant that continuous processing would run.

#### 6. Run processing

Call:

```python
rain_ds = workflow.process(cml_ds, window_start, window_end)
```

#### 7. Plot results

At minimum, the notebook should plot for one selected link:
- `tl`
- `wet`
- `baseline`
- `waa`
- `a_rain`
- `r`

Recommended plotting style:
- one figure with multiple aligned subplots sharing the same time axis
- optional overlay of `tl`, `baseline`, and `a_rain`
- optional highlighting of wet periods

#### 8. Optional export

The notebook may optionally:
- convert `rain_ds` to a DataFrame
- save results to CSV for inspection
- save plots to PNG

It should **not** write to `cml_rain_data` by default.

### Minimal Plotting Expectations

The notebook should include at least:

1. **Time series plot of TL and baseline**
2. **Wet/dry indicator plot**
3. **Time series plot of A_rain**
4. **Time series plot of R**

If possible, also include:
- a quick summary table of missing values
- number of wet timestamps
- min/max/mean of `r`

### Why Notebook Validation Is Useful

This helps catch issues before continuous processing is enabled, for example:
- wrong sign convention in `tl = tsl - rsl`
- unrealistic baseline behavior
- `waa` correction too large or too small
- rain rates always zero or unrealistically high
- metadata problems such as missing length or frequency
- irregular time sampling causing unstable results

### Pitfall Warnings for the Implementing Agent

- The notebook must import the production workflow code, not a notebook-only copy.
- If the notebook duplicates logic, later changes in the service may not be reflected in validation.
- The notebook should use the same canonical `xarray` layout as the service.
- Plotting all links at once may be too heavy; default to one selected `cml_id` / `sublink_id`.
- Some workflows may require a context window larger than the plotted interval. The notebook should allow this.
- If the workflow trims output to the target interval, the notebook should make that explicit.
- The notebook should fail clearly if the selected workflow variant is not registered.
- The notebook should not silently write to production tables.

### Suggested Supporting Helper Functions

To keep the notebook simple, it is useful to add small reusable helper functions in normal Python modules, for example in:

`processor/plotting.py`

Possible helper functions:

```python
def select_single_link(ds, cml_id=None, sublink_id=None):
    """Return a dataset slice for one link for plotting."""

def plot_rain_workflow_overview(ds):
    """Create a standard multi-panel plot for tl, wet, baseline, waa, a_rain, r."""

def summarize_rain_dataset(ds):
    """Return simple summary statistics for validation."""
```

These helpers should also be importable by future scripts or tests.

### Recommended Scope for First Implementation

For the first implementation, keep the notebook simple:
- one user
- one selected day
- one workflow variant
- one selected link plotted
- no DB writes

That is enough to validate whether the workflow behaves plausibly.

### Relationship to Continuous Processing

The intended workflow is:

1. implement or update a workflow in `processor/workflows/`
2. test it in the notebook for a selected day
3. inspect plots and summary statistics
4. only then enable the workflow in `processor/config/rain_processing.yml`

This should be the recommended operational path.

---

## Planned Website Integration

The website presentation of processed rain data has now been decided at a high level.

### New Subsite

Add a new subsite named:

- `Rain rates`

This subsite should follow the **same general layout as the existing real-time subsite**.

This is recommended because:
- users already know that layout
- the existing map interaction model can be reused
- implementation effort is lower than creating a completely new page design

### Map Behavior

The map should reuse the same CML geometry and general interaction pattern as the real-time page.

The main difference is the coloring mode.

The map should support at least these coloring options:

1. **Last rain rate value**
2. **Rainfall sum of last hour**
3. **Rainfall sum of last day**

### Definition of Rainfall Sum

For this project, the rainfall sum over a time window should be defined as:

- the **average of the rain-rate values** over that window

This is the chosen definition because it also works when:
- the time step `delta_t` is not constant
- different processing variants produce different sampling intervals

So for a selected interval, such as the last hour or last day:

$$
	ext{rainfall sum proxy} = \frac{1}{N} \sum_{i=1}^{N} R_i
$$

where $R_i$ are the rain-rate values available in that interval.

**Important note for the implementing agent:**
- This is not a physical time integral of rainfall depth.
- It is a project-specific aggregation rule chosen for robustness and simplicity.
- The implementation must not silently replace this with a weighted sum over time unless the plan is updated later.

### Grafana / Detail Panel Behavior

When a user clicks a CML on the map:

- the detail panel should show the **processed rain-rate time series** for that CML
- the Grafana panel should update based on the selected CML
- the interaction should mirror the current real-time page behavior as closely as possible

The intended behavior is:

1. user opens `Rain rates`
2. user selects a map coloring mode
3. user clicks a CML
4. Grafana panel updates to show processed rain-rate data for that CML

### Backend / Query Implications

To support the website efficiently, the backend will likely need query support for:

- latest rain-rate value per CML
- average rain-rate over the last hour per CML
- average rain-rate over the last day per CML

These may later be implemented as:
- SQL views
- materialized views
- Timescale continuous aggregates

The exact implementation is not fixed yet, but the frontend target behavior is fixed.

### Recommended Data Products for the Website

The following derived products will likely be needed:

1. **Latest rain-rate per CML**
2. **1-hour average rain-rate per CML**
3. **24-hour average rain-rate per CML**

These products should be user-scoped in the same way as the rest of the application.

### Pitfall Warnings for the Implementing Agent

- Do not assume “rainfall sum” means a time integral. In this plan it explicitly means the average of available rain-rate values in the interval.
- If there are no values in the interval, the result should be `NULL` / missing, not zero.
- The map geometry should be reused from the existing real-time page rather than rebuilt separately.
- The Grafana panel should show processed rain-rate data, not raw RSL/TSL.
- The selected CML identifier used by the map must match the identifier used in the processed rain-rate queries.
- If the real-time page uses a specific route, template, or API structure, the new `Rain rates` page should follow the same pattern where possible.

### Scope Note

This section defines the intended UI behavior, but it does **not** yet fully specify:
- exact webserver routes
- exact SQL/API endpoints
- exact Grafana dashboard/panel configuration

Those can be designed later, but the target behavior is now fixed enough for planning.

---

## Summary

This implementation provides a robust, maintainable foundation for continuous rain rate processing:

✅ **Simple configuration** via YAML (version-controlled, easy to edit)  
✅ **Persistent state** via JSON (survives restarts)  
✅ **Pluggable workflows** (easy to add MNO-specific variants)  
✅ **Fixed interface** between data layer and processing logic  
✅ **No backfill** by default (starts from "now" when enabled)  
✅ **Per-user control** of frequency and data window  
✅ **Robust error handling** (one user's failure doesn't affect others)  

The modular design allows incremental development: start with a simple workflow that just calculates TL, then gradually add wet/dry classification, baseline estimation, and full rain rate retrieval as the algorithms are refined.
