#!/usr/bin/env python3
"""
Load archive CML data directly from NetCDF to database.

This script reads a NetCDF file, shifts timestamps to end at current time,
and loads data directly into the database using PostgreSQL COPY FROM.
This is optimized for large datasets (millions of rows).

The script preserves the original temporal resolution and time span of the NetCDF file.
"""

import os
import sys
import io
from datetime import datetime, timedelta
import logging
from pathlib import Path

import xarray as xr
import pandas as pd
import numpy as np
import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Paths - use parser's own directory structure
SCRIPT_DIR = Path(__file__).parent
EXAMPLE_DATA_DIR = SCRIPT_DIR / "example_data"
EXAMPLE_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Configuration from environment variables
NETCDF_FILE = os.getenv(
    "ARCHIVE_NETCDF_FILE", str(EXAMPLE_DATA_DIR / "openMRG_cmls_20150827_3months.nc")
)
NETCDF_URL = os.getenv(
    "ARCHIVE_NETCDF_URL", "https://bwsyncandshare.kit.edu/s/jSAFftGXcJjQbSJ/download"
)

# Limit time range (in days from end) - set to None for full dataset
# For demo purposes, default to 7 days to avoid overwhelming the database
MAX_DAYS = int(os.getenv("ARCHIVE_MAX_DAYS", "7"))  # Set to 0 for full dataset

# Database connection from environment
DB_NAME = os.getenv("POSTGRES_DB", "mydatabase")
DB_USER = os.getenv("POSTGRES_USER", "myuser")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mypassword")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Batch size for COPY operations (balance memory vs transaction size)
BATCH_SIZE = 1000  # timestamps per batch (1000 × 728 = 728K rows per batch)


def download_netcdf(url, output_path):
    """Download NetCDF file if it doesn't exist."""
    if os.path.exists(output_path):
        logger.info(f"NetCDF file already exists: {output_path}")
        return

    logger.info(f"Downloading NetCDF file from {url}...")

    import urllib.request

    def progress_hook(count, block_size, total_size):
        percent = int(count * block_size * 100 / total_size)
        if count % 100 == 0:  # Update every 100 blocks
            logger.info(f"  Download progress: {percent}%")

    try:
        urllib.request.urlretrieve(url, output_path, reporthook=progress_hook)
        logger.info("Download complete!")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise


def load_metadata_from_netcdf(ds):
    """Extract CML metadata from NetCDF dataset."""
    logger.info("Extracting metadata from NetCDF...")

    # NetCDF dimensions: (sublink_id=2, cml_id=364, time=...)
    # cml_id: (364,) - unique CML site IDs
    # site coords: (364,) - one per CML
    # frequency, polarization: (2, 364) - one per (sublink, cml)

    cml_ids = ds.cml_id.values  # (364,)
    site_0_lon = ds.site_0_lon.values  # (364,)
    site_0_lat = ds.site_0_lat.values  # (364,)
    site_1_lon = ds.site_1_lon.values  # (364,)
    site_1_lat = ds.site_1_lat.values  # (364,)
    frequency = ds.frequency.values  # (2, 364)
    polarization = ds.polarization.values  # (2, 364)

    # Calculate link length using haversine formula
    def haversine_distance(lon1, lat1, lon2, lat2):
        R = 6371000  # Earth radius in meters
        phi1, phi2 = np.radians(lat1), np.radians(lat2)
        dphi = np.radians(lat2 - lat1)
        dlambda = np.radians(lon2 - lon1)
        a = (
            np.sin(dphi / 2) ** 2
            + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
        )
        return 2 * R * np.arcsin(np.sqrt(a))

    length = haversine_distance(site_0_lon, site_0_lat, site_1_lon, site_1_lat)

    # Create metadata records (728 total: 364 CMLs × 2 sublinks)
    metadata_records = []

    for cml_idx, cml_id in enumerate(cml_ids):
        for sublink_idx in range(2):  # 0 and 1
            sublink_id = f"sublink_{sublink_idx + 1}"

            metadata_records.append(
                {
                    "cml_id": str(cml_id),
                    "sublink_id": sublink_id,
                    "site_0_lon": float(site_0_lon[cml_idx]),
                    "site_0_lat": float(site_0_lat[cml_idx]),
                    "site_1_lon": float(site_1_lon[cml_idx]),
                    "site_1_lat": float(site_1_lat[cml_idx]),
                    "frequency": float(frequency[sublink_idx, cml_idx]),
                    "polarization": str(polarization[sublink_idx, cml_idx]),
                    "length": float(length[cml_idx]),
                }
            )

    metadata_df = pd.DataFrame(metadata_records)
    logger.info(
        f"Extracted {len(metadata_df)} metadata records ({metadata_df['cml_id'].nunique()} unique CML IDs)"
    )

    return metadata_df


def copy_dataframe_to_db(cursor, df, table_name, columns):
    """Use PostgreSQL COPY FROM to efficiently load dataframe."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cursor.copy_from(buffer, table_name, sep=",", null="\\N", columns=columns)


def load_timeseries_from_netcdf(ds, metadata_df, cursor, conn):
    """
    Load time-series data from NetCDF with shifted timestamps.

    Preserves original temporal resolution and shifts timestamps
    so that the data ends at the current time.
    """
    logger.info("Loading time-series data...")

    # Get original timestamps
    original_times = pd.to_datetime(ds.time.values)
    n_timestamps_full = len(original_times)

    # Limit to recent data if MAX_DAYS is set
    if MAX_DAYS > 0:
        # Calculate how many timestamps for MAX_DAYS
        # Assuming 10-second resolution: 86400 / 10 = 8640 timestamps per day
        timestamps_per_day = 8640
        max_timestamps = MAX_DAYS * timestamps_per_day

        if n_timestamps_full > max_timestamps:
            start_idx = n_timestamps_full - max_timestamps
            original_times = original_times[start_idx:]
            logger.info(
                f"Limiting to last {MAX_DAYS} days ({max_timestamps:,} timestamps)"
            )
        else:
            start_idx = 0
    else:
        start_idx = 0

    n_timestamps = len(original_times)

    # Calculate time shift to end at current time
    current_time = pd.Timestamp.now()
    time_shift = current_time - original_times[-1]
    shifted_times = original_times + time_shift

    logger.info(f"Original time range: {original_times[0]} to {original_times[-1]}")
    logger.info(f"Shifted time range:  {shifted_times[0]} to {shifted_times[-1]}")
    logger.info(f"Time shift applied: {time_shift}")

    # Get NetCDF dimensions
    # NetCDF shape: (sublink_id=2, cml_id=364, time=794887)
    # But we need (time, sublink, cml) for iteration
    n_sublinks = ds.sizes["sublink_id"]  # 2
    n_cmls = ds.sizes["cml_id"]  # 364
    cml_ids_nc = ds.cml_id.values  # (364,)

    # Build CML ID to DB mapping from metadata
    # metadata_df has 728 rows (364 CMLs × 2 sublinks)
    cml_to_dbid = {}  # Maps (cml_id, sublink_id) -> (cml_id_str, sublink_id_str)
    for _, row in metadata_df.iterrows():
        cml_to_dbid[(row["cml_id"], row["sublink_id"])] = (
            row["cml_id"],
            row["sublink_id"],
        )

    # Calculate total rows (728 CMLs × timestamps)
    total_cmls = len(metadata_df)
    total_rows = n_timestamps * total_cmls
    logger.info(
        f"Total data points: {n_timestamps:,} timestamps × {total_cmls} CMLs = {total_rows:,} rows"
    )
    logger.info(f"Processing in batches of {BATCH_SIZE:,} timestamps...")

    start_time = datetime.now()
    rows_loaded = 0

    # Process in batches to manage memory
    for batch_num, batch_start_rel in enumerate(range(0, n_timestamps, BATCH_SIZE), 1):
        batch_end_rel = min(batch_start_rel + BATCH_SIZE, n_timestamps)
        batch_times = shifted_times[batch_start_rel:batch_end_rel]
        batch_size_actual = batch_end_rel - batch_start_rel

        # Convert relative indices to absolute NetCDF indices
        batch_start_abs = start_idx + batch_start_rel
        batch_end_abs = start_idx + batch_end_rel

        # Load only this batch's data from NetCDF
        # NetCDF data dimensions: (time, sublink_id, cml_id) = (794887, 2, 364)
        tsl_batch = ds.tsl.isel(
            time=slice(batch_start_abs, batch_end_abs)
        ).values  # (batch_size, 2, 364)
        rsl_batch = ds.rsl.isel(
            time=slice(batch_start_abs, batch_end_abs)
        ).values  # (batch_size, 2, 364)

        # Reshape data for database insertion
        # Create arrays for each column
        batch_rows = batch_size_actual * n_cmls * n_sublinks

        # Pre-allocate arrays
        times_arr = np.empty(batch_rows, dtype="datetime64[ns]")
        cml_ids_arr = np.empty(batch_rows, dtype=object)
        sublink_ids_arr = np.empty(batch_rows, dtype=object)
        tsl_arr = np.empty(batch_rows, dtype=float)
        rsl_arr = np.empty(batch_rows, dtype=float)

        idx = 0
        for t_idx, timestamp in enumerate(batch_times):
            for cml_idx, cml_id in enumerate(cml_ids_nc):
                for sublink_idx in range(n_sublinks):
                    times_arr[idx] = timestamp
                    cml_ids_arr[idx] = str(cml_id)
                    sublink_ids_arr[idx] = f"sublink_{sublink_idx + 1}"
                    tsl_arr[idx] = tsl_batch[t_idx, sublink_idx, cml_idx]
                    rsl_arr[idx] = rsl_batch[t_idx, sublink_idx, cml_idx]
                    idx += 1

        # Create DataFrame from arrays
        batch_df = pd.DataFrame(
            {
                "time": times_arr,
                "cml_id": cml_ids_arr,
                "sublink_id": sublink_ids_arr,
                "tsl": tsl_arr,
                "rsl": rsl_arr,
            }
        )

        # Load batch to database
        copy_dataframe_to_db(
            cursor,
            batch_df,
            "cml_data",
            ["time", "cml_id", "sublink_id", "tsl", "rsl"],
        )

        rows_loaded += len(batch_df)

        # Log progress every batch
        elapsed = (datetime.now() - start_time).total_seconds()
        progress = (rows_loaded / total_rows) * 100
        rate = rows_loaded / elapsed if elapsed > 0 else 0

        total_batches = (n_timestamps + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(
            f"  Batch {batch_num}/{total_batches}: "
            f"{progress:5.1f}% complete, {rate:,.0f} rows/sec"
        )

        # Commit periodically (every 10 batches)
        if (batch_num % 10) == 0 or batch_end_rel == n_timestamps:
            conn.commit()
            logger.info(f"  ✓ Committed to database")

    total_duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Loaded {rows_loaded:,} data records in {total_duration:.0f} seconds")

    return rows_loaded


def main():
    """Main function to load archive data from NetCDF to database."""

    logger.info("=" * 70)
    logger.info("NetCDF to Database Archive Loader")
    logger.info("=" * 70)
    logger.info(f"NetCDF file: {NETCDF_FILE}")

    # Download NetCDF if needed
    if not os.path.exists(NETCDF_FILE):
        if NETCDF_URL:
            download_netcdf(NETCDF_URL, NETCDF_FILE)
        else:
            logger.error(
                f"NetCDF file not found and no download URL provided: {NETCDF_FILE}"
            )
            sys.exit(1)

    # Open NetCDF dataset
    logger.info("Opening NetCDF dataset...")
    try:
        ds = xr.open_dataset(NETCDF_FILE)
    except Exception as e:
        logger.error(f"Failed to open NetCDF file: {e}")
        sys.exit(1)

    # Connect to database
    logger.info("Connecting to database...")
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        conn.autocommit = False
        cursor = conn.cursor()
        logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        ds.close()
        sys.exit(1)

    try:
        # Clear existing data before loading archive
        logger.info("Clearing existing database data...")
        cursor.execute("TRUNCATE TABLE cml_data")
        cursor.execute("TRUNCATE TABLE cml_metadata")
        conn.commit()
        logger.info("Existing data cleared")

        # Load metadata
        metadata_df = load_metadata_from_netcdf(ds)

        logger.info("Loading metadata to database...")
        copy_dataframe_to_db(
            cursor,
            metadata_df,
            "cml_metadata",
            [
                "cml_id",
                "sublink_id",
                "site_0_lon",
                "site_0_lat",
                "site_1_lon",
                "site_1_lat",
                "frequency",
                "polarization",
                "length",
            ],
        )
        conn.commit()
        logger.info(f"✓ Loaded {len(metadata_df)} metadata records")

        # Load time-series data
        rows_loaded = load_timeseries_from_netcdf(ds, metadata_df, cursor, conn)

        # Verify loaded data
        cursor.execute(
            """
            SELECT 
                MIN(time) as start_time,
                MAX(time) as end_time,
                COUNT(*) as total_rows
            FROM cml_data
        """
        )
        result = cursor.fetchone()

        logger.info("=" * 70)
        logger.info("Archive Data Successfully Loaded!")
        logger.info("=" * 70)
        logger.info(f"Time range: {result[0]} to {result[1]}")
        logger.info(f"Total rows: {result[2]:,}")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
        ds.close()


if __name__ == "__main__":
    main()
