#!/usr/bin/env python3
"""
Generate archive CML data for database initialization.

This script uses the existing CMLDataGenerator to create archive data
with real RSL/TSL values from the NetCDF file, but with fake timestamps
spanning the configured archive period.

Usage:
    python generate_archive.py [--days N] [--interval-seconds S] [--output-dir PATH] [--netcdf-file PATH]

Environment variables (fallbacks for CLI args):
    ARCHIVE_DAYS              Number of days of history to generate (default: 7)
    ARCHIVE_INTERVAL_SECONDS  Time resolution in seconds between data points (default: 10)
    ARCHIVE_OUTPUT_DIR        Output directory for archive files
    NETCDF_FILE               Path to the NetCDF source file
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging
import numpy as np
import pandas as pd

from data_generator import CMLDataGenerator, ensure_netcdf_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Defaults (overridable via CLI args or environment variables)
# Production uses the 3-month / 10-second-resolution file (downloaded at startup).
# Tests point to the small 12-hour file directly via the --netcdf-file flag.
DEFAULT_NETCDF_FILE = "../parser/example_data/openMRG_cmls_20150827_3months.nc"
DEFAULT_NETCDF_FILE_URL = "https://bwsyncandshare.kit.edu/s/jSAFftGXcJjQbSJ/download"
DEFAULT_OUTPUT_DIR = "../database/archive_data"
DEFAULT_ARCHIVE_DAYS = 1
DEFAULT_INTERVAL_SECONDS = 10

# Output files
METADATA_OUTPUT = "metadata_archive.csv"
DATA_OUTPUT = "data_archive.csv"


def generate_archive_data(
    archive_days, output_dir, netcdf_file, interval_seconds, netcdf_file_url=None
):
    """Generate archive metadata and time-series data."""

    netcdf_path = Path(netcdf_file)
    if not netcdf_path.is_absolute():
        netcdf_path = Path(__file__).parent / netcdf_file

    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = Path(__file__).parent / output_dir

    ensure_netcdf_file(netcdf_path, netcdf_file_url)

    if not netcdf_path.exists():
        logger.error(f"NetCDF file not found: {netcdf_path}")
        sys.exit(1)

    output_path.mkdir(parents=True, exist_ok=True)

    archive_end_date = datetime.now()
    archive_start_date = archive_end_date - timedelta(days=archive_days)

    logger.info("=" * 60)
    logger.info("Generating Archive Data from NetCDF")
    logger.info("=" * 60)
    logger.info(f"NetCDF file: {netcdf_path}")
    logger.info(
        f"Archive period: {archive_start_date} to {archive_end_date} ({archive_days} days)"
    )

    # Initialize the data generator
    generator = CMLDataGenerator(
        netcdf_file=str(netcdf_path),
        loop_duration_seconds=archive_days * 24 * 3600,  # bounds the replay window
    )

    # Generate and save metadata using existing function
    logger.info("\nGenerating metadata...")
    metadata_path = output_path / METADATA_OUTPUT
    metadata_df = generator.get_metadata_dataframe()

    metadata_df.to_csv(metadata_path, index=False)

    logger.info(f"Saved {len(metadata_df)} metadata rows to {metadata_path}")
    logger.info(f"  Unique CML IDs: {metadata_df['cml_id'].nunique()}")

    # Generate timestamps for the archive period with configured interval
    logger.info(f"\nGenerating time-series data...")
    logger.info(f"  Time interval: {interval_seconds} seconds")

    timestamps = pd.date_range(
        start=archive_start_date,
        end=archive_end_date,
        freq=f"{interval_seconds}s",
    )

    logger.info(f"  Total timestamps: {len(timestamps):,}")
    logger.info(f"  Total rows (estimate): {len(timestamps) * len(metadata_df):,}")

    # Set the generator's loop start time to archive start
    generator.loop_start_time = archive_start_date

    # --- Fast numpy-cached generation ---
    # Map each archive timestamp to a NetCDF index.
    all_indices = np.array(
        [generator._get_netcdf_index_for_timestamp(ts) for ts in timestamps]
    )
    unique_indices = np.unique(all_indices)
    logger.info(
        f"  Unique NetCDF time slices needed: {len(unique_indices)} "
        f"(of {len(generator.original_time_points)} in file)"
    )

    # Load RSL/TSL for all needed time steps in one contiguous slice.
    # unique_indices are always low-numbered (they start at 0 for any archive
    # shorter than the source file), so reading slice(0, max+1) is a single
    # sequential disk read — much faster than indexed/fancy access.
    logger.info("  Loading RSL/TSL arrays from NetCDF (one contiguous slice)...")
    max_idx = int(unique_indices.max())
    ds_slice = generator.dataset[["rsl", "tsl"]].isel(time=slice(0, max_idx + 1))
    ds_stacked = ds_slice.stack(link=("cml_id", "sublink_id"))
    rsl_arr = ds_stacked["rsl"].values  # shape: (max_idx+1, n_links)
    tsl_arr = ds_stacked["tsl"].values
    # Recover per-link identifiers from the stacked MultiIndex
    link_index = ds_stacked.indexes["link"]
    cml_ids = np.array([v[0] for v in link_index])
    sublink_ids = np.array([v[1] for v in link_index])
    n_links = len(cml_ids)
    # For a 0-based slice the original index IS the row in rsl_arr/tsl_arr
    idx_to_row = {int(idx): int(idx) for idx in unique_indices}
    logger.info(
        f"  Loaded {max_idx + 1} time slices × {n_links} links, generating output..."
    )

    # Write in batches using pre-cached numpy arrays
    batch_size = 5000  # timestamps per batch (not rows)
    total_rows = 0
    data_path = output_path / DATA_OUTPUT

    with open(data_path, "w") as f:
        first_batch = True
        for i in range(0, len(timestamps), batch_size):
            batch_ts = timestamps[i : i + batch_size]
            batch_indices = all_indices[i : i + batch_size]
            batch_n = len(batch_ts)

            time_col = np.repeat(batch_ts.values, n_links)
            cml_col = np.tile(cml_ids, batch_n)
            sub_col = np.tile(sublink_ids, batch_n)
            rows = [idx_to_row[int(idx)] for idx in batch_indices]
            tsl_col = tsl_arr[rows, :].ravel()
            rsl_col = rsl_arr[rows, :].ravel()

            df = pd.DataFrame(
                {
                    "time": time_col,
                    "cml_id": cml_col,
                    "sublink_id": sub_col,
                    "tsl": tsl_col,
                    "rsl": rsl_col,
                }
            )
            df.to_csv(f, index=False, header=first_batch)
            first_batch = False
            total_rows += len(df)

            progress_interval = max(batch_size, len(timestamps) // 10)
            if (i + batch_size) % progress_interval < batch_size:
                progress = min(100, (i + batch_size) / len(timestamps) * 100)
                logger.info(f"  Progress: {progress:.0f}% ({total_rows:,} rows)")

    logger.info(f"\nSaved {total_rows:,} data rows to {data_path}")

    # Report file sizes
    metadata_size = metadata_path.stat().st_size / 1024
    data_size = data_path.stat().st_size / (1024 * 1024)
    logger.info(f"\nFile sizes:")
    logger.info(f"  {metadata_path.name}: {metadata_size:.1f} KB")
    logger.info(f"  {data_path.name}: {data_size:.1f} MB")

    logger.info("\n" + "=" * 60)
    logger.info("Archive data generation complete!")
    logger.info("=" * 60)

    generator.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate archive CML data for database initialization."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=int(os.getenv("ARCHIVE_DAYS", str(DEFAULT_ARCHIVE_DAYS))),
        help=f"Number of days of archive data to generate (default: {DEFAULT_ARCHIVE_DAYS}, or ARCHIVE_DAYS env var)",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(
            os.getenv("ARCHIVE_INTERVAL_SECONDS", str(DEFAULT_INTERVAL_SECONDS))
        ),
        help=f"Time resolution in seconds between archive data points (default: {DEFAULT_INTERVAL_SECONDS}, or ARCHIVE_INTERVAL_SECONDS env var)",
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv("ARCHIVE_OUTPUT_DIR", DEFAULT_OUTPUT_DIR),
        help="Output directory for archive files (default: ../database/archive_data, or ARCHIVE_OUTPUT_DIR env var)",
    )
    parser.add_argument(
        "--netcdf-file",
        default=os.getenv("NETCDF_FILE", DEFAULT_NETCDF_FILE),
        help="Path to the NetCDF source file (default: ../parser/example_data/..., or NETCDF_FILE env var)",
    )
    parser.add_argument(
        "--netcdf-file-url",
        default=os.getenv("NETCDF_FILE_URL", DEFAULT_NETCDF_FILE_URL),
        help="URL to download the NetCDF file if it is not already present (or NETCDF_FILE_URL env var)",
    )
    args = parser.parse_args()

    generate_archive_data(
        archive_days=args.days,
        output_dir=args.output_dir,
        netcdf_file=args.netcdf_file,
        interval_seconds=args.interval_seconds,
        netcdf_file_url=args.netcdf_file_url,
    )
