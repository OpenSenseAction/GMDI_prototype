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

from data_generator import CMLDataGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Defaults (overridable via CLI args or environment variables)
DEFAULT_NETCDF_FILE = "../parser/example_data/openMRG_cmls_20150827_12hours.nc"
DEFAULT_OUTPUT_DIR = "../database/archive_data"
DEFAULT_ARCHIVE_DAYS = 7
DEFAULT_INTERVAL_SECONDS = 300  # 5-minute default; use 10 for raw real-time resolution

# Output files
METADATA_OUTPUT = "metadata_archive.csv"
DATA_OUTPUT = "data_archive.csv"


def generate_archive_data(archive_days, output_dir, netcdf_file, interval_seconds):
    """Generate archive metadata and time-series data."""

    netcdf_path = Path(netcdf_file)
    if not netcdf_path.is_absolute():
        netcdf_path = Path(__file__).parent / netcdf_file

    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = Path(__file__).parent / output_dir

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
        loop_duration_seconds=archive_days * 24 * 3600,  # Loop over archive period
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
    # Map each archive timestamp to a NetCDF index (cycles through 720 steps)
    all_indices = np.array(
        [generator._get_netcdf_index_for_timestamp(ts) for ts in timestamps]
    )
    unique_indices = np.unique(all_indices)
    logger.info(
        f"  Unique NetCDF time slices needed: {len(unique_indices)} "
        f"(of {len(generator.original_time_points)} in file)"
    )

    # Pre-cache RSL/TSL arrays for each unique NetCDF index (one isel call each)
    logger.info("  Pre-caching NetCDF slices...")
    base_df = (
        generator.dataset.isel(time=int(unique_indices[0]))
        .to_dataframe()
        .reset_index()[["cml_id", "sublink_id", "tsl", "rsl"]]
    )
    cml_ids = base_df["cml_id"].values
    sublink_ids = base_df["sublink_id"].values
    n_links = len(cml_ids)

    rsl_cache = {}
    tsl_cache = {}
    for idx in unique_indices:
        df_slice = (
            generator.dataset.isel(time=int(idx)).to_dataframe().reset_index()
        )
        rsl_cache[idx] = df_slice["rsl"].values
        tsl_cache[idx] = df_slice["tsl"].values
    logger.info(f"  Cached {len(unique_indices)} slices, generating output...")

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
            tsl_col = np.concatenate([tsl_cache[idx] for idx in batch_indices])
            rsl_col = np.concatenate([rsl_cache[idx] for idx in batch_indices])

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
        default=int(os.getenv("ARCHIVE_INTERVAL_SECONDS", str(DEFAULT_INTERVAL_SECONDS))),
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
    args = parser.parse_args()

    generate_archive_data(
        archive_days=args.days,
        output_dir=args.output_dir,
        netcdf_file=args.netcdf_file,
        interval_seconds=args.interval_seconds,
    )
