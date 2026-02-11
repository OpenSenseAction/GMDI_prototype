#!/usr/bin/env python3
"""
Generate archive CML data for database initialization.

This script uses the existing CMLDataGenerator to create archive data
with real RSL/TSL values from the NetCDF file, but with fake timestamps
spanning the configured archive period.
"""

import sys
import gzip
from pathlib import Path
from datetime import datetime, timedelta
import logging
import pandas as pd

from data_generator import CMLDataGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration
NETCDF_FILE = "../parser/example_data/openMRG_cmls_20150827_12hours.nc"
ARCHIVE_DAYS = 7  # Archive period in days (reduced for demo purposes)
TIME_INTERVAL_MINUTES = 5  # Resample to 5-minute intervals (reduces data size)
ARCHIVE_END_DATE = datetime.now()
ARCHIVE_START_DATE = ARCHIVE_END_DATE - timedelta(days=ARCHIVE_DAYS)
OUTPUT_DIR = "../database/archive_data"

# Output files (gzipped)
METADATA_OUTPUT = "metadata_archive.csv.gz"
DATA_OUTPUT = "data_archive.csv.gz"


def generate_archive_data():
    """Generate archive metadata and time-series data."""

    netcdf_path = Path(__file__).parent / NETCDF_FILE
    output_path = Path(__file__).parent / OUTPUT_DIR

    if not netcdf_path.exists():
        logger.error(f"NetCDF file not found: {netcdf_path}")
        sys.exit(1)

    output_path.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Generating Archive Data from NetCDF")
    logger.info("=" * 60)
    logger.info(f"NetCDF file: {netcdf_path}")
    logger.info(
        f"Archive period: {ARCHIVE_START_DATE} to {ARCHIVE_END_DATE} ({ARCHIVE_DAYS} days)"
    )

    # Initialize the data generator
    generator = CMLDataGenerator(
        netcdf_file=str(netcdf_path),
        loop_duration_seconds=ARCHIVE_DAYS * 24 * 3600,  # Loop over archive period
    )

    # Generate and save metadata using existing function
    logger.info("\nGenerating metadata...")
    metadata_path = output_path / METADATA_OUTPUT
    metadata_df = generator.get_metadata_dataframe()

    with gzip.open(metadata_path, "wt") as f:
        metadata_df.to_csv(f, index=False)

    logger.info(f"Saved {len(metadata_df)} metadata rows to {metadata_path}")
    logger.info(f"  Unique CML IDs: {metadata_df['cml_id'].nunique()}")

    # Generate timestamps for the archive period with configured interval
    logger.info(f"\nGenerating time-series data...")
    logger.info(f"  Time interval: {TIME_INTERVAL_MINUTES} minutes")

    timestamps = pd.date_range(
        start=ARCHIVE_START_DATE,
        end=ARCHIVE_END_DATE,
        freq=f"{TIME_INTERVAL_MINUTES}min",
    )

    logger.info(f"  Total timestamps: {len(timestamps):,}")
    logger.info(f"  Total rows (estimate): {len(timestamps) * len(metadata_df):,}")

    # Set the generator's loop start time to archive start
    generator.loop_start_time = ARCHIVE_START_DATE

    # Generate data in batches using existing generate_data function
    batch_size = 100
    total_rows = 0
    data_path = output_path / DATA_OUTPUT

    with gzip.open(data_path, "wt") as f:
        first_batch = True

        for i in range(0, len(timestamps), batch_size):
            batch_timestamps = timestamps[i : i + batch_size]

            # Use existing generate_data function
            df = generator.generate_data(batch_timestamps)

            # Write to gzipped CSV
            df.to_csv(f, index=False, header=first_batch)
            first_batch = False

            total_rows += len(df)

            # Progress indicator every 10%
            if (i + batch_size) % (len(timestamps) // 10) < batch_size:
                progress = min(100, ((i + batch_size) / len(timestamps)) * 100)
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
    generate_archive_data()
