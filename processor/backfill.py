"""
One-shot backfill script: process historical CML data in daily batches.

Usage:
    python backfill.py --user demo_openmrg --days 3 --variant openmrg_basic

Splits the requested number of past days into 1-day batches (oldest first),
runs the workflow for each batch, and writes results to the database.
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta, timezone

from data_interface import CMLDataInterface
from dataset_builder import build_cml_dataset, flatten_rain_dataset
from registry import WorkflowRegistry

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_batch(data_interface, user_id, variant_name, window_start, window_end):
    logger.info(
        f"Batch {window_start.strftime('%Y-%m-%d %H:%M')} → "
        f"{window_end.strftime('%Y-%m-%d %H:%M')}"
    )

    raw_data = data_interface.fetch_raw_cml_data_rows(user_id, window_start, window_end)
    if raw_data.empty:
        logger.warning("No raw data found for this window, skipping.")
        return 0

    metadata = data_interface.fetch_cml_metadata_rows(user_id)
    logger.info(
        f"Fetched {len(raw_data)} raw data points, {len(metadata)} metadata records"
    )

    cml_ds = build_cml_dataset(raw_data, metadata)
    workflow = WorkflowRegistry.get_workflow(variant_name)
    rain_ds = workflow.process(cml_ds, window_start, window_end)
    rain_data = flatten_rain_dataset(rain_ds)

    if rain_data.empty:
        logger.warning("Workflow produced no output for this window.")
        return 0

    logger.info(f"Workflow produced {len(rain_data)} results")
    rows_written = data_interface.write_rain_data(rain_data)
    logger.info(f"Wrote {rows_written} rows to database")
    return rows_written


def main():
    parser = argparse.ArgumentParser(
        description="Backfill rain rate data in daily batches"
    )
    parser.add_argument("--user", default="demo_openmrg", help="User ID to process")
    parser.add_argument(
        "--days", type=int, default=3, help="Number of past days to backfill"
    )
    parser.add_argument(
        "--batch-hours", type=int, default=6, help="Hours per batch (default 6)"
    )
    parser.add_argument("--variant", default="openmrg_basic", help="Workflow variant")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)

    data_interface = CMLDataInterface(database_url)
    now = datetime.now(timezone.utc)

    # Build list of (start, end) windows in batch_hours chunks, oldest first
    batch_td = timedelta(hours=args.batch_hours)
    backfill_start = now - timedelta(days=args.days)
    batches = []
    t = backfill_start
    while t < now:
        batch_end = min(t + batch_td, now)
        batches.append((t, batch_end))
        t = batch_end

    logger.info(f"Starting backfill: {args.days} batches for user '{args.user}'")
    total_rows = 0

    for idx, (batch_start, batch_end) in enumerate(batches, 1):
        logger.info(f"=== Batch {idx}/{len(batches)} ===")
        try:
            rows = run_batch(
                data_interface, args.user, args.variant, batch_start, batch_end
            )
            total_rows += rows
        except Exception as e:
            logger.error(f"Batch {idx} failed: {e}", exc_info=True)
            logger.info("Continuing with next batch...")

    logger.info(f"Backfill complete. Total rows written: {total_rows}")


if __name__ == "__main__":
    main()
