"""
Main entry point for rain rate processing service.
Implements continuous polling loop for processing CML data.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from config_loader import RainProcessingConfig
from state_manager import StateManager
from data_interface import CMLDataInterface
from dataset_builder import build_cml_dataset, flatten_rain_dataset
from registry import WorkflowRegistry

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
    database_url = os.getenv("DATABASE_URL")
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
            reload_interval = global_config.get("config_reload_interval_seconds", 60)
            if config.should_reload(reload_interval):
                logger.info("Reloading configuration")
                config.load()

            # Process enabled users
            enabled_users = config.get_enabled_users()
            logger.debug(f"Enabled users: {list(enabled_users.keys())}")

            for user_id, user_config in enabled_users.items():
                try:
                    process_user_if_ready(
                        user_id, user_config, state_manager, data_interface
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
    data_interface: CMLDataInterface,
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
    now = datetime.now(timezone.utc)
    poll_interval_seconds = user_config.get("poll_interval_seconds", 900)
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
    window_minutes = user_config.get("data_window_minutes", 90)
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

    cml_ids = raw_rows["cml_id"].unique().tolist()
    metadata_rows = data_interface.fetch_cml_metadata_rows(user_id, cml_ids)

    # Build xarray dataset
    cml_ds = build_cml_dataset(raw_rows, metadata_rows)

    logger.info(
        f"User {user_id}: fetched {len(raw_rows)} raw data points, "
        f"{len(metadata_rows)} metadata records"
    )

    # Run workflow
    variant_name = user_config.get("processing_variant", "default")
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
