"""
Main orchestration script for the MNO Data Source Simulator.

This script coordinates the data generation and SFTP upload processes.
"""

import time
import logging
import os
import sys
from pathlib import Path
import yaml

from data_generator import CMLDataGenerator
from sftp_uploader import SFTPUploader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yml") -> dict:
    """
    Load configuration from YAML file.

    Parameters
    ----------
    config_path : str
        Path to the configuration file.

    Returns
    -------
    dict
        Configuration dictionary.
    """
    logger.info(f"Loading configuration from: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logger.info("Configuration loaded successfully")
    return config


def main():
    """Main execution loop."""
    logger.info("Starting MNO Data Source Simulator")

    # Load configuration
    config = load_config()

    # Initialize data generator
    generator = CMLDataGenerator(
        netcdf_file=config["data_source"]["netcdf_file"],
        loop_duration_seconds=config["data_source"]["loop_duration_seconds"],
        output_dir=config["generator"]["output_dir"],
    )

    # Initialize SFTP uploader if enabled
    sftp_uploader = None
    if config["sftp"]["enabled"]:
        # Get SFTP password from environment variable
        sftp_password = os.getenv("SFTP_PASSWORD")
        if not sftp_password:
            logger.warning(
                "SFTP_PASSWORD environment variable not set. SFTP upload disabled."
            )
        else:
            try:
                sftp_uploader = SFTPUploader(
                    host=config["sftp"]["host"],
                    port=config["sftp"]["port"],
                    username=config["sftp"]["username"],
                    password=sftp_password,
                    remote_path=config["sftp"]["remote_path"],
                    source_dir=config["file_management"]["source_dir"],
                    archive_dir=config["file_management"]["archive_dir"],
                )
                sftp_uploader.connect()
                logger.info("SFTP uploader initialized")
            except Exception as e:
                logger.error(f"Failed to initialize SFTP uploader: {e}")
                logger.info("Continuing without SFTP upload")
                sftp_uploader = None

    # Get upload frequency
    upload_frequency = config["sftp"]["upload_frequency_seconds"]
    last_upload_time = time.time()

    try:
        logger.info("Entering main loop")

        while True:
            try:
                # Generate data and write to CSV file
                csv_file = generator.generate_data_and_write_csv()
                logger.info(f"Generated CSV file: {csv_file}")

                # Check if it's time to upload
                current_time = time.time()
                if (
                    sftp_uploader
                    and current_time - last_upload_time >= upload_frequency
                ):
                    try:
                        # Upload all pending files
                        uploaded_count = sftp_uploader.upload_pending_files()
                        if uploaded_count > 0:
                            logger.info(f"Uploaded {uploaded_count} files")
                            last_upload_time = current_time
                    except Exception as e:
                        logger.error(f"Upload failed: {e}")

                # Wait until next generation cycle
                sleep_time = config["generator"]["generation_frequency_seconds"]
                logger.debug(f"Sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                raise
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                logger.info("Continuing after error...")
                time.sleep(10)  # Wait before retrying

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully")
    finally:
        # Cleanup
        logger.info("Cleaning up resources")
        generator.close()
        if sftp_uploader:
            sftp_uploader.close()

    logger.info("MNO Data Source Simulator stopped")


if __name__ == "__main__":
    main()
