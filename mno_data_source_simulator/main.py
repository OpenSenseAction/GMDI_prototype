"""
Main orchestration script for the MNO Data Source Simulator.

This script coordinates the data generation and SFTP upload processes.
"""

import time
import logging
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
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
        # Get SFTP authentication credentials - require exactly one method
        sftp_password = os.getenv("SFTP_PASSWORD")
        private_key_path = config["sftp"].get("private_key_path")

        # Validate authentication configuration: exactly one method required
        if sftp_password and private_key_path:
            logger.error(
                "Multiple SFTP authentication methods configured. "
                "Use either SFTP_PASSWORD environment variable OR private_key_path in config.yml, not both. "
                "SFTP upload disabled."
            )
        elif not sftp_password and not private_key_path:
            logger.warning(
                "No SFTP authentication method configured. "
                "Set SFTP_PASSWORD environment variable OR configure private_key_path in config.yml. "
                "SFTP upload disabled."
            )
        else:
            try:
                # Expand user paths if present
                if private_key_path:
                    private_key_path = os.path.expanduser(private_key_path)

                known_hosts = config["sftp"].get("known_hosts_path")
                if known_hosts:
                    known_hosts = os.path.expanduser(known_hosts)

                sftp_uploader = SFTPUploader(
                    host=config["sftp"]["host"],
                    port=config["sftp"]["port"],
                    username=config["sftp"]["username"],
                    password=sftp_password,
                    private_key_path=private_key_path,
                    known_hosts_path=known_hosts,
                    remote_path=config["sftp"]["remote_path"],
                    source_dir=config["file_management"]["source_dir"],
                    archive_dir=config["file_management"]["archive_dir"],
                    connection_timeout=config["sftp"].get("connection_timeout", 30),
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

    # Get generation configuration
    timestamps_per_file = config["generator"].get("timestamps_per_file", 1)
    time_resolution_seconds = config["generator"].get("time_resolution_seconds", 60)

    # Generate metadata file at startup (metadata is static)
    try:
        metadata_file = generator.write_metadata_csv()
        logger.info(f"Generated metadata file: {metadata_file}")

        # If SFTP uploader is available, upload the metadata file immediately
        if sftp_uploader:
            try:
                uploaded_count = sftp_uploader.upload_pending_files()
                if uploaded_count > 0:
                    logger.info(f"Uploaded {uploaded_count} file(s) including metadata")
                    last_upload_time = time.time()
            except Exception as e:
                logger.error(f"Failed to upload initial metadata: {e}")
    except Exception as e:
        logger.error(f"Failed to generate metadata file: {e}")

    try:
        logger.info("Entering main loop")

        while True:
            try:
                # Generate timestamps for this cycle
                current_time = datetime.now()
                if timestamps_per_file > 1:
                    # Generate multiple timestamps with specified resolution
                    timestamps = [
                        current_time + timedelta(seconds=i * time_resolution_seconds)
                        for i in range(timestamps_per_file)
                    ]
                else:
                    timestamps = None  # Will use current time

                # Generate data and write to CSV file
                csv_file = generator.generate_data_and_write_csv(timestamps=timestamps)
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
