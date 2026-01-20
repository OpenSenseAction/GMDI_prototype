"""
Data generator module for the MNO simulator.

This module reads CML data from a NetCDF file and generates fake real-time data
by altering timestamps and looping through the existing data.
"""

import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class CMLDataGenerator:
    """Generate fake real-time CML data from historical NetCDF files."""

    def __init__(
        self,
        netcdf_file: str,
        loop_duration_seconds: int = 3600,
        output_dir: str = "data_to_upload",
    ):
        """
        Initialize the CML data generator.

        Parameters
        ----------
        netcdf_file : str
            Path to the NetCDF file containing CML data.
        loop_duration_seconds : int, optional
            Duration of one loop cycle in seconds (default: 3600 = 1 hour).
        output_dir : str, optional
            Directory where CSV files will be written (default: "data_to_upload").
        """
        self.netcdf_file = netcdf_file
        self.loop_duration_seconds = loop_duration_seconds
        self.output_dir = Path(output_dir)
        self.dataset = None
        self.original_time_points = None
        self.time_delta = None
        self.loop_start_time = None

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {self.output_dir}")

        self._load_dataset()

    def _load_dataset(self):
        """Load the NetCDF dataset and extract time information."""
        logger.info(f"Loading NetCDF file: {self.netcdf_file}")
        self.dataset = xr.open_dataset(self.netcdf_file)

        # Get original time points
        self.original_time_points = pd.to_datetime(self.dataset.time.values)

        # Calculate time delta between consecutive points
        if len(self.original_time_points) > 1:
            time_diffs = np.diff(self.original_time_points)
            self.time_delta = pd.Timedelta(time_diffs[0])
        else:
            # Default to 1 minute if only one time point
            self.time_delta = pd.Timedelta(minutes=1)

        # Set the loop start time to now
        self.loop_start_time = datetime.now()

        logger.info(
            f"Dataset loaded with {len(self.original_time_points)} time points, "
            f"time delta: {self.time_delta}"
        )

    def get_current_data_point(self) -> dict:
        """
        Get the current data point with adjusted timestamp.

        Returns
        -------
        dict
            Dictionary containing:
            - timestamp: Current timestamp
            - data: xarray.Dataset with CML data for this timestamp
            - metadata: Metadata about CMls (coordinates, etc.)
        """
        current_time = datetime.now()

        # Calculate elapsed time since loop start
        elapsed = (current_time - self.loop_start_time).total_seconds()

        # Calculate position within the loop
        loop_position = elapsed % self.loop_duration_seconds

        # Map loop position to original data index
        # Scale loop position to the range of available data points
        original_duration = (
            self.original_time_points[-1] - self.original_time_points[0]
        ).total_seconds()

        if original_duration > 0:
            # Calculate which original time index to use
            time_fraction = loop_position / self.loop_duration_seconds
            original_index = int(time_fraction * (len(self.original_time_points) - 1))
        else:
            original_index = 0

        # Get data for this time index
        data_slice = self.dataset.isel(time=original_index)

        # Extract metadata (time-independent data)
        metadata = self.dataset.drop_vars(self.dataset.data_vars).drop_dims("time")

        logger.debug(
            f"Generated data point at index {original_index}/{len(self.original_time_points)-1}"
        )

        return {
            "timestamp": current_time,
            "data": data_slice,
            "metadata": metadata,
        }

    def get_data_as_dataframe(self, data_point: dict) -> pd.DataFrame:
        """
        Convert a data point to a pandas DataFrame.

        Parameters
        ----------
        data_point : dict
            Data point dictionary from get_current_data_point()

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: time, cml_id, sublink_id, tsl, rsl
        """
        # Convert to DataFrame
        df = data_point["data"].to_dataframe()

        # Add timestamp
        df["time"] = data_point["timestamp"]

        # Reset index to get cml_id and sublink_id as columns
        df = df.reset_index()

        # Select relevant columns
        if "tsl" in df.columns and "rsl" in df.columns:
            df = df[["time", "cml_id", "sublink_id", "tsl", "rsl"]]
        else:
            logger.warning("Expected columns 'tsl' and 'rsl' not found in dataset")

        return df

    def get_metadata_as_dataframe(self, data_point: dict) -> pd.DataFrame:
        """
        Convert metadata to a pandas DataFrame.

        Parameters
        ----------
        data_point : dict
            Data point dictionary from get_current_data_point()

        Returns
        -------
        pd.DataFrame
            DataFrame with CML metadata
        """
        return data_point["metadata"].to_dataframe()

    def generate_and_write_csv(self) -> str:
        """
        Generate current data point and write it to a CSV file.

        Returns
        -------
        str
            Path to the generated CSV file.
        """
        # Generate current data point
        data_point = self.get_current_data_point()

        # Convert to DataFrame
        df = self.get_data_as_dataframe(data_point)

        # Generate filename with timestamp
        timestamp = data_point["timestamp"].strftime("%Y%m%d_%H%M%S")
        filename = f"cml_data_{timestamp}.csv"
        filepath = self.output_dir / filename

        # Write to CSV
        df.to_csv(filepath, index=False)
        logger.info(f"Generated CSV file: {filepath} ({len(df)} rows)")

        return str(filepath)

    def close(self):
        """Close the dataset."""
        if self.dataset is not None:
            self.dataset.close()
            logger.info("Dataset closed")
