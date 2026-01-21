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

    def _get_netcdf_index_for_timestamp(self, timestamp: pd.Timestamp) -> int:
        """
        Map a timestamp to the corresponding NetCDF data index.

        Parameters
        ----------
        timestamp : pd.Timestamp
            Timestamp to map.

        Returns
        -------
        int
            Index in the NetCDF dataset.
        """
        # Calculate elapsed time since loop start
        elapsed = (timestamp - self.loop_start_time).total_seconds()

        # Calculate position within the loop
        loop_position = elapsed % self.loop_duration_seconds

        # Map loop position to original data index
        original_duration = (
            self.original_time_points[-1] - self.original_time_points[0]
        ).total_seconds()

        if original_duration > 0:
            time_fraction = loop_position / self.loop_duration_seconds
            original_index = int(time_fraction * (len(self.original_time_points) - 1))
        else:
            original_index = 0

        return original_index

    def generate_data(
        self, timestamps: pd.DatetimeIndex | list | np.ndarray | None = None
    ) -> pd.DataFrame:
        """
        Generate fake CML data from NetCDF source.

        If no timestamps provided, generates data for current time.
        Otherwise, generates data for all specified timestamps.

        Parameters
        ----------
        timestamps : pd.DatetimeIndex, list, np.ndarray, or None
            Timestamps for which to generate data.
            If None, uses current time (default).

        Returns
        -------
        pd.DataFrame
            DataFrame with columns: time, cml_id, sublink_id, tsl, rsl
        """
        # Handle default: current timestamp
        if timestamps is None:
            timestamps = [pd.Timestamp(datetime.now())]
        elif not isinstance(timestamps, pd.DatetimeIndex):
            timestamps = pd.DatetimeIndex(timestamps)

        all_data = []

        for ts in timestamps:
            # Get NetCDF index for this timestamp
            original_index = self._get_netcdf_index_for_timestamp(ts)

            # Get data for this time index
            data_slice = self.dataset.isel(time=original_index)

            # Convert to DataFrame
            df = data_slice.to_dataframe().reset_index()
            df["time"] = ts

            # Select relevant columns
            if "tsl" in df.columns and "rsl" in df.columns:
                df = df[["time", "cml_id", "sublink_id", "tsl", "rsl"]]

            all_data.append(df)

        # Combine all data
        result = pd.concat(all_data, ignore_index=True)

        logger.debug(
            f"Generated data for {len(timestamps)} timestamp(s) ({len(result)} rows)"
        )

        return result

    def get_metadata_dataframe(self) -> pd.DataFrame:
        """
        Get CML metadata as a pandas DataFrame.

        Extracts all metadata coordinates from the NetCDF dataset
        (excluding dimension coordinates like time, cml_id, sublink_id).

        Returns
        -------
        pd.DataFrame
            DataFrame with CML metadata.
        """
        # Identify metadata coordinates (non-dimension coordinates)
        dimension_coords = set(self.dataset.sizes.keys())
        all_coords = set(self.dataset.coords.keys())
        metadata_coord_names = list(all_coords - dimension_coords)

        # Extract metadata as DataFrame
        metadata_df = self.dataset[metadata_coord_names].to_dataframe()

        # Sort by index to ensure deterministic order across different systems
        metadata_df = metadata_df.sort_index()

        return metadata_df

    def generate_data_and_write_csv(
        self,
        timestamps=None,
        split_freq: str = None,
    ) -> list:
        """
        Generate data and write it to CSV file(s).

        Parameters
        ----------
        timestamps : array-like, optional
            Timestamps for which to generate data. If None, generates
            data for the current time.
        split_freq : str, optional
            Frequency for splitting data into separate files using pandas
            frequency strings (e.g., '1h', '1d', '1W'). If None, writes
            all data to a single file.

        Returns
        -------
        list
            List of paths to the generated CSV file(s).
        """
        # Generate data
        df = self.generate_data(timestamps)

        # Determine if we need to split by frequency
        if split_freq is None:
            # Write all data to a single file
            timestamp = df["time"].iloc[0]
            timestamp_str = pd.Timestamp(timestamp).strftime("%Y%m%d_%H%M%S")
            filename = f"cml_data_{timestamp_str}.csv"
            filepath = self.output_dir / filename

            df.to_csv(filepath, index=False)
            logger.info(f"Generated CSV file: {filepath} ({len(df)} rows)")

            return [str(filepath)]
        else:
            # Split data by frequency and write multiple files
            filepaths = []

            # Group by the specified frequency
            df["time_pd"] = pd.to_datetime(df["time"])
            grouped = df.groupby(pd.Grouper(key="time_pd", freq=split_freq))

            for period_start, group_df in grouped:
                if len(group_df) == 0:
                    continue

                # Remove the temporary time_pd column
                group_df = group_df.drop(columns=["time_pd"])

                # Generate filename based on period start
                timestamp_str = period_start.strftime("%Y%m%d_%H%M%S")
                filename = f"cml_data_{timestamp_str}.csv"
                filepath = self.output_dir / filename

                group_df.to_csv(filepath, index=False)
                logger.info(f"Generated CSV file: {filepath} ({len(group_df)} rows)")
                filepaths.append(str(filepath))

            return filepaths

    def write_metadata_csv(self, filepath: str = None) -> str:
        """
        Write CML metadata to a CSV file.

        Parameters
        ----------
        filepath : str, optional
            Full path to the output CSV file. If not provided, generates
            a filename with timestamp in the output directory.

        Returns
        -------
        str
            Path to the generated metadata CSV file.
        """
        # Get metadata as DataFrame
        metadata_df = self.get_metadata_dataframe()

        # Reset index to include cml_id and sublink_id as columns
        # This ensures the sorted order is preserved in the CSV
        metadata_df = metadata_df.reset_index()

        # Reorder columns: cml_id, sublink_id, site_0 (lon, lat), site_1 (lon, lat), frequency, polarization, length
        column_order = [
            "cml_id",
            "sublink_id",
            "site_0_lon",
            "site_0_lat",
            "site_1_lon",
            "site_1_lat",
            "frequency",
            "polarization",
            "length",
        ]
        # Only include columns that exist in the dataframe
        column_order = [col for col in column_order if col in metadata_df.columns]
        metadata_df = metadata_df[column_order]

        # Generate filepath if not provided
        if filepath is None:
            timestamp_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cml_metadata_{timestamp_str}.csv"
            filepath = self.output_dir / filename

        # Write to CSV
        metadata_df.to_csv(filepath, index=False)
        logger.info(
            f"Generated metadata CSV file: {filepath} ({len(metadata_df)} rows)"
        )

        return str(filepath)

    def close(self):
        """Close the dataset."""
        if self.dataset is not None:
            self.dataset.close()
            logger.info("Dataset closed")
