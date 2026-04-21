"""
Data generator module for the MNO simulator.

This module reads CML data from a NetCDF file and generates fake real-time data
by altering timestamps and looping through the existing data.
"""

import urllib.request
import urllib.error
import os
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def ensure_netcdf_file(path: Path, url: str | None) -> None:
    """Download the NetCDF file from *url* if *path* does not exist yet.

    Downloads via a temp file so an interrupted transfer never leaves a
    truncated file behind.  Does nothing if the file already exists or if no
    URL is provided.
    """
    if path.exists():
        logger.info(f"NetCDF file found: {path}")
        return
    if not url:
        return  # caller's existence check will log the error
    logger.info(f"NetCDF file not found at {path}")
    logger.info(f"Downloading from: {url}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".nc.download")
    try:
        with urllib.request.urlopen(url) as response, open(tmp_path, "wb") as out:
            total_raw = response.headers.get("Content-Length")
            total = int(total_raw) if total_raw else None
            downloaded = 0
            block_size = 8 * 1024 * 1024  # 8 MB chunks
            while True:
                block = response.read(block_size)
                if not block:
                    break
                out.write(block)
                downloaded += len(block)
                if total:
                    pct = downloaded / total * 100
                    logger.info(
                        f"  {pct:.0f}%  ({downloaded / 1e6:.0f} / {total / 1e6:.0f} MB)"
                    )
                else:
                    logger.info(f"  {downloaded / 1e6:.0f} MB downloaded")
        tmp_path.rename(path)
        logger.info(f"Download complete: {path} ({path.stat().st_size / 1e6:.1f} MB)")
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        logger.error(f"Download failed: {exc}")
        raise


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
        # Variable names to use for RSL/TSL (configurable for datasets that
        # provide rsl_min/tsl_min instead of rsl/tsl)
        self.rsl_var = os.getenv("NETCDF_RSL_VAR", "rsl")
        self.tsl_var = os.getenv("NETCDF_TSL_VAR", "tsl")

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
        logger.info(f"RSL variable: {self.rsl_var}, TSL variable: {self.tsl_var}")

        # Identify sublinks that have at least one valid (non-NaN) value across
        # all CMLs and time steps.  Fully-NaN sublinks are structural padding
        # in the NetCDF file and should not be emitted as data rows.
        rsl_data = self.dataset[self.rsl_var]  # (..., sublink_id, ...)
        # Compute a boolean mask: True where the sublink has ≥1 non-NaN value
        # across all other dimensions.
        sublink_dim = rsl_data.dims.index("sublink_id")
        other_axes = tuple(i for i in range(rsl_data.ndim) if i != sublink_dim)
        has_data = ~np.all(np.isnan(rsl_data.values), axis=other_axes)
        all_sublinks = self.dataset.sublink_id.values
        self.valid_sublinks = all_sublinks[has_data]
        n_dropped = len(all_sublinks) - len(self.valid_sublinks)
        if n_dropped:
            logger.info(
                f"Dropped {n_dropped} fully-NaN sublinks; "
                f"{len(self.valid_sublinks)} sublinks retained."
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
            # Cycle through the source data at its native pace rather than
            # stretching/compressing it to fill loop_duration_seconds.  This
            # avoids long plateaus of identical values followed by sudden jumps
            # when the archive period is much longer than the source file.
            position_in_original = loop_position % original_duration
            time_fraction = position_in_original / original_duration
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

            # Get data for this time index, filtered to valid sublinks only
            data_slice = self.dataset.sel(sublink_id=self.valid_sublinks).isel(time=original_index)

            # Convert to DataFrame
            df = data_slice.to_dataframe().reset_index()
            df["time"] = ts

            # Rename rsl_min/tsl_min → rsl/tsl so the CSV format stays consistent
            rename_map = {}
            if self.rsl_var != "rsl":
                rename_map[self.rsl_var] = "rsl"
            if self.tsl_var != "tsl":
                rename_map[self.tsl_var] = "tsl"
            if rename_map:
                df = df.rename(columns=rename_map)

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
        Get CML metadata as a pandas DataFrame, with one row per (cml_id, sublink_id).
        Only sublinks that contain at least one valid data value are included.
        Includes: cml_id, sublink_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat,
        frequency, polarization (if present, else None), length
        """
        # Filter to valid sublinks before extracting metadata
        ds = self.dataset.sel(sublink_id=self.valid_sublinks)

        coord_vars = ["site_0_lon", "site_0_lat", "site_1_lon", "site_1_lat",
                      "frequency", "length"]
        available = [v for v in coord_vars if v in ds.coords or v in ds.data_vars]
        df = ds[available].to_dataframe().reset_index()
        # Keep only (cml_id, sublink_id) index columns plus the coordinate columns
        keep = ["cml_id", "sublink_id"] + [v for v in available if v in df.columns]
        df = df[[c for c in keep if c in df.columns]]
        df = df.loc[:, ~df.columns.duplicated()]

        # Add polarization column (may not exist in all datasets)
        if "polarization" in ds.coords or "polarization" in ds.data_vars:
            pol_df = ds["polarization"].to_dataframe().reset_index()
            df = df.merge(pol_df[["cml_id", "sublink_id", "polarization"]],
                          on=["cml_id", "sublink_id"], how="left")
        else:
            df["polarization"] = None

        df = df.sort_values(["cml_id", "sublink_id"]).reset_index(drop=True)
        return df

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
        Write CML metadata to a CSV file, with one row per (cml_id, sublink_id).
        Database schema now expects one row per (cml_id, sublink_id) to preserve
        sublink-specific metadata like frequency and polarization.
        """
        metadata_df = self.get_metadata_dataframe()

        # Keep only the columns needed for the database
        db_columns = [
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
        # Filter to database columns (no deduplication needed)
        metadata_df = metadata_df[db_columns]

        # Generate filepath if not provided
        if filepath is None:
            timestamp_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cml_metadata_{timestamp_str}.csv"
            filepath = self.output_dir / filename
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
