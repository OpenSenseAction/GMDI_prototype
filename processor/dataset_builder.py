"""
Dataset builder for CML data.
Converts SQL query results into canonical xarray.Dataset structure for workflows.
"""

import pandas as pd
import xarray as xr
import numpy as np
from datetime import datetime, timezone
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def build_cml_dataset(
    raw_rows: pd.DataFrame,
    metadata_rows: pd.DataFrame,
    round_to_seconds: Optional[int] = None,
) -> xr.Dataset:
    """
    Build the canonical xarray dataset used by all workflows.

    Uses xr.Dataset.from_dataframe() for memory efficiency with large datasets.
    This avoids creating a dense 3D grid and instead keeps data in sparse format.

    Args:
        raw_rows: DataFrame with columns [time, cml_id, sublink_id, user_id, rsl, tsl]
        metadata_rows: DataFrame with columns [cml_id, sublink_id, site_0_lon, site_0_lat,
                       site_1_lon, site_1_lat, frequency, polarization, length]
        round_to_seconds: Round timestamps to this interval to reduce memory usage from
                         irregular sampling. Useful when CMLs have slightly different
                         observation times. Set to None to keep original timestamps.
                         Examples: 60 for 1-min data, 900 for 15-min data, None for irregular.

    Returns:
        xr.Dataset with dimensions including time and link identifiers
        and variables [rsl, tsl, frequency, polarization, length, ...]
    """
    # Validate required columns in raw data
    required_raw_cols = ["time", "cml_id", "sublink_id", "user_id", "rsl", "tsl"]
    missing_raw = [col for col in required_raw_cols if col not in raw_rows.columns]
    if missing_raw:
        raise ValueError(f"Missing required columns in raw data: {missing_raw}")

    if raw_rows.empty:
        logger.warning("Empty raw data provided, returning empty dataset")
        return xr.Dataset()

    # Ensure time is timezone-aware and sorted
    raw_rows = raw_rows.copy()
    if raw_rows["time"].dt.tz is None:
        raw_rows["time"] = raw_rows["time"].dt.tz_localize("UTC")
    else:
        raw_rows["time"] = raw_rows["time"].dt.tz_convert("UTC")

    # Round timestamps to reduce memory footprint from irregular sampling
    # This is especially important when CMLs have slightly different observation times
    if round_to_seconds is not None:
        original_count = len(raw_rows)
        raw_rows["time"] = raw_rows["time"].dt.round(f"{round_to_seconds}s")
        # Remove duplicates that may result from rounding
        raw_rows = raw_rows.drop_duplicates(subset=["time", "cml_id", "sublink_id"])
        rounded_count = len(raw_rows)
        if original_count != rounded_count:
            logger.info(
                f"Rounded {original_count:,} timestamps to {round_to_seconds}s intervals, "
                f"reduced to {rounded_count:,} rows (removed {original_count - rounded_count:,} duplicates)"
            )

    raw_rows = raw_rows.sort_values("time")

    # Get unique user_id (should be constant for this dataset)
    user_ids = raw_rows["user_id"].unique()
    if len(user_ids) > 1:
        logger.warning(f"Multiple user_ids in dataset: {user_ids}, using first")
    user_id = user_ids[0]

    # Create MultiIndex from relevant columns
    raw_rows_indexed = raw_rows.set_index(["time", "cml_id", "sublink_id"])

    # Convert to xarray using from_dataframe - much more memory efficient!
    # This preserves the sparse structure instead of creating a dense 3D grid
    ds = xr.Dataset.from_dataframe(raw_rows_indexed)

    # Add metadata as coordinates following poligrain/OpenMRG convention
    # See: https://poligrain.readthedocs.io/en/latest/notebooks/Explore_example_data.html
    if not metadata_rows.empty:
        metadata_indexed = metadata_rows.set_index(["cml_id", "sublink_id"])

        # Get unique coordinate values
        cml_ids_meta = sorted(
            metadata_indexed.index.get_level_values("cml_id").unique().tolist()
        )
        sublink_ids_meta = sorted(
            metadata_indexed.index.get_level_values("sublink_id").unique().tolist()
        )

        # Add site coordinates (indexed by cml_id only)
        for col in ["site_0_lon", "site_0_lat", "site_1_lon", "site_1_lat"]:
            if col in metadata_indexed.columns:
                # Create mapping from cml_id to value (average across sublinks if needed)
                site_values = {}
                for cml_id in cml_ids_meta:
                    cml_data = metadata_indexed.xs(
                        cml_id, level="cml_id", drop_level=False
                    )
                    if len(cml_data) > 0:
                        site_values[cml_id] = cml_data[
                            col
                        ].mean()  # Average if multiple sublinks
                    else:
                        site_values[cml_id] = np.nan

                ds.coords[col] = (
                    "cml_id",
                    [site_values.get(cid, np.nan) for cid in cml_ids_meta],
                )

        # Add length coordinate (indexed by cml_id only)
        if "length" in metadata_indexed.columns:
            length_values = {}
            for cml_id in cml_ids_meta:
                cml_data = metadata_indexed.xs(cml_id, level="cml_id", drop_level=False)
                if len(cml_data) > 0:
                    length_values[cml_id] = cml_data["length"].mean()
                else:
                    length_values[cml_id] = np.nan

            ds.coords["length"] = (
                "cml_id",
                [length_values.get(cid, np.nan) for cid in cml_ids_meta],
            )

        # Add frequency and polarization (indexed by both sublink_id and cml_id)
        for col in ["frequency", "polarization"]:
            if col in metadata_indexed.columns:
                # Create 2D array with dimensions (sublink_id, cml_id)
                meta_array = np.full(
                    (len(sublink_ids_meta), len(cml_ids_meta)),
                    np.nan if col == "frequency" else None,
                    dtype=object,
                )

                for (cml_id, sublink_id), row in metadata_indexed.iterrows():
                    try:
                        i = sublink_ids_meta.index(sublink_id)
                        j = cml_ids_meta.index(cml_id)
                        val = row[col]
                        if col == "frequency":
                            val = float(val) if pd.notna(val) else np.nan
                        meta_array[i, j] = val
                    except (ValueError, IndexError):
                        pass

                ds.coords[col] = (["sublink_id", "cml_id"], meta_array)

    # Store user_id in attributes
    ds.attrs["user_id"] = user_id

    logger.info(f"Built xarray dataset from {len(raw_rows)} rows")
    logger.debug(f"Dataset dimensions: {dict(ds.dims)}")
    logger.debug(f"Dataset variables: {list(ds.data_vars)}")
    logger.debug(f"Dataset coordinates: {list(ds.coords)}")

    return ds


def flatten_rain_dataset(rain_ds: xr.Dataset) -> pd.DataFrame:
    """
    Convert processed xarray dataset back to tabular rows for DB writing.

    Expected variables in rain_ds:
        tl, wet, baseline, waa, a_rain, r

    Args:
        rain_ds: Processed xarray dataset with dimensions [time, cml_id, sublink_id]

    Returns:
        DataFrame with columns: time, cml_id, sublink_id, user_id,
                               tl, wet, baseline, waa, a_rain, r
    """
    # Validate required variables
    required_vars = ["tl", "wet", "baseline", "waa", "a_rain", "r"]
    missing_vars = [var for var in required_vars if var not in rain_ds.variables]

    if missing_vars:
        logger.warning(f"Missing variables in rain dataset: {missing_vars}")

    if rain_ds.dims.get("time", 0) == 0:
        logger.debug("Empty rain dataset, returning empty DataFrame")
        return pd.DataFrame(
            columns=[
                "time",
                "cml_id",
                "sublink_id",
                "user_id",
                "tl",
                "wet",
                "baseline",
                "waa",
                "a_rain",
                "r",
            ]
        )

    # Convert to DataFrame
    df = rain_ds.to_dataframe()

    # Reset index to get coordinates as columns
    df = df.reset_index()

    # Filter out rows where all output variables are NaN
    output_cols = [col for col in required_vars if col in df.columns]
    if output_cols:
        df = df.dropna(subset=output_cols, how="all")

    # Add user_id from attrs if not already present
    if "user_id" not in df.columns:
        df["user_id"] = rain_ds.attrs.get("user_id", "")

    # Ensure proper column order
    column_order = [
        "time",
        "cml_id",
        "sublink_id",
        "user_id",
        "tl",
        "wet",
        "baseline",
        "waa",
        "a_rain",
        "r",
    ]

    # Only include columns that exist
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]

    # Ensure time is timezone-aware
    if df["time"].dt.tz is None:
        df["time"] = df["time"].dt.tz_localize("UTC")

    logger.debug(f"Flattened rain dataset to {len(df)} rows")

    return df
