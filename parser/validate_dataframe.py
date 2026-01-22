"""Validation utilities for parsed DataFrames."""

import pandas as pd
from typing import Literal


def validate_dataframe(df: pd.DataFrame, kind: Literal["rawdata", "metadata"]) -> bool:
    if df is None or df.empty:
        return False
    if kind == "rawdata":
        required = ["time", "cml_id", "sublink_id", "tsl", "rsl"]
        for col in required:
            if col not in df.columns:
                return False
        if df["time"].isna().any():
            return False
    elif kind == "metadata":
        required = ["cml_id", "site_0_lon", "site_0_lat", "site_1_lon", "site_1_lat"]
        for col in required:
            if col not in df.columns:
                return False
        # Check coordinate ranges
        if (
            df["site_0_lon"].notna().any()
            and not df["site_0_lon"].between(-180, 180).all()
        ):
            return False
        if (
            df["site_1_lon"].notna().any()
            and not df["site_1_lon"].between(-180, 180).all()
        ):
            return False
        if (
            df["site_0_lat"].notna().any()
            and not df["site_0_lat"].between(-90, 90).all()
        ):
            return False
        if (
            df["site_1_lat"].notna().any()
            and not df["site_1_lat"].between(-90, 90).all()
        ):
            return False
    else:
        return False
    return True
