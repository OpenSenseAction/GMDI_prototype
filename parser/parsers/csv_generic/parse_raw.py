"""Configurable generic CSV rawdata parser."""

import pandas as pd
from pathlib import Path
from typing import Optional


def parse_rawdata_csv(filepath: Path, config: dict) -> Optional[pd.DataFrame]:
    """Parse a raw CML time-series CSV with format driven by *config*.

    Recognised config keys (all optional):

    read_csv_kwargs
        Dict of kwargs forwarded verbatim to ``pd.read_csv`` (e.g. ``sep``,
        ``encoding``, ``decimal``, ``skiprows``).

    rawdata_columns
        ``{source_column_name: canonical_column_name}`` rename map applied
        before any further processing.  Canonical names are: ``time``,
        ``cml_id``, ``sublink_id``, ``tsl``, ``rsl``.

    timezone
        IANA timezone name (e.g. ``"Africa/Douala"``).  Naive timestamps are
        localised to this zone and then converted to UTC.  Already-aware
        timestamps are just converted to UTC.  Omit to leave timestamps as-is.
    """
    read_kwargs = config.get("read_csv_kwargs") or {}
    df = pd.read_csv(filepath, **read_kwargs)

    col_map = config.get("rawdata_columns") or {}
    if col_map:
        df = df.rename(columns=col_map)

    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    tz = config.get("timezone")
    if tz:
        if df["time"].dt.tz is None:
            df["time"] = df["time"].dt.tz_localize(tz).dt.tz_convert("UTC")
        else:
            df["time"] = df["time"].dt.tz_convert("UTC")

    df["cml_id"] = df["cml_id"].fillna("nan").astype(str)
    df["sublink_id"] = df["sublink_id"].astype(str)
    df["tsl"] = pd.to_numeric(df["tsl"], errors="coerce")
    df["rsl"] = pd.to_numeric(df["rsl"], errors="coerce")
    return df
