"""Configurable generic CSV metadata parser."""

import pandas as pd
from pathlib import Path
from typing import Optional


def parse_metadata_csv(filepath: Path, config: dict) -> Optional[pd.DataFrame]:
    """Parse a CML metadata CSV with format driven by *config*.

    Recognised config keys (all optional):

    read_csv_kwargs
        Dict of kwargs forwarded verbatim to ``pd.read_csv``.

    metadata_columns
        ``{source_column_name: canonical_column_name}`` rename map applied
        before any further processing.  Canonical names are: ``cml_id``,
        ``sublink_id``, ``site_0_lon``, ``site_0_lat``, ``site_1_lon``,
        ``site_1_lat``, ``frequency``, ``polarization``, ``length``.
    """
    read_kwargs = config.get("read_csv_kwargs") or {}
    df = pd.read_csv(filepath, **read_kwargs)

    col_map = config.get("metadata_columns") or {}
    if col_map:
        df = df.rename(columns=col_map)

    df["cml_id"] = df["cml_id"].astype(str)
    for col in ["site_0_lon", "site_0_lat", "site_1_lon", "site_1_lat"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
