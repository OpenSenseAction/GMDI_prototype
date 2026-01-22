"""Parse raw CML time series CSV files."""

import pandas as pd
from pathlib import Path
from typing import Optional


def parse_rawdata_csv(filepath: Path) -> Optional[pd.DataFrame]:
    df = pd.read_csv(filepath)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["cml_id"] = df["cml_id"].fillna("nan").astype(str)
    df["sublink_id"] = df["sublink_id"].astype(str)
    df["tsl"] = pd.to_numeric(df["tsl"], errors="coerce")
    df["rsl"] = pd.to_numeric(df["rsl"], errors="coerce")
    return df
