"""Parse CML metadata CSV files."""

import pandas as pd
from pathlib import Path
from typing import Optional


def parse_metadata_csv(filepath: Path) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(filepath)
    except Exception:
        return None
    df["cml_id"] = df["cml_id"].astype(str)
    for col in ["site_0_lon", "site_0_lat", "site_1_lon", "site_1_lat"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
