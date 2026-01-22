"""CSV parser for raw CML time series data."""

from pathlib import Path
import re
from typing import Optional, Tuple
import pandas as pd

from .base_parser import BaseParser


class CSVRawDataParser(BaseParser):
    REQUIRED_COLUMNS = ["time", "cml_id", "sublink_id", "tsl", "rsl"]
    FILE_PATTERN = re.compile(r"^cml_data_.*\.csv$", re.IGNORECASE)

    def can_parse(self, filepath: Path) -> bool:
        return bool(self.FILE_PATTERN.match(filepath.name))

    def parse(self, filepath: Path) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            return None, f"Failed to read CSV: {e}"

        # Validate columns
        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            return None, f"Missing required columns: {missing}"

        try:
            df["time"] = pd.to_datetime(df["time"], errors="coerce")
            # Preserve rows even when cml_id is missing — convert NaN -> literal 'nan'
            df["cml_id"] = df["cml_id"].fillna("nan").astype(str)
            df["sublink_id"] = df["sublink_id"].astype(str)
            df["tsl"] = pd.to_numeric(df["tsl"], errors="coerce")
            df["rsl"] = pd.to_numeric(df["rsl"], errors="coerce")
        except Exception as e:
            return None, f"Column conversion error: {e}"

        if df["time"].isna().any():
            return None, "Invalid timestamps found"

        # Note: missing `cml_id` values are converted to the string 'nan'
        # so rows with missing IDs are preserved for ingestion.
        return df, None

    def get_file_type(self) -> str:
        return "rawdata"
