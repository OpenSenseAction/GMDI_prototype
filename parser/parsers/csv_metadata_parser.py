"""CSV parser for CML metadata files."""

from pathlib import Path
import re
from typing import Optional, Tuple
import pandas as pd

from .base_parser import BaseParser


class CSVMetadataParser(BaseParser):
    REQUIRED_COLUMNS = [
        "cml_id",
        "site_0_lon",
        "site_0_lat",
        "site_1_lon",
        "site_1_lat",
    ]
    FILE_PATTERN = re.compile(r"^cml_metadata_.*\.csv$", re.IGNORECASE)

    def can_parse(self, filepath: Path) -> bool:
        return bool(self.FILE_PATTERN.match(filepath.name))

    def parse(self, filepath: Path) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            return None, f"Failed to read CSV: {e}"

        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            return None, f"Missing required columns: {missing}"

        try:
            df["cml_id"] = df["cml_id"].astype(str)
            for col in ["site_0_lon", "site_0_lat", "site_1_lon", "site_1_lat"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        except Exception as e:
            return None, f"Column conversion error: {e}"

        # Basic coordinate validation
        if df["site_0_lon"].notna().any():
            if not df["site_0_lon"].between(-180, 180).all():
                return None, "Invalid longitude values in site_0_lon"
        if df["site_1_lon"].notna().any():
            if not df["site_1_lon"].between(-180, 180).all():
                return None, "Invalid longitude values in site_1_lon"

        if df["site_0_lat"].notna().any():
            if not df["site_0_lat"].between(-90, 90).all():
                return None, "Invalid latitude values in site_0_lat"
        if df["site_1_lat"].notna().any():
            if not df["site_1_lat"].between(-90, 90).all():
                return None, "Invalid latitude values in site_1_lat"

        df = df.loc[:, self.REQUIRED_COLUMNS]

        return df, None

    def get_file_type(self) -> str:
        return "metadata"
