"""Parser for OtherMNO CSV format.

This is an example of a simple code-based parser, demonstrating the pattern
for adding new MNO-specific parsers without using the config-driven approach.

Differences from demo_csv_data:
- Uses semicolon separator by default
- Column names are different (requires rename)
- Timestamps include timezone offset (+01:00)
"""

import pandas as pd
from pathlib import Path
from typing import Optional


def parse_rawdata_csv(filepath: Path) -> Optional[pd.DataFrame]:
    """Parse raw data CSV from OtherMNO format.

    Expected input columns:
        timestamp, link_id, sublink, tx_power, rx_power

    Output columns (canonical):
        time, cml_id, sublink_id, tsl, rsl
    """
    df = pd.read_csv(filepath, sep=";")

    # Rename to canonical names
    df = df.rename(
        columns={
            "timestamp": "time",
            "link_id": "cml_id",
            "sublink": "sublink_id",
            "tx_power": "tsl",
            "rx_power": "rsl",
        }
    )

    # Parse timestamps (they include timezone offset)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Ensure canonical types
    df["cml_id"] = df["cml_id"].fillna("nan").astype(str)
    df["sublink_id"] = df["sublink_id"].astype(str)
    df["tsl"] = pd.to_numeric(df.get("tsl"), errors="coerce")
    df["rsl"] = pd.to_numeric(df.get("rsl"), errors="coerce")

    return df


def parse_metadata_csv(filepath: Path) -> Optional[pd.DataFrame]:
    """Parse metadata CSV from OtherMNO format.

    Expected input columns:
        link_id, sublink, lon_a, lat_a, lon_b, lat_b, freq_mhz, pol, length_km

    Output columns (canonical):
        cml_id, sublink_id, site_0_lon, site_0_lat, site_1_lon, site_1_lat,
        frequency, polarization, length
    """
    df = pd.read_csv(filepath, sep=";")

    # Rename to canonical names
    df = df.rename(
        columns={
            "link_id": "cml_id",
            "sublink": "sublink_id",
            "lon_a": "site_0_lon",
            "lat_a": "site_0_lat",
            "lon_b": "site_1_lon",
            "lat_b": "site_1_lat",
            "freq_mhz": "frequency",
            "pol": "polarization",
            "length_km": "length",
        }
    )

    # Ensure canonical types
    df["cml_id"] = df["cml_id"].astype(str)
    for col in ["site_0_lon", "site_0_lat", "site_1_lon", "site_1_lat", "frequency", "length"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df
