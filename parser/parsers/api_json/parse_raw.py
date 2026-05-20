"""Parse a raw JSON file produced by the API fetcher.

The JSON file is a list of records, e.g.::

    [
        {"timestamp": "2026-01-01T00:00:00Z", "link_id": "10001",
         "sublink_id": "1", "value": -45.2},
        ...
    ]

A *field map* (loaded from the path in the ``FIELD_MAP_PATH`` environment
variable) maps file-name prefixes to column renaming / routing rules::

    # field_map.yml
    mock_operator_rsl:
      time:       timestamp
      cml_id:     link_id
      sublink_id: sublink_id
      rsl:        value        # "value" field → rsl column; tsl will be NaN
    mock_operator_tsl:
      time:       timestamp
      cml_id:     link_id
      sublink_id: sublink_id
      tsl:        value        # "value" field → tsl column; rsl will be NaN

The key lookup is a *longest-prefix* match against the file-name stem
(without extension), so a single file-name prefix can cover many date-stamped
files.
"""

import json
import logging
import os
from pathlib import Path

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

_FIELD_MAP_PATH_ENV = "FIELD_MAP_PATH"
_DEFAULT_FIELD_MAP_PATH = "/app/config/field_map.yml"
_REQUIRED_OUTPUT_COLS = ["time", "cml_id", "sublink_id", "rsl", "tsl"]


def _load_field_map(field_map_path: str) -> dict:
    with open(field_map_path) as f:
        return yaml.safe_load(f)


def _longest_prefix_match(stem: str, field_map: dict) -> str:
    """Return the field-map key that is the longest prefix of *stem*."""
    candidates = [k for k in field_map if stem.startswith(k)]
    if not candidates:
        raise ValueError(
            f"No field-map entry matches filename stem {stem!r}. "
            f"Available prefixes: {sorted(field_map)}"
        )
    return max(candidates, key=len)


def parse_api_json_raw(filepath: Path) -> pd.DataFrame:
    """Load a raw API JSON file and return a normalised DataFrame.

    The returned DataFrame always has columns ``[time, cml_id, sublink_id, rsl, tsl]``.
    Columns not provided by the source file are filled with ``NaN``.

    :param filepath: Path to the ``.json`` file in ``data/incoming/``.
    :raises FileNotFoundError: if the file does not exist.
    :raises ValueError: if no field-map entry matches the filename.
    :raises KeyError: if a required source field is missing from the records.
    """
    field_map_path = os.environ.get(_FIELD_MAP_PATH_ENV, _DEFAULT_FIELD_MAP_PATH)
    field_map = _load_field_map(field_map_path)

    stem = filepath.stem  # e.g. "mock_operator_20260101_20260102_rsl_data"
    matched_key = _longest_prefix_match(stem, field_map)
    mapping: dict = field_map[matched_key]  # internal_col → source_field

    with open(filepath) as f:
        records: list = json.load(f)

    if not records:
        logger.warning("Empty JSON file: %s", filepath.name)
        return pd.DataFrame(columns=_REQUIRED_OUTPUT_COLS)

    df_raw = pd.DataFrame(records)

    # Build output DataFrame column by column from the mapping
    df = pd.DataFrame()
    for internal_col, source_field in mapping.items():
        if source_field not in df_raw.columns:
            raise KeyError(
                f"Field {source_field!r} not found in {filepath.name}. "
                f"Available fields: {list(df_raw.columns)}"
            )
        df[internal_col] = df_raw[source_field]

    # Fill any missing output columns with NaN
    for col in _REQUIRED_OUTPUT_COLS:
        if col not in df.columns:
            df[col] = float("nan")

    # Normalise the time column to UTC-aware datetime
    df["time"] = pd.to_datetime(df["time"], utc=True)

    # Enforce expected column order
    df = df[_REQUIRED_OUTPUT_COLS]

    logger.info("Parsed %d records from %s", len(df), filepath.name)
    return df
