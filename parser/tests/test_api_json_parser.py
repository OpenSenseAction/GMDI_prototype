"""Tests for parser/parsers/api_json/parse_raw.py."""

import json
import os

import pandas as pd
import pytest

from ..parsers.api_json.parse_raw import (
    _longest_prefix_match,
    parse_api_json_raw,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FIELD_MAP = {
    "mock_operator_rsl": {
        "time": "timestamp",
        "cml_id": "link_id",
        "sublink_id": "sublink_id",
        "rsl": "value",
    },
    "mock_operator_tsl": {
        "time": "timestamp",
        "cml_id": "link_id",
        "sublink_id": "sublink_id",
        "tsl": "value",
    },
}

_RECORDS = [
    {"timestamp": "2026-01-01T00:00:00Z", "link_id": "10001", "sublink_id": "1", "value": -45.2},
    {"timestamp": "2026-01-01T01:00:00Z", "link_id": "10001", "sublink_id": "1", "value": -44.8},
]


def _write_field_map(tmp_path, content=None) -> str:
    import yaml
    path = tmp_path / "field_map.yml"
    path.write_text(yaml.dump(content or _FIELD_MAP))
    return str(path)


def _write_json(tmp_path, name, records=None) -> object:
    from pathlib import Path
    p = tmp_path / name
    p.write_text(json.dumps(records if records is not None else _RECORDS))
    return p


# ---------------------------------------------------------------------------
# _longest_prefix_match
# ---------------------------------------------------------------------------

def test_prefix_match_exact():
    assert _longest_prefix_match("mock_operator_rsl_20260101", _FIELD_MAP) == "mock_operator_rsl"


def test_prefix_match_longest_wins():
    fm = {"mock": {}, "mock_operator_rsl": {}}
    assert _longest_prefix_match("mock_operator_rsl_20260101", fm) == "mock_operator_rsl"


def test_prefix_match_no_match_raises():
    with pytest.raises(ValueError, match="No field-map entry"):
        _longest_prefix_match("unknown_operator_20260101", _FIELD_MAP)


# ---------------------------------------------------------------------------
# parse_api_json_raw — happy paths
# ---------------------------------------------------------------------------

def test_parse_rsl_file(tmp_path, monkeypatch):
    fm_path = _write_field_map(tmp_path)
    monkeypatch.setenv("FIELD_MAP_PATH", fm_path)
    f = _write_json(tmp_path, "mock_operator_rsl_20260101_20260102_data.json")

    df = parse_api_json_raw(f)

    assert list(df.columns) == ["time", "cml_id", "sublink_id", "rsl", "tsl"]
    assert len(df) == 2
    assert df["rsl"].notna().all()
    assert df["tsl"].isna().all()
    assert df["cml_id"].iloc[0] == "10001"


def test_parse_tsl_file(tmp_path, monkeypatch):
    fm_path = _write_field_map(tmp_path)
    monkeypatch.setenv("FIELD_MAP_PATH", fm_path)
    f = _write_json(tmp_path, "mock_operator_tsl_20260101_20260102_data.json")

    df = parse_api_json_raw(f)

    assert df["tsl"].notna().all()
    assert df["rsl"].isna().all()


def test_time_column_is_utc_datetime(tmp_path, monkeypatch):
    fm_path = _write_field_map(tmp_path)
    monkeypatch.setenv("FIELD_MAP_PATH", fm_path)
    f = _write_json(tmp_path, "mock_operator_rsl_20260101_data.json")

    df = parse_api_json_raw(f)

    assert pd.api.types.is_datetime64_any_dtype(df["time"])
    assert str(df["time"].dt.tz) == "UTC"


def test_empty_json_file_returns_empty_df(tmp_path, monkeypatch):
    fm_path = _write_field_map(tmp_path)
    monkeypatch.setenv("FIELD_MAP_PATH", fm_path)
    f = _write_json(tmp_path, "mock_operator_rsl_20260101_data.json", records=[])

    df = parse_api_json_raw(f)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    assert list(df.columns) == ["time", "cml_id", "sublink_id", "rsl", "tsl"]


# ---------------------------------------------------------------------------
# parse_api_json_raw — error paths
# ---------------------------------------------------------------------------

def test_missing_source_field_raises(tmp_path, monkeypatch):
    fm_path = _write_field_map(tmp_path)
    monkeypatch.setenv("FIELD_MAP_PATH", fm_path)
    records = [{"timestamp": "2026-01-01T00:00:00Z", "link_id": "10001", "sublink_id": "1"}]
    # 'value' key is absent → KeyError
    f = _write_json(tmp_path, "mock_operator_rsl_20260101_data.json", records=records)

    with pytest.raises(KeyError, match="value"):
        parse_api_json_raw(f)


def test_no_matching_prefix_raises(tmp_path, monkeypatch):
    fm_path = _write_field_map(tmp_path)
    monkeypatch.setenv("FIELD_MAP_PATH", fm_path)
    f = _write_json(tmp_path, "unknown_operator_20260101_data.json")

    with pytest.raises(ValueError, match="No field-map entry"):
        parse_api_json_raw(f)
