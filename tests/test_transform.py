"""
tests/test_transform.py — unit tests for pipeline/transform.py.

Uses a temporary directory with synthetic JSONL files so no live data
or internet access is required.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from pipeline.transform import build_prescribing_df, veracity_report

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SYNTHETIC_RECORDS = [
    {"date": "2024-01-01", "actual_cost": 100.0, "items": 10, "quantity": 500.0,
     "row_id": "A001-2024-01-01", "setting": 4, "ccg": "03V", "drug": "metformin"},
    {"date": "2024-02-01", "actual_cost": 200.0, "items": 20, "quantity": 1000.0,
     "row_id": "A001-2024-02-01", "setting": 4, "ccg": "03V", "drug": "metformin"},
    {"date": "2024-03-01", "actual_cost": None,  "items": 15, "quantity": 750.0,
     "row_id": "A001-2024-03-01", "setting": 4, "ccg": "03V", "drug": "metformin"},
    {"date": "2024-01-01", "actual_cost": 50.0,  "items": 5,  "quantity": 250.0,
     "row_id": "B001-2024-01-01", "setting": 4, "ccg": "14L", "drug": "atorvastatin"},
    {"date": "2024-02-01", "actual_cost": 75.0,  "items": None, "quantity": 375.0,
     "row_id": "B001-2024-02-01", "setting": 4, "ccg": "14L", "drug": "atorvastatin"},
]


@pytest.fixture()
def lake_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """Create a minimal lake structure with two drug JSONL files."""
    # metformin
    met_dir = tmp_path / "metformin"
    met_dir.mkdir()
    with (met_dir / "prescribing.jsonl").open("w") as fh:
        for rec in SYNTHETIC_RECORDS[:3]:
            fh.write(json.dumps(rec) + "\n")

    # atorvastatin
    ato_dir = tmp_path / "atorvastatin"
    ato_dir.mkdir()
    with (ato_dir / "prescribing.jsonl").open("w") as fh:
        for rec in SYNTHETIC_RECORDS[3:]:
            fh.write(json.dumps(rec) + "\n")

    return tmp_path


# ---------------------------------------------------------------------------
# build_prescribing_df tests
# ---------------------------------------------------------------------------


def test_build_prescribing_df_returns_lazy_frame(lake_dir):
    result = build_prescribing_df(lake_dir)
    assert isinstance(result, pl.LazyFrame)


def test_build_prescribing_df_nic_per_item_column_exists(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    assert "nic_per_item" in df.columns


def test_build_prescribing_df_year_month_column_exists(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    assert "year_month" in df.columns


def test_build_prescribing_df_year_month_format(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    # year_month should be "YYYY-MM" (7 chars)
    for val in df["year_month"].drop_nulls():
        assert len(val) == 7
        assert val[4] == "-"


def test_build_prescribing_df_actual_cost_is_float64(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    assert df["actual_cost"].dtype == pl.Float64


def test_build_prescribing_df_items_is_int64(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    assert df["items"].dtype == pl.Int64


def test_build_prescribing_df_row_count(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    assert len(df) == 5


def test_build_prescribing_df_nic_per_item_is_null_when_cost_null(lake_dir):
    """nic_per_item should be null when actual_cost is null."""
    df = build_prescribing_df(lake_dir).collect()
    null_cost_rows = df.filter(pl.col("actual_cost").is_null())
    assert null_cost_rows["nic_per_item"].null_count() == len(null_cost_rows)


# ---------------------------------------------------------------------------
# veracity_report tests
# ---------------------------------------------------------------------------


def test_veracity_report_returns_dataframe(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    report = veracity_report(df)
    assert isinstance(report, pl.DataFrame)


def test_veracity_report_has_field_column(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    report = veracity_report(df)
    assert "field" in report.columns


def test_veracity_report_has_required_columns(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    report = veracity_report(df)
    assert set(report.columns) == {"field", "null_count", "null_pct", "unique_count"}


def test_veracity_report_checks_four_fields(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    report = veracity_report(df)
    assert len(report) == 4


def test_veracity_report_detects_nulls(lake_dir):
    """actual_cost has 1 null and items has 1 null in the synthetic data."""
    df = build_prescribing_df(lake_dir).collect()
    report = veracity_report(df)

    cost_row = report.filter(pl.col("field") == "actual_cost")
    items_row = report.filter(pl.col("field") == "items")

    assert cost_row["null_count"][0] == 1
    assert items_row["null_count"][0] == 1


def test_veracity_report_null_pct_is_float(lake_dir):
    df = build_prescribing_df(lake_dir).collect()
    report = veracity_report(df)
    assert report["null_pct"].dtype == pl.Float64
