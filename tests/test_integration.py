"""
tests/test_integration.py — end-to-end integration tests.

Tests the full Bronze → Silver → Gold pipeline using synthetic data written
to a temporary lake directory and a temporary DuckDB file.  No internet
access, no mocks — every layer executes real code against real (synthetic) data.

Teaching point
--------------
These tests guard against cross-layer regressions: a change to the Silver
SQL that drops rows silently would pass unit tests for individual modules but
fail here because Gold row counts depend on what Silver contains.
"""

from __future__ import annotations

import json
import pathlib

import duckdb
import pytest

from pipeline.lake import write_lake
from pipeline.medallion import build_dim_practice, build_gold, build_silver

# ---------------------------------------------------------------------------
# Synthetic Bronze data — two drugs, 6 records, 1 both-null row (dropped in Silver)
# ---------------------------------------------------------------------------

_BRONZE_RECORDS = [
    # metformin — 3 records (1 cost-null, kept; the both-null is dropped)
    {"date": "2024-01-01", "actual_cost": 120.0, "items": 12, "quantity": 600.0,
     "row_id": "P001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    {"date": "2024-02-01", "actual_cost": 240.0, "items": 24, "quantity": 1200.0,
     "row_id": "P001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    {"date": "2024-03-01", "actual_cost": None,  "items": 18, "quantity": 900.0,
     "row_id": "P001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    # both-null — dropped in Silver
    {"date": "2024-01-01", "actual_cost": None,  "items": None, "quantity": 0.0,
     "row_id": "P002", "setting": "4", "ccg": "03V", "drug": "metformin"},
    # atorvastatin — 2 records
    {"date": "2024-01-01", "actual_cost": 60.0,  "items": 6,  "quantity": 300.0,
     "row_id": "Q001", "setting": "4", "ccg": "14L", "drug": "atorvastatin"},
    {"date": "2024-02-01", "actual_cost": 90.0,  "items": 9,  "quantity": 450.0,
     "row_id": "Q001", "setting": "4", "ccg": "14L", "drug": "atorvastatin"},
]

# Expected Silver row count: 6 total - 1 both-null = 5
_EXPECTED_SILVER_ROWS = 5


@pytest.fixture()
def bronze_lake(tmp_path: pathlib.Path) -> pathlib.Path:
    """Write synthetic records to a Bronze lake using write_lake()."""
    met_records = [r for r in _BRONZE_RECORDS if r["drug"] == "metformin"]
    ato_records = [r for r in _BRONZE_RECORDS if r["drug"] == "atorvastatin"]

    write_lake({"drug": "metformin",    "type": "openprescribing", "records": met_records}, tmp_path)
    write_lake({"drug": "atorvastatin", "type": "openprescribing", "records": ato_records}, tmp_path)
    return tmp_path


@pytest.fixture()
def pipeline_db(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path / "pipeline.duckdb"


# ---------------------------------------------------------------------------
# Full pipeline integration tests
# ---------------------------------------------------------------------------


def test_silver_row_count_after_write_lake(bronze_lake, pipeline_db):
    """Silver must contain exactly the non-both-null rows from Bronze."""
    rows = build_silver(bronze_lake, pipeline_db)
    assert rows == _EXPECTED_SILVER_ROWS


def test_gold_tables_exist_after_full_pipeline(bronze_lake, pipeline_db):
    """All three Gold tables must exist after Silver → Gold."""
    build_silver(bronze_lake, pipeline_db)
    counts = build_gold(pipeline_db)
    assert set(counts.keys()) == {
        "gold.drug_summary",
        "gold.drug_monthly_spend",
        "gold.practice_leaderboard",
    }


def test_gold_drug_summary_matches_silver(bronze_lake, pipeline_db):
    """Gold drug_summary total_items must equal sum of items in Silver."""
    build_silver(bronze_lake, pipeline_db)
    build_gold(pipeline_db)

    with duckdb.connect(str(pipeline_db)) as con:
        silver_total = con.execute(
            "SELECT SUM(items) FROM silver.prescribing"
        ).fetchone()[0]
        gold_total = con.execute(
            "SELECT SUM(total_items) FROM gold.drug_summary"
        ).fetchone()[0]

    assert silver_total == gold_total


def test_gold_monthly_spend_row_count(bronze_lake, pipeline_db):
    """Gold monthly_spend must have one row per (drug, month) combination."""
    build_silver(bronze_lake, pipeline_db)
    build_gold(pipeline_db)

    with duckdb.connect(str(pipeline_db)) as con:
        # metformin has 3 months, atorvastatin has 2 months → 5 rows
        count = con.execute(
            "SELECT COUNT(*) FROM gold.drug_monthly_spend"
        ).fetchone()[0]

    assert count == 5


def test_gold_practice_leaderboard_only_has_known_practices(bronze_lake, pipeline_db):
    """Leaderboard must not introduce practice IDs not in Silver."""
    build_silver(bronze_lake, pipeline_db)
    build_gold(pipeline_db)

    with duckdb.connect(str(pipeline_db)) as con:
        lb_practices = {
            r[0]
            for r in con.execute(
                "SELECT DISTINCT practice_id FROM gold.practice_leaderboard"
            ).fetchall()
        }
        silver_practices = {
            r[0]
            for r in con.execute(
                "SELECT DISTINCT row_id FROM silver.prescribing"
            ).fetchall()
        }

    assert lb_practices.issubset(silver_practices)


def test_dim_practice_built_from_silver(bronze_lake, pipeline_db):
    """dim_practice must be buildable from Silver and return at least 1 row."""
    build_silver(bronze_lake, pipeline_db)
    rows = build_dim_practice(pipeline_db)
    assert rows >= 1


def test_full_pipeline_idempotent(bronze_lake, pipeline_db):
    """Running the full pipeline twice must produce identical row counts."""
    build_silver(bronze_lake, pipeline_db)
    counts_1 = build_gold(pipeline_db)
    dim_1 = build_dim_practice(pipeline_db)

    # Run again — should produce same counts
    build_silver(bronze_lake, pipeline_db)
    counts_2 = build_gold(pipeline_db)
    dim_2 = build_dim_practice(pipeline_db)

    assert counts_1 == counts_2
    assert dim_1 == dim_2


def test_silver_nic_per_item_zero_items_is_null(tmp_path):
    """nic_per_item must be NULL (not inf) when items = 0."""
    lake = tmp_path / "lake"
    drug_dir = lake / "testdrug"
    drug_dir.mkdir(parents=True)
    record = {"date": "2024-01-01", "actual_cost": 100.0, "items": 0,
              "quantity": 0.0, "row_id": "X001", "setting": "4",
              "ccg": "03V", "drug": "testdrug"}
    with (drug_dir / "prescribing.jsonl").open("w") as fh:
        fh.write(json.dumps(record) + "\n")

    db = tmp_path / "test.duckdb"
    build_silver(lake, db)

    with duckdb.connect(str(db)) as con:
        val = con.execute(
            "SELECT nic_per_item FROM silver.prescribing WHERE row_id = 'X001'"
        ).fetchone()[0]

    assert val is None, f"Expected NULL for items=0, got {val}"
