"""
tests/test_medallion.py — unit tests for pipeline/medallion.py.

Uses a temporary lake directory with synthetic JSONL files and a
temporary DuckDB file so no live data or internet access is required.
"""

from __future__ import annotations

import json
import pathlib

import duckdb
import pytest

from pipeline.medallion import build_dim_practice, build_gold, build_silver, build_silver_for_range

# ---------------------------------------------------------------------------
# Fixtures — reuse the same synthetic data shape as test_transform.py
# ---------------------------------------------------------------------------

SYNTHETIC_RECORDS = [
    {"date": "2024-01-01", "actual_cost": 100.0, "items": 10, "quantity": 500.0,
     "row_id": "A001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    {"date": "2024-02-01", "actual_cost": 200.0, "items": 20, "quantity": 1000.0,
     "row_id": "A001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    {"date": "2024-03-01", "actual_cost": None,  "items": 15, "quantity": 750.0,
     "row_id": "A001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    # Row where BOTH cost and items are NULL — should be dropped in Silver
    {"date": "2024-01-01", "actual_cost": None,  "items": None, "quantity": 250.0,
     "row_id": "A002", "setting": "4", "ccg": "03V", "drug": "metformin"},
    {"date": "2024-01-01", "actual_cost": 50.0,  "items": 5,   "quantity": 250.0,
     "row_id": "B001", "setting": "4", "ccg": "14L", "drug": "atorvastatin"},
    {"date": "2024-02-01", "actual_cost": 75.0,  "items": 8,   "quantity": 400.0,
     "row_id": "B001", "setting": "4", "ccg": "14L", "drug": "atorvastatin"},
]


@pytest.fixture()
def lake_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """Minimal Bronze lake: two drug subdirectories with JSONL files."""
    met_dir = tmp_path / "metformin"
    met_dir.mkdir()
    with (met_dir / "prescribing.jsonl").open("w") as fh:
        for rec in SYNTHETIC_RECORDS[:4]:  # includes the both-null row
            fh.write(json.dumps(rec) + "\n")

    ato_dir = tmp_path / "atorvastatin"
    ato_dir.mkdir()
    with (ato_dir / "prescribing.jsonl").open("w") as fh:
        for rec in SYNTHETIC_RECORDS[4:]:
            fh.write(json.dumps(rec) + "\n")

    return tmp_path


@pytest.fixture()
def db_path(tmp_path: pathlib.Path) -> pathlib.Path:
    """Isolated DuckDB file for each test."""
    return tmp_path / "test_pipeline.duckdb"


# ---------------------------------------------------------------------------
# build_silver tests
# ---------------------------------------------------------------------------


def test_build_silver_returns_row_count(lake_dir, db_path):
    count = build_silver(lake_dir, db_path)
    assert isinstance(count, int)
    assert count > 0


def test_build_silver_drops_both_null_rows(lake_dir, db_path):
    """Rows where both actual_cost AND items are NULL must be dropped."""
    count = build_silver(lake_dir, db_path)
    # 4 metformin records written but 1 has both-null → 3 metformin + 2 atorvastatin = 5
    assert count == 5


def test_build_silver_creates_table(lake_dir, db_path):
    build_silver(lake_dir, db_path)
    con = duckdb.connect(str(db_path))
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'silver' AND table_name = 'prescribing'"
    ).fetchone()[0]
    con.close()
    assert result == 1


def test_build_silver_date_is_date_type(lake_dir, db_path):
    build_silver(lake_dir, db_path)
    con = duckdb.connect(str(db_path))
    dtype = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema='silver' AND table_name='prescribing' AND column_name='date'"
    ).fetchone()[0]
    con.close()
    assert dtype == "DATE"


def test_build_silver_has_nic_per_item_column(lake_dir, db_path):
    build_silver(lake_dir, db_path)
    con = duckdb.connect(str(db_path))
    cols = [
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='silver' AND table_name='prescribing'"
        ).fetchall()
    ]
    con.close()
    assert "nic_per_item" in cols


def test_build_silver_has_year_month_column(lake_dir, db_path):
    build_silver(lake_dir, db_path)
    con = duckdb.connect(str(db_path))
    cols = [
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='silver' AND table_name='prescribing'"
        ).fetchall()
    ]
    con.close()
    assert "year_month" in cols


def test_build_silver_has_ingested_at_column(lake_dir, db_path):
    build_silver(lake_dir, db_path)
    con = duckdb.connect(str(db_path))
    cols = [
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='silver' AND table_name='prescribing'"
        ).fetchall()
    ]
    con.close()
    assert "ingested_at" in cols


def test_build_silver_nic_per_item_null_when_cost_null(lake_dir, db_path):
    """nic_per_item must be NULL when actual_cost is NULL."""
    build_silver(lake_dir, db_path)
    con = duckdb.connect(str(db_path))
    result = con.execute(
        """
        SELECT COUNT(*) FROM silver.prescribing
        WHERE actual_cost IS NULL AND nic_per_item IS NOT NULL
        """
    ).fetchone()[0]
    con.close()
    assert result == 0


def test_build_silver_idempotent(lake_dir, db_path):
    """Calling build_silver twice should not double the row count."""
    count_first = build_silver(lake_dir, db_path)
    count_second = build_silver(lake_dir, db_path)
    assert count_first == count_second


# ---------------------------------------------------------------------------
# build_gold tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def silver_db(lake_dir, db_path):
    """Build Silver first so Gold tests have something to aggregate."""
    build_silver(lake_dir, db_path)
    return db_path


def test_build_gold_returns_dict(silver_db):
    counts = build_gold(silver_db)
    assert isinstance(counts, dict)


def test_build_gold_returns_three_tables(silver_db):
    counts = build_gold(silver_db)
    assert len(counts) == 3


def test_build_gold_creates_drug_summary(silver_db):
    build_gold(silver_db)
    con = duckdb.connect(str(silver_db))
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='gold' AND table_name='drug_summary'"
    ).fetchone()[0]
    con.close()
    assert result == 1


def test_build_gold_drug_summary_has_both_drugs(silver_db):
    build_gold(silver_db)
    con = duckdb.connect(str(silver_db))
    drugs = {
        row[0]
        for row in con.execute(
            "SELECT drug FROM gold.drug_summary"
        ).fetchall()
    }
    con.close()
    assert drugs == {"metformin", "atorvastatin"}


def test_build_gold_drug_summary_total_items_positive(silver_db):
    build_gold(silver_db)
    con = duckdb.connect(str(silver_db))
    result = con.execute(
        "SELECT MIN(total_items) FROM gold.drug_summary"
    ).fetchone()[0]
    con.close()
    assert result > 0


def test_build_gold_monthly_spend_has_rows(silver_db):
    counts = build_gold(silver_db)
    assert counts["gold.drug_monthly_spend"] > 0


def test_build_gold_practice_leaderboard_rank_max_20(silver_db):
    build_gold(silver_db)
    con = duckdb.connect(str(silver_db))
    max_rank = con.execute(
        "SELECT MAX(rank) FROM gold.practice_leaderboard"
    ).fetchone()[0]
    con.close()
    assert max_rank <= 20


def test_build_gold_idempotent(silver_db):
    """Calling build_gold twice should produce the same row counts."""
    counts_first = build_gold(silver_db)
    counts_second = build_gold(silver_db)
    assert counts_first == counts_second


# ---------------------------------------------------------------------------
# build_dim_practice (SCD Type 2) tests
#
# Uses a dedicated fixture with a practice that changes CCG mid-stream —
# simulating the July 2022 CCG → ICB reorganisation in England.
# ---------------------------------------------------------------------------

# Practice A001: stable throughout (one version expected)
# Practice A002: changes CCG from "03V" to "15N" (two versions expected)
#   — mirrors a real GP practice that moved from a CCG to a new ICB
SCD_RECORDS = [
    {"date": "2022-01-01", "actual_cost": 100.0, "items": 10, "quantity": 500.0,
     "row_id": "A001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    {"date": "2022-04-01", "actual_cost": 110.0, "items": 11, "quantity": 550.0,
     "row_id": "A001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    # A002 before CCG reorganisation
    {"date": "2022-01-01", "actual_cost": 50.0, "items": 5, "quantity": 250.0,
     "row_id": "A002", "setting": "4", "ccg": "03V", "drug": "metformin"},
    # A002 after CCG → ICB reorganisation (July 2022 → new CCG code "15N")
    {"date": "2022-08-01", "actual_cost": 60.0, "items": 6, "quantity": 300.0,
     "row_id": "A002", "setting": "4", "ccg": "15N", "drug": "metformin"},
]


@pytest.fixture()
def scd_db(tmp_path: pathlib.Path) -> pathlib.Path:
    """Lake with a CCG-changing practice; Silver already built."""
    met_dir = tmp_path / "metformin"
    met_dir.mkdir()
    with (met_dir / "prescribing.jsonl").open("w") as fh:
        for rec in SCD_RECORDS:
            fh.write(json.dumps(rec) + "\n")
    db_path = tmp_path / "scd.duckdb"
    build_silver(tmp_path, db_path)
    return db_path


def test_build_dim_practice_creates_table(scd_db):
    build_dim_practice(scd_db)
    con = duckdb.connect(str(scd_db))
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='gold' AND table_name='dim_practice'"
    ).fetchone()[0]
    con.close()
    assert result == 1


def test_build_dim_practice_returns_row_count(scd_db):
    count = build_dim_practice(scd_db)
    assert isinstance(count, int)
    # A001: 1 version, A002: 2 versions → 3 rows total
    assert count == 3


def test_build_dim_practice_stable_practice_has_one_version(scd_db):
    """A001 never changes CCG — should have exactly one row, is_current=True."""
    build_dim_practice(scd_db)
    con = duckdb.connect(str(scd_db))
    rows = con.execute(
        "SELECT version_num, is_current, valid_to "
        "FROM gold.dim_practice WHERE practice_id = 'A001'"
    ).fetchall()
    con.close()
    assert len(rows) == 1
    assert rows[0][0] == 1         # version_num = 1
    assert rows[0][1] is True      # is_current
    assert rows[0][2] is None      # valid_to = NULL


def test_build_dim_practice_changing_practice_has_two_versions(scd_db):
    """A002 changes CCG — should have two versioned rows."""
    build_dim_practice(scd_db)
    con = duckdb.connect(str(scd_db))
    rows = con.execute(
        "SELECT version_num, ccg, valid_to, is_current "
        "FROM gold.dim_practice WHERE practice_id = 'A002' "
        "ORDER BY version_num"
    ).fetchall()
    con.close()
    assert len(rows) == 2

    # Version 1: old CCG, has a valid_to, not current
    assert rows[0][0] == 1          # version_num
    assert rows[0][1] == "03V"      # old ccg
    assert rows[0][2] is not None   # valid_to IS SET
    assert rows[0][3] is False      # not current

    # Version 2: new ICB code, valid_to = NULL, is current
    assert rows[1][0] == 2          # version_num
    assert rows[1][1] == "15N"      # new ccg (ICB)
    assert rows[1][2] is None       # valid_to = NULL
    assert rows[1][3] is True       # is_current


def test_build_dim_practice_exactly_one_current_per_practice(scd_db):
    """Each practice must have exactly one is_current = True row."""
    build_dim_practice(scd_db)
    con = duckdb.connect(str(scd_db))
    violations = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT practice_id, COUNT(*) AS n
            FROM gold.dim_practice
            WHERE is_current = TRUE
            GROUP BY practice_id
            HAVING n <> 1
        )
        """
    ).fetchone()[0]
    con.close()
    assert violations == 0


def test_build_dim_practice_old_version_valid_to_equals_next_valid_from(scd_db):
    """The valid_to of version 1 must equal the valid_from of version 2."""
    build_dim_practice(scd_db)
    con = duckdb.connect(str(scd_db))
    gap = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT
                d1.practice_id,
                d1.valid_to,
                d2.valid_from
            FROM gold.dim_practice d1
            JOIN gold.dim_practice d2
              ON  d1.practice_id = d2.practice_id
              AND d2.version_num  = d1.version_num + 1
            WHERE d1.valid_to <> d2.valid_from
        )
        """
    ).fetchone()[0]
    con.close()
    assert gap == 0


def test_build_dim_practice_idempotent(scd_db):
    count1 = build_dim_practice(scd_db)
    count2 = build_dim_practice(scd_db)
    assert count1 == count2


# ---------------------------------------------------------------------------
# build_silver_for_range tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def silver_db(lake_dir, db_path):
    """DuckDB with a pre-built Silver table ready for backfill tests."""
    build_silver(lake_dir, db_path)
    return db_path


def test_build_silver_for_range_returns_int(silver_db, lake_dir):
    result = build_silver_for_range(lake_dir, silver_db, "2024-01", "2024-01")
    assert isinstance(result, int)


def test_build_silver_for_range_inserts_rows_in_window(silver_db, lake_dir):
    """Rows for the target month must appear after backfill."""
    inserted = build_silver_for_range(lake_dir, silver_db, "2024-01", "2024-01")
    assert inserted > 0


def test_build_silver_for_range_excludes_rows_outside_window(silver_db, lake_dir):
    """Rows outside the window must not be affected."""
    con = duckdb.connect(str(silver_db))
    before_feb = con.execute(
        "SELECT COUNT(*) FROM silver.prescribing WHERE year_month = '2024-02'"
    ).fetchone()[0]
    con.close()

    build_silver_for_range(lake_dir, silver_db, "2024-01", "2024-01")

    con = duckdb.connect(str(silver_db))
    after_feb = con.execute(
        "SELECT COUNT(*) FROM silver.prescribing WHERE year_month = '2024-02'"
    ).fetchone()[0]
    con.close()

    assert before_feb == after_feb


def test_build_silver_for_range_is_idempotent(silver_db, lake_dir):
    """Running the backfill twice must produce the same row count."""
    first = build_silver_for_range(lake_dir, silver_db, "2024-01", "2024-02")
    second = build_silver_for_range(lake_dir, silver_db, "2024-01", "2024-02")
    assert first == second


def test_build_silver_for_range_no_double_counting(silver_db, lake_dir):
    """After two runs the total Silver rows must equal one run, not two."""
    build_silver_for_range(lake_dir, silver_db, "2024-01", "2024-01")
    build_silver_for_range(lake_dir, silver_db, "2024-01", "2024-01")

    con = duckdb.connect(str(silver_db))
    count = con.execute(
        "SELECT COUNT(*) FROM silver.prescribing WHERE year_month = '2024-01'"
    ).fetchone()[0]
    con.close()

    # Synthetic data: Jan 2024 has 1 metformin (A001) + 1 atorvastatin (B001) = 2 valid rows.
    # The metformin A002 row (both cost AND items NULL) is dropped by the WHERE clause.
    assert count == 2


def test_build_silver_for_range_raises_without_silver(lake_dir, db_path):
    """Must raise RuntimeError when silver.prescribing does not exist."""
    with pytest.raises(RuntimeError, match="silver.prescribing does not exist"):
        build_silver_for_range(lake_dir, db_path, "2024-01", "2024-01")
