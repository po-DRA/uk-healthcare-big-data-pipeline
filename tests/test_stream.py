"""
tests/test_stream.py — unit tests for pipeline/stream.py.

Uses a temporary lake directory with synthetic JSONL files and a temporary
DuckDB file.  No live data or network access required.
"""

from __future__ import annotations

import json
import pathlib

import duckdb
import pytest

from pipeline.stream import prescribing_event_stream, stream_into_duckdb

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_RECORDS = [
    {"date": "2024-01-01", "actual_cost": 10.0, "items": 1, "quantity": 28.0,
     "row_id": "A001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    {"date": "2024-02-01", "actual_cost": 20.0, "items": 2, "quantity": 56.0,
     "row_id": "A001", "setting": "4", "ccg": "03V", "drug": "metformin"},
    {"date": "2024-03-01", "actual_cost": 30.0, "items": 3, "quantity": 84.0,
     "row_id": "B001", "setting": "4", "ccg": "14L", "drug": "metformin"},
    {"date": "2024-04-01", "actual_cost": 40.0, "items": 4, "quantity": 112.0,
     "row_id": "B001", "setting": "4", "ccg": "14L", "drug": "metformin"},
    {"date": "2024-05-01", "actual_cost": 50.0, "items": 5, "quantity": 140.0,
     "row_id": "C001", "setting": "4", "ccg": "99X", "drug": "metformin"},
]


@pytest.fixture()
def lake_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """Bronze lake with a single metformin JSONL file."""
    drug_dir = tmp_path / "metformin" / "EPD_202506"
    drug_dir.mkdir(parents=True)
    with (drug_dir / "prescribing.jsonl").open("w") as fh:
        for rec in _RECORDS:
            fh.write(json.dumps(rec) + "\n")
    return tmp_path


@pytest.fixture()
def db_path(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path / "stream_test.duckdb"


# ---------------------------------------------------------------------------
# prescribing_event_stream tests
# ---------------------------------------------------------------------------


def test_stream_yields_all_records(lake_dir):
    """Total records across all batches must equal JSONL line count."""
    total = sum(
        len(batch)
        for batch in prescribing_event_stream(lake_dir, "metformin", batch_size=2)
    )
    assert total == len(_RECORDS)


def test_stream_batch_size_respected(lake_dir):
    """No batch (except the last) should exceed batch_size."""
    batch_size = 2
    batches = list(prescribing_event_stream(lake_dir, "metformin", batch_size=batch_size))
    for batch in batches[:-1]:
        assert len(batch) == batch_size


def test_stream_last_batch_flushes_remainder(lake_dir):
    """The final batch must contain the remainder records (5 % 2 = 1)."""
    batches = list(prescribing_event_stream(lake_dir, "metformin", batch_size=2))
    assert len(batches[-1]) == 1  # 5 records, batch_size=2 → last batch has 1


def test_stream_batch_size_larger_than_file(lake_dir):
    """When batch_size > record count, exactly one batch is yielded."""
    batches = list(
        prescribing_event_stream(lake_dir, "metformin", batch_size=1000)
    )
    assert len(batches) == 1
    assert len(batches[0]) == len(_RECORDS)


def test_stream_records_are_dicts(lake_dir):
    """Each item in a batch must be a dict."""
    for batch in prescribing_event_stream(lake_dir, "metformin", batch_size=3):
        for record in batch:
            assert isinstance(record, dict)


def test_stream_file_not_found_raises(tmp_path):
    """FileNotFoundError must be raised if the JSONL file does not exist."""
    with pytest.raises(FileNotFoundError, match="Bronze lake file not found"):
        list(prescribing_event_stream(tmp_path, "nonexistent_drug"))


def test_stream_empty_lines_skipped(tmp_path):
    """Blank lines in JSONL must be skipped without error."""
    drug_dir = tmp_path / "metformin" / "EPD_202506"
    drug_dir.mkdir(parents=True)
    with (drug_dir / "prescribing.jsonl").open("w") as fh:
        fh.write(json.dumps(_RECORDS[0]) + "\n")
        fh.write("\n")  # blank line
        fh.write(json.dumps(_RECORDS[1]) + "\n")

    total = sum(
        len(b) for b in prescribing_event_stream(tmp_path, "metformin", batch_size=10)
    )
    assert total == 2


# ---------------------------------------------------------------------------
# stream_into_duckdb tests
# ---------------------------------------------------------------------------


def test_stream_into_duckdb_returns_dict(lake_dir, db_path):
    """Return value must be a dict with the expected keys."""
    result = stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=2)
    assert isinstance(result, dict)
    assert set(result.keys()) == {
        "drug", "total_batches", "total_records", "total_items", "total_cost_gbp"
    }


def test_stream_into_duckdb_total_records(lake_dir, db_path):
    """total_records in return value must equal the JSONL line count."""
    result = stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=2)
    assert result["total_records"] == len(_RECORDS)


def test_stream_into_duckdb_total_batches(lake_dir, db_path):
    """Batch count must equal ceil(records / batch_size)."""
    result = stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=2)
    import math
    assert result["total_batches"] == math.ceil(len(_RECORDS) / 2)


def test_stream_into_duckdb_creates_table(lake_dir, db_path):
    """streaming.live_prescribing table must exist after streaming."""
    stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=10)
    with duckdb.connect(str(db_path)) as con:
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'streaming'"
            ).fetchall()
        ]
    assert "live_prescribing" in tables


def test_stream_into_duckdb_row_count_in_table(lake_dir, db_path):
    """DuckDB table row count must match total_records."""
    result = stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=2)
    with duckdb.connect(str(db_path)) as con:
        db_count = con.execute(
            "SELECT COUNT(*) FROM streaming.live_prescribing WHERE drug = 'metformin'"
        ).fetchone()[0]
    assert db_count == result["total_records"]


def test_stream_into_duckdb_total_items_correct(lake_dir, db_path):
    """total_items must equal sum of items across all records."""
    expected = sum(r["items"] for r in _RECORDS)
    result = stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=10)
    assert result["total_items"] == expected


def test_stream_into_duckdb_idempotent(lake_dir, db_path):
    """Running twice must produce the same row count (not doubled)."""
    r1 = stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=10)
    r2 = stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=10)
    assert r1["total_records"] == r2["total_records"]

    with duckdb.connect(str(db_path)) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM streaming.live_prescribing WHERE drug = 'metformin'"
        ).fetchone()[0]
    assert count == len(_RECORDS)


def test_stream_into_duckdb_batch_num_assigned(lake_dir, db_path):
    """Each row must have a batch_num > 0."""
    stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=2)
    with duckdb.connect(str(db_path)) as con:
        min_batch = con.execute(
            "SELECT MIN(batch_num) FROM streaming.live_prescribing"
        ).fetchone()[0]
    assert min_batch == 1
