"""
tests/test_contracts.py — unit tests for pipeline/contracts.py.

Covers:
  - Pydantic model validation (happy paths and error paths)
  - SilverDQViolation exception structure
  - DEFAULT_NULL_THRESHOLDS sanity checks
"""

from __future__ import annotations

import json
import pathlib

import duckdb
import pytest
from pydantic import ValidationError

from pipeline.contracts import (
    DEFAULT_NULL_THRESHOLDS,
    NHSBSAPayload,
    NHSPage,
    NHSPagesPayload,
    PrescribingRecord,
    SilverDQViolation,
)

# ---------------------------------------------------------------------------
# Canned valid data
# ---------------------------------------------------------------------------

VALID_RECORD = {
    "date": "2024-01-01",
    "actual_cost": 100.0,
    "items": 10,
    "quantity": 500.0,
    "row_id": "A81001",
    "setting": "4",
    "ccg": "03V",
    "drug": "metformin",
}

VALID_PRESCRIBING_PAYLOAD = {
    "drug": "metformin",
    "bnf_code": "0601022B0",
    "source": "nhsbsa_epd",
    "resource": "EPD_202506",
    "total_rows": 1,
    "records": [VALID_RECORD],
}

VALID_PAGE = {
    "url": "https://www.nhs.uk/medicines/metformin/side-effects/",
    "page_type": "side_effects",
    "heading": "Common side effects",
    "text": "Nausea is common.",
    "bullets": ["Feeling sick", "Diarrhoea"],
}

VALID_NHS_PAYLOAD = {
    "drug": "metformin",
    "type": "nhs_pages",
    "pages": [VALID_PAGE],
}


# ---------------------------------------------------------------------------
# PrescribingRecord
# ---------------------------------------------------------------------------


def test_prescribing_record_valid():
    rec = PrescribingRecord.model_validate(VALID_RECORD)
    assert rec.drug == "metformin"
    assert rec.actual_cost == 100.0
    assert rec.items == 10


def test_prescribing_record_nulls_allowed():
    """actual_cost and items may be None (sparse API data)."""
    rec = PrescribingRecord.model_validate({**VALID_RECORD, "actual_cost": None, "items": None})
    assert rec.actual_cost is None
    assert rec.items is None


def test_prescribing_record_coerces_numeric_string():
    """Pydantic v2 coerces '100' → 100.0 for float fields."""
    rec = PrescribingRecord.model_validate({**VALID_RECORD, "actual_cost": "100"})
    assert rec.actual_cost == 100.0


def test_prescribing_record_rejects_non_numeric_cost():
    with pytest.raises(ValidationError):
        PrescribingRecord.model_validate({**VALID_RECORD, "actual_cost": "xyz"})


def test_prescribing_record_ignores_extra_fields():
    """Extra fields from the API (e.g. new columns) are silently dropped."""
    rec = PrescribingRecord.model_validate({**VALID_RECORD, "unknown_future_field": "ignored"})
    assert not hasattr(rec, "unknown_future_field")


def test_prescribing_record_missing_required_field_raises():
    bad = {k: v for k, v in VALID_RECORD.items() if k != "drug"}
    with pytest.raises(ValidationError):
        PrescribingRecord.model_validate(bad)


# ---------------------------------------------------------------------------
# NHSBSAPayload
# ---------------------------------------------------------------------------


def test_nhsbsa_payload_valid():
    payload = NHSBSAPayload.model_validate(VALID_PRESCRIBING_PAYLOAD)
    assert payload.drug == "metformin"
    assert payload.source == "nhsbsa_epd"
    assert payload.resource == "EPD_202506"
    assert len(payload.records) == 1


def test_nhsbsa_payload_wrong_source_raises():
    bad = {**VALID_PRESCRIBING_PAYLOAD, "source": "openprescribing"}
    with pytest.raises(ValidationError):
        NHSBSAPayload.model_validate(bad)


def test_nhsbsa_payload_negative_total_rows_raises():
    bad = {**VALID_PRESCRIBING_PAYLOAD, "total_rows": -1}
    with pytest.raises(ValidationError):
        NHSBSAPayload.model_validate(bad)


def test_nhsbsa_payload_empty_records_is_valid():
    """An empty records list is valid — drug may have no rows in target month."""
    payload = NHSBSAPayload.model_validate(
        {**VALID_PRESCRIBING_PAYLOAD, "records": [], "total_rows": 0}
    )
    assert payload.records == []


def test_nhsbsa_payload_model_dump_round_trips():
    payload = NHSBSAPayload.model_validate(VALID_PRESCRIBING_PAYLOAD)
    dumped = payload.model_dump()
    assert dumped["drug"] == "metformin"
    assert isinstance(dumped["records"], list)
    assert dumped["records"][0]["actual_cost"] == 100.0


# ---------------------------------------------------------------------------
# NHSPage
# ---------------------------------------------------------------------------


def test_nhs_page_valid():
    page = NHSPage.model_validate(VALID_PAGE)
    assert page.page_type == "side_effects"
    assert page.bullets == ["Feeling sick", "Diarrhoea"]


def test_nhs_page_invalid_page_type_raises():
    bad = {**VALID_PAGE, "page_type": "dosage"}
    with pytest.raises(ValidationError):
        NHSPage.model_validate(bad)


def test_nhs_page_optional_fields_have_defaults():
    minimal = {"url": "https://nhs.uk/", "page_type": "side_effects"}
    page = NHSPage.model_validate(minimal)
    assert page.heading == ""
    assert page.text == ""
    assert page.bullets == []


# ---------------------------------------------------------------------------
# NHSPagesPayload
# ---------------------------------------------------------------------------


def test_nhs_pages_payload_valid():
    payload = NHSPagesPayload.model_validate(VALID_NHS_PAYLOAD)
    assert payload.drug == "metformin"
    assert payload.type == "nhs_pages"
    assert len(payload.pages) == 1


def test_nhs_pages_payload_wrong_type_raises():
    bad = {**VALID_NHS_PAYLOAD, "type": "nhsbsa_epd"}
    with pytest.raises(ValidationError):
        NHSPagesPayload.model_validate(bad)


def test_nhs_pages_payload_empty_pages_is_valid():
    """Empty pages list is valid — all NHS URLs may 404 for an obscure drug."""
    payload = NHSPagesPayload.model_validate({**VALID_NHS_PAYLOAD, "pages": []})
    assert payload.pages == []


def test_nhs_pages_payload_model_dump_round_trips():
    payload = NHSPagesPayload.model_validate(VALID_NHS_PAYLOAD)
    dumped = payload.model_dump()
    assert dumped["type"] == "nhs_pages"
    assert dumped["pages"][0]["page_type"] == "side_effects"


# ---------------------------------------------------------------------------
# SilverDQViolation
# ---------------------------------------------------------------------------


def test_silver_dq_violation_carries_violations():
    violations = {"actual_cost": {"null_pct": 7.2, "threshold": 5.0}}
    exc = SilverDQViolation(violations)
    assert exc.violations == violations


def test_silver_dq_violation_message_contains_field():
    violations = {"items": {"null_pct": 4.5, "threshold": 3.0}}
    exc = SilverDQViolation(violations)
    assert "items" in str(exc)
    assert "4.50" in str(exc)
    assert "3.0" in str(exc)


def test_silver_dq_violation_is_exception():
    exc = SilverDQViolation({"actual_cost": {"null_pct": 6.0, "threshold": 5.0}})
    assert isinstance(exc, Exception)


def test_silver_dq_violation_multiple_fields():
    violations = {
        "actual_cost": {"null_pct": 6.0, "threshold": 5.0},
        "items": {"null_pct": 4.0, "threshold": 3.0},
    }
    exc = SilverDQViolation(violations)
    msg = str(exc)
    assert "actual_cost" in msg
    assert "items" in msg


# ---------------------------------------------------------------------------
# DEFAULT_NULL_THRESHOLDS
# ---------------------------------------------------------------------------


def test_default_thresholds_cover_critical_fields():
    for field in ("actual_cost", "items", "setting"):
        assert field in DEFAULT_NULL_THRESHOLDS


def test_default_thresholds_are_positive():
    assert all(v > 0 for v in DEFAULT_NULL_THRESHOLDS.values())


# ---------------------------------------------------------------------------
# validate_silver_task integration (using DuckDB in-memory)
# ---------------------------------------------------------------------------


def _build_silver_table(db_path: pathlib.Path, rows: list[dict]) -> None:
    """Helper: create silver.prescribing with synthetic rows."""
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("""
            CREATE OR REPLACE TABLE silver.prescribing (
                date DATE,
                actual_cost DOUBLE,
                items BIGINT,
                quantity DOUBLE,
                row_id VARCHAR,
                setting VARCHAR,
                ccg VARCHAR,
                drug VARCHAR,
                nic_per_item DOUBLE,
                year_month VARCHAR,
                ingested_at TIMESTAMP
            )
        """)
        for row in rows:
            con.execute(
                "INSERT INTO silver.prescribing VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())",
                [
                    row.get("date"),
                    row.get("actual_cost"),
                    row.get("items"),
                    row.get("quantity"),
                    row.get("row_id", ""),
                    row.get("setting", ""),
                    row.get("ccg", ""),
                    row.get("drug", ""),
                    row.get("nic_per_item"),
                    row.get("year_month", ""),
                ],
            )


def test_validate_silver_passes_clean_data(tmp_path):
    """All-good data should pass without raising."""
    from flows.pipeline_flow import validate_silver_task

    db = tmp_path / "test.duckdb"
    rows = [
        {"date": "2024-01-01", "actual_cost": 100.0, "items": 10, "quantity": 500.0,
         "row_id": "A001", "setting": "4", "ccg": "03V", "drug": "metformin",
         "nic_per_item": 10.0, "year_month": "2024-01"},
    ]
    _build_silver_table(db, rows)
    # Should not raise
    result = validate_silver_task.fn(db)
    assert isinstance(result, dict)
    assert all(v == 0.0 for v in result.values())


def test_validate_silver_raises_on_high_null_rate(tmp_path):
    """When null rate exceeds threshold, SilverDQViolation must be raised."""
    from flows.pipeline_flow import validate_silver_task

    db = tmp_path / "test.duckdb"
    # 3 out of 4 rows have actual_cost=NULL → 75% > 5% threshold
    rows = [
        {"date": "2024-01-01", "actual_cost": None, "items": 10, "setting": "4",
         "row_id": f"A{i:03d}", "drug": "metformin", "year_month": "2024-01"}
        for i in range(3)
    ] + [
        {"date": "2024-01-01", "actual_cost": 100.0, "items": 10, "setting": "4",
         "row_id": "A003", "drug": "metformin", "year_month": "2024-01"}
    ]
    _build_silver_table(db, rows)

    with pytest.raises(SilverDQViolation) as exc_info:
        validate_silver_task.fn(db)

    assert "actual_cost" in exc_info.value.violations


def test_validate_silver_custom_threshold(tmp_path):
    """A strict threshold of 0% should fail even a single null."""
    from flows.pipeline_flow import validate_silver_task

    db = tmp_path / "test.duckdb"
    rows = [
        {"date": "2024-01-01", "actual_cost": None, "items": 5, "setting": "4",
         "row_id": "A001", "drug": "metformin", "year_month": "2024-01"},
    ]
    _build_silver_table(db, rows)

    with pytest.raises(SilverDQViolation):
        validate_silver_task.fn(db, thresholds={"actual_cost": 0.0})


def test_validate_silver_empty_table_skips_check(tmp_path):
    """An empty Silver table should skip the DQ gate, not raise."""
    from flows.pipeline_flow import validate_silver_task

    db = tmp_path / "test.duckdb"
    _build_silver_table(db, [])
    result = validate_silver_task.fn(db)
    assert result == {}
