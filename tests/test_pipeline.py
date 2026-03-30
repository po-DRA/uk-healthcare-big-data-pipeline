"""
tests/test_pipeline.py — slow integration tests that make live API calls.

These tests are marked with @pytest.mark.slow and are excluded from CI.
Run manually to verify end-to-end pipeline connectivity:

    uv run pytest tests/test_pipeline.py -v -m slow

They require internet access and will hit the real OpenPrescribing API.
"""

from __future__ import annotations

import pytest

from pipeline.fetch import DRUG_CODES, fetch_openprescribing


@pytest.mark.slow
def test_live_fetch_openprescribing_metformin():
    """Live call: metformin records must be non-empty with expected keys."""
    result = fetch_openprescribing(DRUG_CODES["metformin"], "metformin")

    assert result["drug"] == "metformin"
    assert result["type"] == "openprescribing"
    assert isinstance(result["total_rows"], int)
    assert result["total_rows"] > 0, "Expected at least one prescribing record"

    first = result["records"][0]
    assert "actual_cost" in first
    assert "items" in first
    assert "date" in first
    assert "quantity" in first


@pytest.mark.slow
def test_live_fetch_openprescribing_atorvastatin():
    """Live call: atorvastatin records must be non-empty."""
    result = fetch_openprescribing(DRUG_CODES["atorvastatin"], "atorvastatin")
    assert result["total_rows"] > 0


@pytest.mark.slow
def test_live_fetch_openprescribing_returns_date_strings():
    """Dates in live data should look like YYYY-MM-DD."""
    result = fetch_openprescribing(DRUG_CODES["metformin"], "metformin")
    for record in result["records"][:10]:
        date = record.get("date", "")
        assert len(date) == 10, f"Expected YYYY-MM-DD, got {date!r}"
        assert date[4] == "-" and date[7] == "-"
