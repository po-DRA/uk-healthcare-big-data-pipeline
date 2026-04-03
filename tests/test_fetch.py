"""
tests/test_fetch.py — unit tests for pipeline/fetch.py.

All HTTP calls are mocked so these tests run offline and fast.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipeline.fetch import fetch_nhs_pages, fetch_openprescribing

# ---------------------------------------------------------------------------
# Canned fixtures
# ---------------------------------------------------------------------------

CANNED_PRESCRIBING = [
    {
        "date": "2024-01-01",
        "actual_cost": 1234.56,
        "items": 100,
        "quantity": 5000.0,
        "row_id": "A81001-2024-01-01",
        "setting": 4,
        "pct_id": "03V",
    },
    {
        "date": "2024-02-01",
        "actual_cost": 987.65,
        "items": 80,
        "quantity": 4000.0,
        "row_id": "A81001-2024-02-01",
        "setting": 4,
        "pct_id": "03V",
    },
]

CANNED_NHS_HTML = """
<html><body><main>
  <h2>Common side effects</h2>
  <p>Nausea and vomiting are common when you first start taking this medicine.</p>
  <ul>
    <li>Feeling sick</li>
    <li>Diarrhoea</li>
  </ul>
  <h2>Serious side effects</h2>
  <p>Contact your doctor immediately if you experience chest pain.</p>
</main></body></html>
"""


# ---------------------------------------------------------------------------
# fetch_openprescribing tests
# ---------------------------------------------------------------------------


def test_fetch_openprescribing_returns_correct_keys():
    """Return dict must contain all required top-level keys."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = CANNED_PRESCRIBING

    with patch("pipeline.fetch.httpx.get", return_value=mock_response):
        result = fetch_openprescribing("0601023A0", "metformin")

    assert set(result.keys()) == {"drug", "bnf_code", "type", "total_rows", "records"}


def test_fetch_openprescribing_type_value():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = CANNED_PRESCRIBING

    with patch("pipeline.fetch.httpx.get", return_value=mock_response):
        result = fetch_openprescribing("0601023A0", "metformin")

    assert result["type"] == "openprescribing"


def test_fetch_openprescribing_total_rows_is_int():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = CANNED_PRESCRIBING

    with patch("pipeline.fetch.httpx.get", return_value=mock_response):
        result = fetch_openprescribing("0601023A0", "metformin")

    assert isinstance(result["total_rows"], int)
    assert result["total_rows"] == 2


def test_fetch_openprescribing_record_keys():
    """Each record must contain the required fields."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = CANNED_PRESCRIBING

    with patch("pipeline.fetch.httpx.get", return_value=mock_response):
        result = fetch_openprescribing("0601023A0", "metformin")

    required = {"date", "actual_cost", "items", "quantity", "row_id", "setting", "ccg", "drug"}
    for record in result["records"]:
        assert required.issubset(set(record.keys()))


def test_fetch_openprescribing_drug_name_propagated():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = CANNED_PRESCRIBING

    with patch("pipeline.fetch.httpx.get", return_value=mock_response):
        result = fetch_openprescribing("0212000B0", "atorvastatin")

    assert result["drug"] == "atorvastatin"
    assert all(r["drug"] == "atorvastatin" for r in result["records"])


# ---------------------------------------------------------------------------
# fetch_nhs_pages tests
# ---------------------------------------------------------------------------


def _make_nhs_mock(html: str, status_code: int = 200) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.text = html
    if status_code != 200:
        from httpx import HTTPStatusError, Request, Response
        mock.raise_for_status.side_effect = HTTPStatusError(
            "error", request=MagicMock(), response=MagicMock(status_code=status_code)
        )
    else:
        mock.raise_for_status.return_value = None
    return mock


def test_fetch_nhs_pages_returns_correct_keys():
    with patch("pipeline.fetch.httpx.get", return_value=_make_nhs_mock(CANNED_NHS_HTML)):
        result = fetch_nhs_pages("metformin")

    assert set(result.keys()) == {"drug", "type", "pages"}


def test_fetch_nhs_pages_type_value():
    with patch("pipeline.fetch.httpx.get", return_value=_make_nhs_mock(CANNED_NHS_HTML)):
        result = fetch_nhs_pages("metformin")

    assert result["type"] == "nhs_pages"


def test_fetch_nhs_pages_pages_list_non_empty():
    with patch("pipeline.fetch.httpx.get", return_value=_make_nhs_mock(CANNED_NHS_HTML)):
        result = fetch_nhs_pages("metformin")

    assert isinstance(result["pages"], list)
    assert len(result["pages"]) > 0


def test_fetch_nhs_pages_each_page_has_required_keys():
    with patch("pipeline.fetch.httpx.get", return_value=_make_nhs_mock(CANNED_NHS_HTML)):
        result = fetch_nhs_pages("metformin")

    required = {"url", "page_type", "heading", "text", "bullets"}
    for page in result["pages"]:
        assert required.issubset(set(page.keys()))


def test_fetch_nhs_pages_headings_extracted():
    with patch("pipeline.fetch.httpx.get", return_value=_make_nhs_mock(CANNED_NHS_HTML)):
        result = fetch_nhs_pages("metformin")

    headings = [p["heading"] for p in result["pages"]]
    assert "Common side effects" in headings


def test_fetch_nhs_pages_404_skipped(caplog):
    """404 responses must be skipped gracefully without raising."""
    import logging

    mock_404 = MagicMock()
    mock_404.status_code = 404

    with patch("pipeline.fetch.httpx.get", return_value=mock_404):
        with caplog.at_level(logging.WARNING, logger="pipeline.fetch"):
            result = fetch_nhs_pages("metformin")

    # Should not raise; pages may be empty if all 3 return 404
    assert isinstance(result["pages"], list)
    assert any("404" in record.message for record in caplog.records)


def test_fetch_nhs_pages_invalid_drug_name_raises():
    """drug_name containing digits or spaces must raise ValueError (line 166)."""
    with pytest.raises(ValueError, match="Invalid drug_name"):
        fetch_nhs_pages("metformin2")


def test_fetch_nhs_pages_http_status_error_skipped(caplog):
    """Non-404 HTTPStatusError must be caught and page skipped (lines 192-194)."""
    import logging

    from httpx import HTTPStatusError

    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.raise_for_status.side_effect = HTTPStatusError(
        "server error", request=MagicMock(), response=MagicMock(status_code=500)
    )

    with patch("pipeline.fetch.httpx.get", return_value=mock_resp):
        with caplog.at_level(logging.WARNING, logger="pipeline.fetch"):
            result = fetch_nhs_pages("metformin")

    assert isinstance(result["pages"], list)
    assert any("500" in record.message for record in caplog.records)


def test_fetch_nhs_pages_request_error_skipped(caplog):
    """Network-level RequestError must be caught and page skipped (lines 195-197)."""
    import logging

    from httpx import RequestError

    with patch(
        "pipeline.fetch.httpx.get",
        side_effect=RequestError("connection refused"),
    ):
        with caplog.at_level(logging.WARNING, logger="pipeline.fetch"):
            result = fetch_nhs_pages("metformin")

    assert isinstance(result["pages"], list)
    assert any("connection refused" in record.message for record in caplog.records)


def test_fetch_nhs_pages_no_headings_fallback():
    """HTML with no h2/h3 headings uses the paragraph fallback (lines 206-208)."""
    html_no_headings = """
    <html><body><main>
      <p>This is some text without any headings.</p>
      <ul><li>A bullet point</li></ul>
    </main></body></html>
    """
    with patch("pipeline.fetch.httpx.get", return_value=_make_nhs_mock(html_no_headings)):
        result = fetch_nhs_pages("metformin")

    assert len(result["pages"]) > 0
    # Fallback sets heading to empty string
    assert any(p["heading"] == "" for p in result["pages"])
