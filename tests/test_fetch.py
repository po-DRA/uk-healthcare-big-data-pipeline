"""
tests/test_fetch.py — unit tests for pipeline/fetch.py.

All HTTP calls are mocked so these tests run offline and fast.
"""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock, patch

import pytest

from pipeline.fetch import fetch_nhsbsa, fetch_nhs_pages

# ---------------------------------------------------------------------------
# Canned fixtures — NHSBSA EPD CSV format
# ---------------------------------------------------------------------------

# Minimal CSV content matching the real EPD schema
CANNED_CSV_HEADER = (
    "YEAR_MONTH,REGIONAL_OFFICE_NAME,REGIONAL_OFFICE_CODE,ICB_NAME,ICB_CODE,"
    "PCO_NAME,PCO_CODE,PRACTICE_NAME,PRACTICE_CODE,ADDRESS_1,ADDRESS_2,"
    "ADDRESS_3,ADDRESS_4,POSTCODE,BNF_CHEMICAL_SUBSTANCE,CHEMICAL_SUBSTANCE_BNF_DESCR,"
    "BNF_CODE,BNF_DESCRIPTION,BNF_CHAPTER_PLUS_CODE,QUANTITY,ITEMS,TOTAL_QUANTITY,"
    "ADQUSAGE,NIC,ACTUAL_COST,UNIDENTIFIED\n"
)

CANNED_CSV_ROW = (
    '202506,"NORTH EAST","Y63","NHS NORTH EAST ICB","QHM","NHS NORTH EAST","QHM",'
    '"TEST PRACTICE","A81001","1 HIGH STREET","","","","NE1 1AA",'
    '"0601022B0","Metformin hydrochloride",'
    '"0601022B0AAAAAA","Metformin 500mg tablets","06: Endocrine System",'
    "60.0,5,60.0,0.0,2.5,2.09144,false\n"
)

CANNED_PACKAGE = {
    "result": {
        "resources": [
            {"name": "EPD_202506", "url": "https://opendata.nhsbsa.net/fake/EPD_202506.csv"},
        ]
    }
}

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


def _make_csv_urlopen(rows: int = 3):
    """Return a mock for urllib.request.urlopen that serves CSV data."""
    csv_content = CANNED_CSV_HEADER + (CANNED_CSV_ROW * rows)

    def fake_urlopen(req_or_url, timeout=None):
        url = req_or_url if isinstance(req_or_url, str) else req_or_url.full_url
        if "package_show" in url:
            mock = MagicMock()
            mock.__enter__ = lambda s: s
            mock.__exit__ = MagicMock(return_value=False)
            mock.read.return_value = json.dumps(CANNED_PACKAGE).encode()
            return mock
        else:
            mock = MagicMock()
            mock.__enter__ = lambda s: s
            mock.__exit__ = MagicMock(return_value=False)
            data = csv_content.encode("utf-8")
            mock.read.side_effect = [data, b""]
            return mock

    return fake_urlopen


# ---------------------------------------------------------------------------
# fetch_nhsbsa tests
# ---------------------------------------------------------------------------


def test_fetch_nhsbsa_returns_correct_keys():
    """Return dict must contain all required top-level keys."""
    with patch("pipeline.fetch.urllib.request.urlopen", side_effect=_make_csv_urlopen(3)):
        result = fetch_nhsbsa("0601022B0", "metformin")

    assert set(result.keys()) == {"drug", "bnf_code", "source", "resource", "total_rows", "records"}


def test_fetch_nhsbsa_source_value():
    with patch("pipeline.fetch.urllib.request.urlopen", side_effect=_make_csv_urlopen(3)):
        result = fetch_nhsbsa("0601022B0", "metformin")

    assert result["source"] == "nhsbsa_epd"


def test_fetch_nhsbsa_total_rows_is_int():
    with patch("pipeline.fetch.urllib.request.urlopen", side_effect=_make_csv_urlopen(3)):
        result = fetch_nhsbsa("0601022B0", "metformin")

    assert isinstance(result["total_rows"], int)


def test_fetch_nhsbsa_record_keys():
    """Each record must contain the required fields."""
    with patch("pipeline.fetch.urllib.request.urlopen", side_effect=_make_csv_urlopen(3)):
        result = fetch_nhsbsa("0601022B0", "metformin")

    required = {"date", "actual_cost", "items", "quantity", "row_id", "setting", "ccg", "drug"}
    for record in result["records"]:
        assert required.issubset(set(record.keys()))


def test_fetch_nhsbsa_drug_name_propagated():
    with patch("pipeline.fetch.urllib.request.urlopen", side_effect=_make_csv_urlopen(3)):
        result = fetch_nhsbsa("0601022B0", "metformin")

    assert result["drug"] == "metformin"
    for rec in result["records"]:
        assert rec["drug"] == "metformin"


def test_fetch_nhsbsa_filters_unidentified_rows():
    """Rows with UNIDENTIFIED=true must be excluded."""
    unidentified_row = CANNED_CSV_ROW.replace(",false\n", ",true\n")
    csv_content = CANNED_CSV_HEADER + unidentified_row

    def fake_urlopen(req_or_url, timeout=None):
        url = req_or_url if isinstance(req_or_url, str) else req_or_url.full_url
        mock = MagicMock()
        mock.__enter__ = lambda s: s
        mock.__exit__ = MagicMock(return_value=False)
        if "package_show" in url:
            mock.read.return_value = json.dumps(CANNED_PACKAGE).encode()
        else:
            mock.read.side_effect = [csv_content.encode(), b""]
        return mock

    with patch("pipeline.fetch.urllib.request.urlopen", side_effect=fake_urlopen):
        result = fetch_nhsbsa("0601022B0", "metformin")

    assert result["total_rows"] == 0
    assert result["records"] == []


# ---------------------------------------------------------------------------
# fetch_nhs_pages tests
# ---------------------------------------------------------------------------


def _make_nhs_mock(html: str, status_code: int = 200) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.text = html
    if status_code != 200:
        from httpx import HTTPStatusError
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

    assert isinstance(result["pages"], list)
    assert any("404" in record.message for record in caplog.records)


def test_fetch_nhs_pages_invalid_drug_name_raises():
    """drug_name containing digits or spaces must raise ValueError."""
    with pytest.raises(ValueError, match="Invalid drug_name"):
        fetch_nhs_pages("metformin2")


def test_fetch_nhs_pages_http_status_error_skipped(caplog):
    """Non-404 HTTPStatusError must be caught and page skipped."""
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
    """Network-level RequestError must be caught and page skipped."""
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
    """HTML with no h2/h3 headings uses the paragraph fallback."""
    html_no_headings = """
    <html><body><main>
      <p>This is some text without any headings.</p>
      <ul><li>A bullet point</li></ul>
    </main></body></html>
    """
    with patch("pipeline.fetch.httpx.get", return_value=_make_nhs_mock(html_no_headings)):
        result = fetch_nhs_pages("metformin")

    assert len(result["pages"]) > 0
    assert any(p["heading"] == "" for p in result["pages"])
