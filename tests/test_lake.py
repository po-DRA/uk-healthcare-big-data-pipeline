"""
tests/test_lake.py — unit tests for pipeline/lake.py.

All tests use tmp_path (pytest fixture) so nothing is written to the real lake.
"""

from __future__ import annotations

import json

import pytest

from pipeline.lake import lake_summary, read_lake, write_lake

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PRESCRIBING_PAYLOAD = {
    "drug": "metformin",
    "bnf_code": "0601022B0",
    "type": "nhsbsa_epd",
    "total_rows": 2,
    "records": [
        {"date": "2024-01-01", "actual_cost": 100.0, "items": 10, "quantity": 500.0,
         "row_id": "A81001-2024-01-01", "setting": 4, "ccg": "03V", "drug": "metformin"},
        {"date": "2024-02-01", "actual_cost": 200.0, "items": 20, "quantity": 1000.0,
         "row_id": "A81001-2024-02-01", "setting": 4, "ccg": "03V", "drug": "metformin"},
    ],
}

NHS_PAGES_PAYLOAD = {
    "drug": "metformin",
    "type": "nhs_pages",
    "pages": [
        {"url": "https://example.com", "page_type": "side_effects",
         "heading": "Common side effects", "text": "Nausea.", "bullets": ["Feeling sick"]},
    ],
}


# ---------------------------------------------------------------------------
# write_lake — nhsbsa_epd
# ---------------------------------------------------------------------------


def test_write_lake_prescribing_returns_path(tmp_path):
    out = write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    assert out == tmp_path / "metformin" / "prescribing.jsonl"


def test_write_lake_prescribing_file_exists(tmp_path):
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    assert (tmp_path / "metformin" / "prescribing.jsonl").exists()


def test_write_lake_prescribing_correct_line_count(tmp_path):
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    lines = (tmp_path / "metformin" / "prescribing.jsonl").read_text().splitlines()
    assert len(lines) == 2


def test_write_lake_prescribing_lines_are_valid_json(tmp_path):
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    lines = (tmp_path / "metformin" / "prescribing.jsonl").read_text().splitlines()
    for line in lines:
        record = json.loads(line)
        assert "date" in record


def test_write_lake_prescribing_no_tmp_file_left(tmp_path):
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    tmp_files = list((tmp_path / "metformin").glob("*.tmp"))
    assert tmp_files == []


def test_write_lake_prescribing_exception_cleans_tmp(tmp_path):
    """If writing raises mid-write, the .tmp file must be removed."""
    bad_payload = {
        "drug": "metformin",
        "type": "nhsbsa_epd",
        # sets are not JSON-serialisable — triggers TypeError inside the with block
        "records": [{"date": "2024-01-01", "bad_value": {1, 2, 3}}],
    }
    with pytest.raises(TypeError):
        write_lake(bad_payload, tmp_path)
    tmp_files = list(tmp_path.rglob("*.tmp"))
    assert tmp_files == []


# ---------------------------------------------------------------------------
# write_lake — nhs_pages
# ---------------------------------------------------------------------------


def test_write_lake_nhs_pages_returns_path(tmp_path):
    out = write_lake(NHS_PAGES_PAYLOAD, tmp_path)
    assert out == tmp_path / "metformin" / "nhs_pages.json"


def test_write_lake_nhs_pages_file_exists(tmp_path):
    write_lake(NHS_PAGES_PAYLOAD, tmp_path)
    assert (tmp_path / "metformin" / "nhs_pages.json").exists()


def test_write_lake_nhs_pages_valid_json(tmp_path):
    write_lake(NHS_PAGES_PAYLOAD, tmp_path)
    content = json.loads((tmp_path / "metformin" / "nhs_pages.json").read_text())
    assert content["drug"] == "metformin"
    assert content["type"] == "nhs_pages"
    assert isinstance(content["pages"], list)


def test_write_lake_nhs_pages_no_tmp_file_left(tmp_path):
    write_lake(NHS_PAGES_PAYLOAD, tmp_path)
    tmp_files = list((tmp_path / "metformin").glob("*.tmp"))
    assert tmp_files == []


# ---------------------------------------------------------------------------
# write_lake — unknown type
# ---------------------------------------------------------------------------


def test_write_lake_nhs_pages_exception_cleans_tmp(tmp_path):
    """If writing nhs_pages raises mid-write, the .tmp file must be removed."""
    bad_payload = {
        "drug": "metformin",
        "type": "nhs_pages",
        # sets are not JSON-serialisable — triggers TypeError inside json.dump
        "pages": [{1, 2, 3}],
    }
    with pytest.raises(TypeError):
        write_lake(bad_payload, tmp_path)
    tmp_files = list(tmp_path.rglob("*.tmp"))
    assert tmp_files == []


def test_write_lake_unknown_type_raises(tmp_path):
    payload = {"drug": "metformin", "type": "unknown"}
    with pytest.raises(ValueError, match="Unknown payload type"):
        write_lake(payload, tmp_path)


# ---------------------------------------------------------------------------
# read_lake — nhsbsa_epd
# ---------------------------------------------------------------------------


def test_read_lake_prescribing_round_trips(tmp_path):
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    records = read_lake("metformin", "nhsbsa_epd", tmp_path)
    assert len(records) == 2
    assert records[0]["date"] == "2024-01-01"
    assert records[1]["date"] == "2024-02-01"


def test_read_lake_prescribing_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_lake("nonexistent", "nhsbsa_epd", tmp_path)


# ---------------------------------------------------------------------------
# read_lake — nhs_pages
# ---------------------------------------------------------------------------


def test_read_lake_nhs_pages_round_trips(tmp_path):
    write_lake(NHS_PAGES_PAYLOAD, tmp_path)
    pages = read_lake("metformin", "nhs_pages", tmp_path)
    assert len(pages) == 1
    assert pages[0]["heading"] == "Common side effects"


def test_read_lake_nhs_pages_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_lake("nonexistent", "nhs_pages", tmp_path)


# ---------------------------------------------------------------------------
# read_lake — unknown type
# ---------------------------------------------------------------------------


def test_read_lake_unknown_type_raises(tmp_path):
    with pytest.raises(ValueError, match="Unknown data_type"):
        read_lake("metformin", "unknown", tmp_path)


# ---------------------------------------------------------------------------
# lake_summary
# ---------------------------------------------------------------------------


def test_lake_summary_empty_when_base_missing(tmp_path):
    summary = lake_summary(tmp_path / "does_not_exist")
    assert summary == []


def test_lake_summary_returns_entries_for_each_file(tmp_path):
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    write_lake(NHS_PAGES_PAYLOAD, tmp_path)
    summary = lake_summary(tmp_path)
    filenames = [s["file"] for s in summary]
    assert "prescribing.jsonl" in filenames
    assert "nhs_pages.json" in filenames


def test_lake_summary_has_required_keys(tmp_path):
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    summary = lake_summary(tmp_path)
    for entry in summary:
        assert {"drug", "file", "size_bytes", "size_kb"} == set(entry.keys())


def test_lake_summary_size_bytes_positive(tmp_path):
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    summary = lake_summary(tmp_path)
    assert all(s["size_bytes"] > 0 for s in summary)


def test_lake_summary_skips_non_directory_entries(tmp_path):
    """A stray file at the base_dir level must not cause an error (line 169)."""
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    # Place a regular file alongside the drug directories
    (tmp_path / "README.txt").write_text("stray file")
    summary = lake_summary(tmp_path)
    # Only the drug dir entries should appear, not the stray file
    assert all(s["drug"] != "README.txt" for s in summary)


def test_lake_summary_multiple_drugs(tmp_path):
    write_lake(PRESCRIBING_PAYLOAD, tmp_path)
    other = {**NHS_PAGES_PAYLOAD, "drug": "atorvastatin"}
    write_lake(other, tmp_path)
    summary = lake_summary(tmp_path)
    drugs = {s["drug"] for s in summary}
    assert "metformin" in drugs
    assert "atorvastatin" in drugs
