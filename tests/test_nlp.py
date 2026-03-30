"""
tests/test_nlp.py — unit tests for pipeline/nlp.py.

Tests tokenise() with exact word-level assertions, and extract_sections()
with a minimal synthetic NHS JSON fixture.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from pipeline.nlp import STOP_WORDS, extract_sections, tokenise, top_terms

# ---------------------------------------------------------------------------
# tokenise tests
# ---------------------------------------------------------------------------


def test_tokenise_includes_clinical_signal_words():
    text = "The patient felt nausea and vomiting after taking the medicine"
    tokens = tokenise(text, STOP_WORDS)
    assert "nausea" in tokens
    assert "vomiting" in tokens


def test_tokenise_excludes_stop_words():
    text = "The patient felt nausea and vomiting after taking the medicine"
    tokens = tokenise(text, STOP_WORDS)
    assert "medicine" not in tokens
    assert "taking" not in tokens


def test_tokenise_all_lowercase():
    text = "Nausea Vomiting Dizziness HEADACHE"
    tokens = tokenise(text, STOP_WORDS)
    for t in tokens:
        assert t == t.lower()


def test_tokenise_minimum_length_four():
    text = "a ab abc abcd abcde"
    tokens = tokenise(text, STOP_WORDS)
    for t in tokens:
        assert len(t) >= 4


def test_tokenise_no_numbers_or_punctuation():
    text = "Take 2 tablets, twice-daily! (morning & evening)"
    tokens = tokenise(text, STOP_WORDS)
    for t in tokens:
        assert t.isalpha()


def test_tokenise_returns_list():
    tokens = tokenise("headache nausea dizziness", STOP_WORDS)
    assert isinstance(tokens, list)


def test_tokenise_empty_string():
    tokens = tokenise("", STOP_WORDS)
    assert tokens == []


def test_tokenise_only_stop_words():
    text = "with the and for from this that your have been"
    tokens = tokenise(text, STOP_WORDS)
    assert tokens == []


def test_tokenise_custom_stop_words():
    custom_stops = {"headache"}
    tokens = tokenise("headache nausea vomiting", custom_stops)
    assert "headache" not in tokens
    assert "nausea" in tokens


# ---------------------------------------------------------------------------
# extract_sections tests
# ---------------------------------------------------------------------------

SYNTHETIC_NHS_PAYLOAD = {
    "drug": "metformin",
    "type": "nhs_pages",
    "pages": [
        {
            "url": "https://www.nhs.uk/medicines/metformin/side-effects-of-metformin/",
            "page_type": "side_effects",
            "heading": "Common side effects",
            "text": "Nausea and vomiting are common when you first start taking this medicine.",
            "bullets": ["Feeling sick", "Diarrhoea", "Stomach pain"],
        },
        {
            "url": "https://www.nhs.uk/medicines/metformin/who-can-and-cannot-take-metformin/",
            "page_type": "contraindications",
            "heading": "Who cannot take metformin",
            "text": "Metformin is not suitable for people with kidney disease.",
            "bullets": ["Kidney failure", "Liver disease"],
        },
    ],
}


@pytest.fixture()
def nhs_json_file(tmp_path: pathlib.Path) -> pathlib.Path:
    drug_dir = tmp_path / "metformin"
    drug_dir.mkdir()
    path = drug_dir / "nhs_pages.json"
    with path.open("w") as fh:
        json.dump(SYNTHETIC_NHS_PAYLOAD, fh)
    return path


def test_extract_sections_returns_list(nhs_json_file):
    sections = extract_sections(nhs_json_file)
    assert isinstance(sections, list)


def test_extract_sections_correct_count(nhs_json_file):
    sections = extract_sections(nhs_json_file)
    assert len(sections) == 2


def test_extract_sections_has_required_keys(nhs_json_file):
    sections = extract_sections(nhs_json_file)
    required = {"drug", "page_type", "heading", "text", "bullets"}
    for section in sections:
        assert required.issubset(set(section.keys()))


def test_extract_sections_drug_name(nhs_json_file):
    sections = extract_sections(nhs_json_file)
    assert all(s["drug"] == "metformin" for s in sections)


def test_extract_sections_page_types(nhs_json_file):
    sections = extract_sections(nhs_json_file)
    page_types = {s["page_type"] for s in sections}
    assert "side_effects" in page_types
    assert "contraindications" in page_types


def test_extract_sections_bullets_is_list(nhs_json_file):
    sections = extract_sections(nhs_json_file)
    for section in sections:
        assert isinstance(section["bullets"], list)


# ---------------------------------------------------------------------------
# top_terms tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def lake_dir_with_nhs(tmp_path: pathlib.Path) -> pathlib.Path:
    drug_dir = tmp_path / "metformin"
    drug_dir.mkdir()
    path = drug_dir / "nhs_pages.json"
    with path.open("w") as fh:
        json.dump(SYNTHETIC_NHS_PAYLOAD, fh)
    return tmp_path


def test_top_terms_returns_dataframe(lake_dir_with_nhs):
    result = top_terms("metformin", lake_dir_with_nhs)
    assert isinstance(result, pl.DataFrame)


def test_top_terms_has_required_columns(lake_dir_with_nhs):
    result = top_terms("metformin", lake_dir_with_nhs)
    assert set(result.columns) == {"drug", "term", "frequency", "page_type"}


def test_top_terms_drug_column_value(lake_dir_with_nhs):
    result = top_terms("metformin", lake_dir_with_nhs)
    if len(result) > 0:
        assert all(result["drug"] == "metformin")


def test_top_terms_frequency_is_positive(lake_dir_with_nhs):
    result = top_terms("metformin", lake_dir_with_nhs)
    if len(result) > 0:
        assert (result["frequency"] > 0).all()
