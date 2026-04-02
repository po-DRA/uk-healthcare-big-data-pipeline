"""
pipeline/nlp.py — lightweight NLP on NHS.uk clinical prose.

Demonstrates **Variety** — converting unstructured HTML-extracted text
into a structured term-frequency DataFrame using only the Python standard
library (re, collections.Counter) and Polars.

No external NLP models required; this is intentionally simple to keep
the focus on the pipeline architecture, not on ML complexity.
"""

from __future__ import annotations

import json
import pathlib
import re
from collections import Counter

import polars as pl

# ---------------------------------------------------------------------------
# Stop words — medical non-signal words excluded from term counts
# ---------------------------------------------------------------------------
STOP_WORDS: set[str] = {
    "with",
    "the",
    "and",
    "for",
    "from",
    "this",
    "that",
    "your",
    "have",
    "been",
    "which",
    "drug",
    "take",
    "taking",
    "medicine",
    "medicines",
    "doctor",
    "pharmacist",
    "nhs",
    "more",
    "also",
    "some",
    "other",
    "tell",
    "used",
    "may",
    "will",
    "does",
    "when",
    "they",
    "what",
    "about",
    # additional common English stop words
    "than",
    "then",
    "there",
    "these",
    "those",
    "such",
    "into",
    "over",
    "after",
    "before",
    "because",
    "should",
    "could",
    "would",
    "their",
    "them",
    "here",
    "very",
    "just",
    "like",
    "make",
    "most",
    "only",
    "same",
    "each",
    "well",
    "even",
    "much",
    "many",
    "know",
    "need",
    "sure",
    "feel",
    "stop",
    "start",
    "keep",
    "body",
    "time",
    "days",
    "week",
    "weeks",
    "include",
    "including",
    "contact",
    "call",
    "seek",
    "help",
    "away",
    "side",
    "effects",
    "effect",
}


def extract_sections(nhs_json_path: pathlib.Path) -> list[dict]:
    """Parse a saved NHS pages JSON file into a flat list of sections.

    Demonstrates **Variety** — transforming the raw nested JSON structure
    into a normalised list suitable for NLP processing.

    Parameters
    ----------
    nhs_json_path:
        Path to a ``nhs_pages.json`` file written by ``write_lake()``,
        e.g. ``lake/metformin/nhs_pages.json``.

    Returns
    -------
    list[dict]
        Each dict has keys:
        - ``drug``      : str
        - ``page_type`` : str — "side_effects", "contraindications", or "interactions"
        - ``heading``   : str
        - ``text``      : str — paragraph text joined
        - ``bullets``   : list[str]
    """
    with nhs_json_path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)

    drug = payload.get("drug", nhs_json_path.parent.name)
    sections = []

    for page in payload.get("pages", []):
        sections.append(
            {
                "drug": drug,
                "page_type": page.get("page_type", ""),
                "heading": page.get("heading", ""),
                "text": page.get("text", ""),
                "bullets": page.get("bullets", []),
            }
        )

    return sections


def tokenise(text: str, stop_words: set[str]) -> list[str]:
    """Tokenise clinical text into lowercase alphabetic tokens.

    Demonstrates **Variety** — extracting signal from unstructured prose
    using a minimal regex approach (no external NLP libraries).

    Parameters
    ----------
    text:
        Raw clinical text string.
    stop_words:
        Set of tokens to exclude (case-insensitive; applied after lowercasing).

    Returns
    -------
    list[str]
        Lowercase alphabetic tokens of length ≥ 4 that are not stop words.
    """
    tokens = re.findall(r"[a-z]{4,}", text.lower())
    return [t for t in tokens if t not in stop_words]


def top_terms(
    drug: str,
    lake_dir: pathlib.Path,
    n: int = 20,
) -> pl.DataFrame:
    """Compute the top N terms across all NHS pages for a drug.

    Aggregates term frequencies across all three page types (side effects,
    contraindications, interactions) and returns a ranked Polars DataFrame.

    Parameters
    ----------
    drug:
        Drug name, e.g. ``"metformin"``.
    lake_dir:
        Root of the data lake.
    n:
        Number of top terms to return per page_type combination.

    Returns
    -------
    pl.DataFrame
        Columns: drug (String), term (String), frequency (Int64),
        page_type (String). Sorted by frequency descending.
    """
    nhs_json_path = lake_dir / drug / "nhs_pages.json"
    sections = extract_sections(nhs_json_path)

    rows: list[dict] = []
    for section in sections:
        page_type = section["page_type"]
        # Combine heading, paragraph text, and bullets into one text blob
        full_text = " ".join([section["heading"], section["text"]] + section["bullets"])
        tokens = tokenise(full_text, STOP_WORDS)
        for term, freq in Counter(tokens).items():
            rows.append(
                {
                    "drug": drug,
                    "term": term,
                    "frequency": freq,
                    "page_type": page_type,
                }
            )

    if not rows:
        return pl.DataFrame(
            schema={
                "drug": pl.String,
                "term": pl.String,
                "frequency": pl.Int64,
                "page_type": pl.String,
            }
        )

    return (
        pl.DataFrame(
            rows,
            schema={
                "drug": pl.String,
                "term": pl.String,
                "frequency": pl.Int64,
                "page_type": pl.String,
            },
        )
        .group_by(["drug", "term", "page_type"])
        .agg(pl.col("frequency").sum())
        .sort("frequency", descending=True)
        .head(n)
    )
