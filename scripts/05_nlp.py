"""
Script 05 — NLP term extraction from NHS.uk clinical prose

What this does
--------------
Reads the NHS.uk pages stored in the Bronze lake and extracts the top
clinical terms for each drug using tokenisation and term-frequency counting.

No external NLP libraries are used — only Python's re module and
collections.Counter.  This is intentionally simple to keep the focus on
the pipeline architecture rather than ML complexity.

Why this matters
----------------
This is the Variety V: the same lake/ directory contains two completely
different data structures:

  prescribing.jsonl  — structured tabular records (rows/columns)
  nhs_pages.json     — unstructured clinical prose (free text)

A pipeline must handle both.  Clinical NLP at scale (named entity
recognition for drugs, dosages, conditions) uses the same tokenise →
count → rank pipeline, but with more sophisticated models (spaCy,
ScispaCy, BioBERT).

Real clinical insight
~~~~~~~~~~~~~~~~~~~~~
The top terms differ meaningfully between drugs:

  metformin    → blood, sugar, diabetes, glucose
  salbutamol   → breathing, inhaler, lung, airways
  atorvastatin → cholesterol, heart, liver, statins
  lisinopril   → blood, pressure, heart, kidney

Run
---
    uv run python scripts/05_nlp.py

Prerequisites
-------------
    Run scripts/01_fetch.py first to populate lake/
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from pipeline.fetch import DRUG_CODES
from pipeline.nlp import top_terms

LAKE_DIR = pathlib.Path("lake")


def main() -> None:
    drugs = list(DRUG_CODES.keys())
    missing = [d for d in drugs if not (LAKE_DIR / d / "nhs_pages.json").exists()]
    if missing:
        print(f"Missing NHS pages for: {missing}")
        print("Run scripts/01_fetch.py first.")
        if len(missing) == len(drugs):
            return

    print("\nTop clinical terms per drug (from NHS.uk pages)\n")

    for drug in drugs:
        nhs_path = LAKE_DIR / drug / "nhs_pages.json"
        if not nhs_path.exists():
            print(f"\n  {drug}: no NHS pages file — skipping")
            continue

        df = top_terms(drug, LAKE_DIR, n=10)
        if df.is_empty():
            print(f"\n  {drug}: no terms extracted (empty pages?)")
            continue

        print(f"\n  {drug.upper()} — top 10 terms:")
        print(f"  {'Term':<20} {'Frequency':>9}  Page type")
        print(f"  {'-'*20} {'-'*9}  {'-'*20}")
        for row in df.iter_rows(named=True):
            print(f"  {row['term']:<20} {row['frequency']:>9}  {row['page_type']}")

    print("\nDone. These term frequencies can be joined to prescribing data")
    print("(see scripts/06_visualise.py) to link clinical language with cost patterns.")


if __name__ == "__main__":
    main()
