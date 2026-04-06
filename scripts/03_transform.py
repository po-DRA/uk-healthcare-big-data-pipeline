"""
Script 03 — Transform with Polars lazy evaluation + veracity report

What this does
--------------
Loads the Bronze prescribing data using Polars lazy evaluation, adds two
derived columns, then runs a veracity report that quantifies data quality.

Polars lazy evaluation
~~~~~~~~~~~~~~~~~~~~~~
``pl.scan_ndjson()`` describes the computation without reading any data yet.
The full query plan is shown before execution.  When ``.collect()`` is called,
Polars executes the plan — applying predicate pushdown and projection pruning
to read only what is needed.

Why this matters
----------------
This is the same principle as Spark's Catalyst optimiser:

  - Polars lazy: scan_ndjson() + filter() → optimised plan → collect()
  - Spark lazy:  textFile() + filter() → DAG → action triggers execution

Understanding lazy evaluation is essential before working with any
distributed framework.  The concept is identical; only the scale differs.

Veracity report
~~~~~~~~~~~~~~~
The report shows null percentages per field.  Decisions documented here:

  actual_cost: nullable — some NHS prescriptions have zero cost (e.g. exempt patients)
  items:       nullable — rare; both NULL means analytically worthless (dropped in Silver)
  setting:     always "4" (primary care) from NHSBSA — should be 0% null

Run
---
    uv run python scripts/03_transform.py

Prerequisites
-------------
    Run scripts/01_fetch.py first to populate lake/
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from pipeline.transform import build_prescribing_df, veracity_report

LAKE_DIR = pathlib.Path("lake")


def main() -> None:
    prescribing_files = list(LAKE_DIR.glob("*/prescribing.jsonl"))
    if not prescribing_files:
        print("No Bronze data found. Run scripts/01_fetch.py first.")
        return

    print("\nBuilding Polars LazyFrame from Bronze lake...")
    lazy = build_prescribing_df(LAKE_DIR)

    print("\nOptimised query plan (Polars decides what to read and when):")
    print("-" * 55)
    print(lazy.explain())
    print("-" * 55)

    print("\nExecuting plan (collect)...")
    df = lazy.collect()
    print(f"Rows collected: {len(df):,}")
    print(f"Columns: {df.columns}")

    print("\nSample rows:")
    print(df.head(5))

    print("\nVeracity report (null analysis per field):")
    print("=" * 55)
    report = veracity_report(df)
    print(report)

    print("\nDerived columns added:")
    print("  nic_per_item = actual_cost / items  (cost efficiency metric)")
    print("  year_month   = first 7 chars of date (for Silver partitioning)")
    print("\nDone. No files written — this is an in-memory transform.")


if __name__ == "__main__":
    main()
