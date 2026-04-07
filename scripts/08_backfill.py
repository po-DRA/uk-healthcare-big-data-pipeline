"""
Script 08 — Partition-level backfill (re-process a date range)

What this does
--------------
Demonstrates backfilling: selectively re-processing a specific date range
in Silver without touching records outside that window.

When does backfill happen?
~~~~~~~~~~~~~~~~~~~~~~~~~~
Three common triggers:

  1. New drug added — need to re-derive Silver for all historical months
     that include that drug's prescribing data.

  2. Bug found in transform logic — e.g. a derived column was calculated
     incorrectly; affected months must be re-derived.

  3. Source data corrected — NHSBSA occasionally revises historical EPD files;
     the affected month must be re-ingested from Bronze and re-transformed.

Full rebuild vs partition backfill
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
At NHS scale, Silver might contain 5+ years × 12 months = 60+ monthly partitions.

  Full rebuild: DELETE all, re-run build_silver() → O(all data) → hours
  Partition backfill: DELETE WHERE year_month BETWEEN ? AND ? → O(target range) → seconds

This script uses build_silver_for_range() which does:
  1. DELETE FROM silver.prescribing WHERE year_month BETWEEN from_month AND to_month
  2. INSERT INTO silver.prescribing ... WHERE year_month BETWEEN from_month AND to_month

Idempotency
~~~~~~~~~~~
Running this script twice gives the same result as running it once.
The DELETE-then-INSERT pattern is idempotent by design.

Run
---
    uv run python scripts/08_backfill.py

    # Override date range
    BACKFILL_FROM=202501 BACKFILL_TO=202503 uv run python scripts/08_backfill.py

Prerequisites
-------------
    Run scripts/04_medallion.py first (Silver must exist before backfill).
"""

from __future__ import annotations

import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

import duckdb

from pipeline.medallion import build_gold, build_silver_for_range

LAKE_DIR = pathlib.Path("lake")
DB_PATH = pathlib.Path("pipeline.duckdb")

# Default: backfill the most recent month in Silver
BACKFILL_FROM = os.environ.get("BACKFILL_FROM", "202506")
BACKFILL_TO = os.environ.get("BACKFILL_TO", "202506")


def main() -> None:
    if not DB_PATH.exists():
        print("Database not found. Run scripts/04_medallion.py first.")
        return

    with duckdb.connect(str(DB_PATH)) as con:
        try:
            before = con.execute(
                "SELECT COUNT(*) FROM silver.prescribing "
                "WHERE year_month BETWEEN ? AND ?",
                [BACKFILL_FROM, BACKFILL_TO],
            ).fetchone()
            total_before = con.execute(
                "SELECT COUNT(*) FROM silver.prescribing"
            ).fetchone()
        except Exception:
            print("Silver table not found. Run scripts/04_medallion.py first.")
            return

    rows_in_window_before = (before or (0,))[0]
    total_before_count = (total_before or (0,))[0]

    print(
        f"\nBackfill: silver.prescribing WHERE year_month BETWEEN {BACKFILL_FROM} AND {BACKFILL_TO}"
    )
    print("\nBefore backfill:")
    print(f"  Total Silver rows:          {total_before_count:,}")
    print(f"  Rows in target window:      {rows_in_window_before:,}")

    print("\nRunning backfill...")
    print("  Step 1: DELETE rows in window")
    print("  Step 2: INSERT re-derived rows from Bronze")

    inserted = build_silver_for_range(LAKE_DIR, DB_PATH, BACKFILL_FROM, BACKFILL_TO)

    with duckdb.connect(str(DB_PATH)) as con:
        after = con.execute(
            "SELECT COUNT(*) FROM silver.prescribing WHERE year_month BETWEEN ? AND ?",
            [BACKFILL_FROM, BACKFILL_TO],
        ).fetchone()
        total_after = con.execute("SELECT COUNT(*) FROM silver.prescribing").fetchone()

    rows_in_window_after = (after or (0,))[0]
    total_after_count = (total_after or (0,))[0]

    print("\nAfter backfill:")
    print(f"  Total Silver rows:          {total_after_count:,}")
    print(f"  Rows in target window:      {rows_in_window_after:,}")
    print(f"  Rows inserted by backfill:  {inserted:,}")

    print("\nIdempotency proof — running again...")
    inserted_again = build_silver_for_range(
        LAKE_DIR, DB_PATH, BACKFILL_FROM, BACKFILL_TO
    )
    print(f"  Rows inserted on second run: {inserted_again:,}")
    assert inserted_again == inserted, "Idempotency violated!"
    print("  Same result — idempotent.")

    print("\nRebuilding Gold from updated Silver...")
    gold_counts = build_gold(DB_PATH)
    for table, count in gold_counts.items():
        print(f"  {table}: {count:,} rows")

    print(f"\nDone. Backfill complete for {BACKFILL_FROM} to {BACKFILL_TO}.")


if __name__ == "__main__":
    main()
