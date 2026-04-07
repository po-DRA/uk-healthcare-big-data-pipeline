"""
Script 04 — Build Silver and Gold layers (Medallion Architecture)

What this does
--------------
Runs the Bronze → Silver → Gold transformation pipeline using DuckDB.

  Bronze  raw JSONL files in lake/ (written by script 01)
  Silver  typed, cleaned, deduplicated DuckDB table
  Gold    three aggregated analytical tables

Medallion architecture
~~~~~~~~~~~~~~~~~~~~~~
Each layer has a contract:

  Bronze: write-once, never modify — the immutable source of truth
  Silver: typed, deduplicated, NULL-filtered — the analytical source of truth
  Gold:   aggregated for a specific use case — what analysts query

Idempotency
~~~~~~~~~~~
Running this script twice gives the same result as running it once.
Silver uses an atomic table swap (CREATE → DROP old → RENAME new).
Gold tables are rebuilt from scratch on each run.
This means you can safely re-run after a failure with no cleanup needed.

Slowly Changing Dimension (SCD Type 2)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The practice dimension (gold.dim_practice) tracks GP practice attributes
over time.  In July 2022, NHS England replaced 135 CCGs with 42 ICBs.
SCD Type 2 creates a new row when attributes change, preserving history:

    practice_id | ccg_code | valid_from | valid_to   | is_current
    A81001      | 03V      | 2022-01-01 | 2022-07-01 | false
    A81001      | QHG      | 2022-07-01 | NULL       | true

Without SCD2, a query joining prescribing records to dim_practice would
get wrong ICB codes for pre-2022 data.

Run
---
    uv run python scripts/04_medallion.py

Prerequisites
-------------
    Run scripts/01_fetch.py first to populate lake/
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

import duckdb

from pipeline.medallion import (
    SILVER_DRUGS,
    build_dim_practice,
    build_gold,
    build_silver,
)

LAKE_DIR = pathlib.Path("lake")
DB_PATH = pathlib.Path("pipeline.duckdb")


def _print_result(rel: duckdb.DuckDBPyRelation) -> None:
    cols = [d[0] for d in rel.description]
    rows = rel.fetchall()
    col_widths = [
        max(len(c), max((len(str(r[i])) for r in rows), default=0))
        for i, c in enumerate(cols)
    ]
    header = "  ".join(c.ljust(col_widths[i]) for i, c in enumerate(cols))
    print(header)
    print("  ".join("-" * w for w in col_widths))
    for row in rows:
        print("  ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)))


def main() -> None:
    prescribing_files = list(LAKE_DIR.glob("*/*/prescribing.jsonl"))
    if not prescribing_files:
        print("No Bronze data found. Run scripts/01_fetch.py first.")
        return

    print(f"\nDatabase: {DB_PATH.resolve()}")

    # --- Silver ---
    bronze_drugs = sorted({p.parent.parent.name for p in prescribing_files})
    print(f"\nBronze holds {len(bronze_drugs)} drugs: {', '.join(bronze_drugs)}")
    print(f"Silver will promote {len(SILVER_DRUGS)} drugs: {', '.join(SILVER_DRUGS)}")
    print("  (remaining stay in Bronze for future use)")

    print("\n[1/3] Building Silver layer...")
    silver_rows = build_silver(LAKE_DIR, DB_PATH)
    print(f"  silver.prescribing: {silver_rows:,} rows")
    print("  Transformations applied:")
    print("    - Drug filter: only SILVER_DRUGS promoted from Bronze")
    print("    - TRY_CAST to proper types (date, float, int)")
    print("    - Derived column: nic_per_item = actual_cost / NULLIF(items, 0)")
    print("    - Derived column: year_month (for partitioning)")
    print("    - Dropped rows where BOTH actual_cost AND items are NULL")
    print("    - Atomic table swap (readers never see an empty table)")

    # --- Gold ---
    print("\n[2/3] Building Gold layer...")
    gold_counts = build_gold(DB_PATH)
    for table, count in gold_counts.items():
        print(f"  {table}: {count:,} rows")

    # --- Practice dimension (SCD Type 2) ---
    print("\n[3/3] Building SCD Type 2 practice dimension...")
    dim_rows = build_dim_practice(DB_PATH)
    print(f"  gold.dim_practice: {dim_rows:,} practice-version rows")

    # --- Show a sample from each Gold table ---
    print("\nSample Gold data:")
    with duckdb.connect(str(DB_PATH)) as con:
        print("\n  gold.drug_summary (top 3 by total cost):")
        df = con.execute("""
            SELECT drug, total_items, ROUND(total_cost_gbp, 2) AS cost_gbp,
                   ROUND(avg_nic_per_item, 4) AS cost_per_item
            FROM gold.drug_summary
            ORDER BY cost_gbp DESC LIMIT 3
        """)
        _print_result(df)

        scd2 = con.execute("""
            SELECT COUNT(*) AS total_rows,
                   COUNT(*) FILTER (WHERE version_num > 1) AS practices_with_history
            FROM gold.dim_practice
        """).fetchone()
        print(
            f"\n  gold.dim_practice: {scd2[0]} rows, "
            f"{scd2[1]} practices with CCG->ICB history"
        )

    print(f"\nDone. DuckDB file: {DB_PATH.resolve()}")
    print(
        'Query it directly: uv run python -c "import duckdb; '
        "con=duckdb.connect('pipeline.duckdb'); "
        "print(con.execute('SHOW TABLES').pl())\""
    )


if __name__ == "__main__":
    main()
