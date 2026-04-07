"""
Script 02 — Query the Bronze lake with DuckDB SQL

What this does
--------------
Queries the raw JSONL files in the Bronze lake directly using DuckDB —
no loading into memory, no schema setup, no database server.

DuckDB reads the files on disk using a glob pattern that matches all four
drug directories at once:

    lake/*/*/prescribing.jsonl

Why this matters
----------------
This is the "query without loading" pattern.  In a production data lake
(S3, GCS, Azure Blob), the same SQL runs against Parquet files using
Athena, BigQuery, or Synapse — the principle is identical.  DuckDB is
doing this locally on JSON files.

Volume: you are querying all four drugs' records in one SQL statement.
Veracity: the raw data still has NULLs and mixed types — this shows
what Bronze looks like before Silver cleaning.

Run
---
    uv run python scripts/02_query_bronze.py

Prerequisites
-------------
    Run scripts/01_fetch.py first to populate lake/
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

import duckdb

LAKE_DIR = pathlib.Path("lake")


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
    jsonl_glob = str(LAKE_DIR / "*" / "*" / "prescribing.jsonl")

    prescribing_files = list(LAKE_DIR.glob("*/*/prescribing.jsonl"))
    if not prescribing_files:
        print("No Bronze data found. Run scripts/01_fetch.py first.")
        return

    print(f"\nQuerying Bronze lake: {jsonl_glob}")
    print(f"Files found: {len(prescribing_files)}\n")

    with duckdb.connect() as con:
        # --- Query 1: row count and date range per drug ---
        print("=" * 55)
        print("Records per drug (Bronze layer — raw, unvalidated)")
        print("=" * 55)
        result = con.execute(f"""
            SELECT
                drug,
                COUNT(*)                       AS rows,
                MIN(date)                      AS earliest,
                MAX(date)                      AS latest,
                COUNT(*) FILTER (WHERE actual_cost IS NULL) AS null_cost_rows
            FROM read_ndjson('{jsonl_glob}', ignore_errors=true)
            GROUP BY drug
            ORDER BY drug
        """)
        _print_result(result)

        # --- Query 2: top 5 ICBs by total cost across all drugs ---
        print("\n" + "=" * 55)
        print("Top 5 ICBs by total prescribing cost (all drugs)")
        print("=" * 55)
        result2 = con.execute(f"""
            SELECT
                ccg                            AS icb_code,
                COUNT(*)                       AS rows,
                ROUND(SUM(actual_cost), 2)     AS total_cost_gbp,
                SUM(items)                     AS total_items
            FROM read_ndjson('{jsonl_glob}', ignore_errors=true)
            WHERE actual_cost IS NOT NULL
            GROUP BY ccg
            ORDER BY total_cost_gbp DESC
            LIMIT 5
        """)
        _print_result(result2)

        # --- Query 3: cross-drug cost-per-item comparison ---
        print("\n" + "=" * 55)
        print("Average cost per item by drug")
        print("(Veracity check: NULLs in cost or items are excluded)")
        print("=" * 55)
        result3 = con.execute(f"""
            SELECT
                drug,
                ROUND(AVG(actual_cost / NULLIF(items, 0)), 4) AS avg_cost_per_item_gbp,
                COUNT(*) FILTER (WHERE actual_cost IS NULL OR items IS NULL) AS null_rows
            FROM read_ndjson('{jsonl_glob}', ignore_errors=true)
            GROUP BY drug
            ORDER BY avg_cost_per_item_gbp DESC
        """)
        _print_result(result3)

    print("\nDone. No files were modified — Bronze is read-only.")


if __name__ == "__main__":
    main()
