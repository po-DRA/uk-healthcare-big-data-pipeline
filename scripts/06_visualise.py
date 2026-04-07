"""
Script 06 — Visualise Gold data (cost trends, drug comparison, NLP terms)

What this does
--------------
Reads the Gold DuckDB tables built by script 04 and produces three charts:

  1. Items prescribed per drug (bar chart) — Volume V
  2. Average cost per item by drug (bar chart) — clinical efficiency metric
  3. Monthly prescribing trend for one drug — Velocity V

Charts are saved to outputs/ as PNG files with timestamped filenames.

Why this matters
----------------
Gold tables are purpose-built for analytical queries — this is why we
separate the layers.  The visualisation code does not touch Bronze or Silver;
it only reads the pre-aggregated Gold tables.

This is the same pattern as a BI tool (Tableau, Power BI, Looker) reading
from a data warehouse: Gold = the warehouse, scripts/06 = the BI layer.

Run
---
    uv run python scripts/06_visualise.py

Prerequisites
-------------
    Run scripts/04_medallion.py first to build the Gold tables.
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

import duckdb
import polars as pl

from pipeline.visualise import (
    figure_to_bytes,
    plot_cost_per_item,
    plot_items_by_drug,
    plot_monthly_trend,
    report_filename,
    save_figure,
)

DB_PATH = pathlib.Path("pipeline.duckdb")
OUTPUT_DIR = pathlib.Path("outputs")


def main() -> None:
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        print("Run scripts/04_medallion.py first.")
        return

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]

    if "drug_summary" not in tables:
        print("Gold tables not found. Run scripts/04_medallion.py first.")
        return

    OUTPUT_DIR.mkdir(exist_ok=True)

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        # Load Gold data as Polars DataFrames via fetchall + schema
        def _to_polars(rel: duckdb.DuckDBPyRelation) -> pl.DataFrame:
            cols = [d[0] for d in rel.description]
            return pl.DataFrame(rel.fetchall(), schema=cols, orient="row")

        summary_df = _to_polars(con.execute("SELECT * FROM gold.drug_summary"))
        monthly_df = _to_polars(con.execute("SELECT * FROM gold.drug_monthly_spend"))

    print(f"\nGold data loaded:")
    print(f"  drug_summary: {len(summary_df)} drugs")
    print(f"  drug_monthly_spend: {len(monthly_df)} drug-month rows")

    # Chart 1: Items by drug
    print("\nGenerating chart 1: Items prescribed per drug...")
    fig1 = plot_items_by_drug(summary_df)
    fname1 = report_filename("items_by_drug")
    save_figure(fig1, OUTPUT_DIR / fname1)
    print(f"  Saved: outputs/{fname1}")

    # Chart 2: Cost per item
    print("Generating chart 2: Average cost per item by drug...")
    fig2 = plot_cost_per_item(summary_df)
    fname2 = report_filename("cost_per_item")
    save_figure(fig2, OUTPUT_DIR / fname2)
    print(f"  Saved: outputs/{fname2}")

    # Chart 3: Monthly trend for the highest-cost drug
    highest_cost_drug = (
        summary_df.sort("total_cost_gbp", descending=True)["drug"][0]
    )
    print(f"Generating chart 3: Monthly trend for {highest_cost_drug}...")
    fig3 = plot_monthly_trend(monthly_df, drug=highest_cost_drug)
    fname3 = report_filename(f"monthly_trend_{highest_cost_drug}")
    save_figure(fig3, OUTPUT_DIR / fname3)
    print(f"  Saved: outputs/{fname3}")

    print(f"\nAll charts saved to {OUTPUT_DIR.resolve()}")
    print("\nGold summary:")
    print(summary_df[["drug", "total_items", "total_cost_gbp", "avg_cost_per_item"]])


if __name__ == "__main__":
    main()
