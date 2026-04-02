"""
pipeline/transform.py — Polars lazy transformation and data quality layer.

Demonstrates:
  - **Volume**: scanning millions of JSONL records with a single lazy expression
  - **Veracity**: quantifying nulls, type coercions, and data quality issues

All Polars operations use method chaining — no intermediate variables.
build_prescribing_df() deliberately returns a LazyFrame so students can
inspect the query plan before collecting.
"""

from __future__ import annotations

import pathlib

import polars as pl


def build_prescribing_df(lake_dir: pathlib.Path) -> pl.LazyFrame:
    """Lazily scan all prescribing JSONL files from the lake into one frame.

    Demonstrates **Volume** (lazy scan across all drugs at once) and
    **Veracity** (type casting surfaces hidden nulls).

    Uses ``pl.scan_ndjson()`` on a glob pattern so DuckDB and Polars can
    both read from the same raw files — the lake is the single source of truth.

    Parameters
    ----------
    lake_dir:
        Root of the data lake, e.g. ``pathlib.Path("lake")``.

    Returns
    -------
    pl.LazyFrame
        Lazy frame with columns:
        date, actual_cost (Float64), items (Int64), quantity (Float64),
        row_id, setting, ccg, drug, nic_per_item (Float64), year_month (String).

    Notes
    -----
    Does **not** call ``.collect()`` — callers decide when to materialise.
    """
    glob_pattern = str(lake_dir / "*" / "prescribing.jsonl")

    return (
        pl.scan_ndjson(glob_pattern)
        .with_columns(
            pl.col("actual_cost").cast(pl.Float64),
            pl.col("items").cast(pl.Int64),
            pl.col("quantity").cast(pl.Float64),
        )
        .with_columns(
            (pl.col("actual_cost") / pl.col("items").replace(0, None))
            .round(4)
            .alias("nic_per_item"),
            pl.col("date").str.slice(0, 7).alias("year_month"),
        )
    )


def veracity_report(df: pl.DataFrame) -> pl.DataFrame:
    """Produce a data-quality summary for the key prescribing fields.

    Demonstrates **Veracity** — quantifying how trustworthy the data is
    by counting nulls and measuring uniqueness.

    Parameters
    ----------
    df:
        Collected prescribing DataFrame (call ``.collect()`` on the LazyFrame
        before passing here).

    Returns
    -------
    pl.DataFrame
        One row per checked field with columns:
        field (String), null_count (Int64), null_pct (Float64), unique_count (Int64).
    """
    fields_to_check = ["actual_cost", "items", "quantity", "setting"]
    total_rows = len(df)

    rows = []
    for field in fields_to_check:
        if field not in df.columns:
            continue
        col = df[field]
        null_count = col.null_count()
        unique_count = col.n_unique()
        null_pct = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0.0
        rows.append(
            {
                "field": field,
                "null_count": null_count,
                "null_pct": null_pct,
                "unique_count": unique_count,
            }
        )

    return pl.DataFrame(
        rows,
        schema={
            "field": pl.String,
            "null_count": pl.Int64,
            "null_pct": pl.Float64,
            "unique_count": pl.Int64,
        },
    )
