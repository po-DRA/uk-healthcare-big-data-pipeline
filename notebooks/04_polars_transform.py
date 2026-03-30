import marimo

__generated_with = "0.6.0"
app = marimo.App(title="04 · Polars Transform — Volume & Veracity")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 04 — Polars Lazy Transformation

        **Learning objective:** Use Polars lazy evaluation to transform the raw lake data,
        inspect the query optimisation plan, and quantify data quality with a veracity report.

        **V's demonstrated:** Volume (lazy scan), Veracity (null quantification)

        **Estimated time:** 8 minutes

        > **Prerequisite:** Run notebook 02 first to populate `lake/`.
        """
    )
    return


@app.cell
def __():
    import pathlib

    import polars as pl

    from pipeline.transform import build_prescribing_df, veracity_report
    return build_prescribing_df, pathlib, pl, veracity_report


@app.cell
def __(mo):
    mo.md(
        """
        ## Lazy vs Eager — the Key Distinction

        **Eager** (like standard Python lists or pandas):
        Computation happens immediately. If you filter 1 million rows and then
        select 2 columns, you process all 1 million rows _before_ dropping columns.

        **Lazy** (Polars LazyFrame, Spark, DuckDB):
        You describe _what_ you want; the engine decides _how_ to do it.
        Polars will push filters down, merge operations, and parallelise automatically.
        """
    )
    return


@app.cell
def __(build_prescribing_df, pathlib):
    LAKE_DIR = pathlib.Path("lake")
    lazy_df = build_prescribing_df(LAKE_DIR)
    return LAKE_DIR, lazy_df


@app.cell
def __(mo):
    mo.md("## Query Plan — What Polars Plans to Do")
    return


@app.cell
def __(lazy_df):
    print("Polars optimised query plan:")
    print("-" * 60)
    print(lazy_df.explain())
    print("-" * 60)
    print()
    print("Notice: Polars may reorder, merge, or push down predicates.")
    print("Nothing has been read from disk yet — this is just a plan.")
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Collecting — Materialising the Result

        `.collect()` triggers execution of the plan. Only now does Polars
        read from disk, apply transformations, and allocate memory.
        """
    )
    return


@app.cell
def __(lazy_df):
    df = lazy_df.collect()
    print(f"Collected DataFrame: {len(df):,} rows × {len(df.columns)} columns")
    print(f"Columns: {df.columns}")
    print(f"\nSchema:")
    for col_name, dtype in df.schema.items():
        print(f"  {col_name:<20} {dtype}")
    return (df,)


@app.cell
def __(mo):
    mo.md("## Derived Columns — Method Chaining in Action")
    return


@app.cell
def __(df):
    # Show the derived columns we added in build_prescribing_df
    print("Sample of nic_per_item and year_month columns:")
    print(
        df.select(["drug", "date", "year_month", "actual_cost", "items", "nic_per_item"])
        .head(8)
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Veracity Report — Quantifying Data Quality

        Real NHS data has nulls, missing values, and inconsistencies.
        The `veracity_report()` function surfaces these explicitly so we know
        exactly how much to trust each field.
        """
    )
    return


@app.cell
def __(df, veracity_report):
    report = veracity_report(df)
    print("Data quality report:")
    print(report)
    print()
    total = len(df)
    for row in report.iter_rows(named=True):
        status = "✓ CLEAN" if row["null_count"] == 0 else f"⚠ {row['null_count']:,} nulls ({row['null_pct']}%)"
        print(f"  {row['field']:<15}  {status}")
    print(f"\nTotal rows: {total:,}")
    return report, row, total


@app.cell
def __(mo):
    mo.md("## Top 10 Practices by Total Items per Drug")
    return


@app.cell
def __(df, pl):
    for drug_name in df["drug"].unique().to_list():
        drug_df = (
            df.filter(pl.col("drug") == drug_name)
            .group_by("row_id")
            .agg(
                pl.col("items").sum().alias("total_items"),
                pl.col("actual_cost").sum().round(2).alias("total_cost_gbp"),
            )
            .sort("total_items", descending=True)
            .head(5)
        )
        print(f"\nTop 5 practices for {drug_name}:")
        print(drug_df)
    return drug_df, drug_name


@app.cell
def __(mo):
    mo.md(
        """
        ## Lazy vs Eager — Side-by-Side Comparison

        Let's demonstrate the difference concretely:
        """
    )
    return


@app.cell
def __(LAKE_DIR, df, json, pl, pathlib):
    import json
    import time

    # Eager: load entire JSONL files with json.load() first
    _t0 = time.perf_counter()
    eager_records = []
    for _jsonl_path in LAKE_DIR.glob("*/prescribing.jsonl"):
        with _jsonl_path.open() as fh:
            eager_records.extend(json.loads(line) for line in fh if line.strip())
    eager_df = pl.DataFrame(eager_records)
    eager_time = time.perf_counter() - _t0

    # Lazy: scan with scan_ndjson (already done above — just time a collect)
    from pipeline.transform import build_prescribing_df
    _t0 = time.perf_counter()
    _ = build_prescribing_df(LAKE_DIR).collect()
    lazy_time = time.perf_counter() - _t0

    print("Loading approach comparison:")
    print(f"  Eager (json.load + pl.DataFrame) : {eager_time:.3f} s")
    print(f"  Lazy  (pl.scan_ndjson + collect) : {lazy_time:.3f} s")
    print()
    print("The lazy approach becomes dramatically faster as data grows,")
    print("because Polars can push filters down and avoid reading unused columns.")
    return build_prescribing_df, eager_df, eager_records, eager_time, fh, json, lazy_time, time


@app.cell
def __(mo):
    mo.md(
        """
        ## Summary

        You have:
        - Inspected the Polars query plan _before_ any data was read
        - Collected the LazyFrame and examined the schema
        - Run the veracity report and identified null fields — this is **Veracity**
        - Compared lazy vs eager loading

        **Reflection question:** The veracity report shows null values in `actual_cost`
        and/or `items` for some records. What could cause a GP practice to have a
        prescribing record with no cost? Is this a data error or a valid business case?

        **→ Next: [05_nlp_unstructured.py](05_nlp_unstructured.py) — NLP on NHS clinical text**
        """
    )
    return


if __name__ == "__main__":
    app.run()
