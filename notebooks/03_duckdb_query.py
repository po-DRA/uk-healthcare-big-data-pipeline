import marimo

__generated_with = "0.6.0"
app = marimo.App(title="03 · DuckDB SQL — Volume")


@app.cell
def __():
    import marimo as mo

    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 03 — DuckDB SQL Analytics

        **Learning objective:** Query the raw lake files directly with SQL using DuckDB,
        without loading all data into memory. See how DuckDB reads JSONL as if it were
        a database table.

        **V's demonstrated:** Volume (SQL across all records), Velocity (fast in-process queries)

        **Estimated time:** 8 minutes

        > **Prerequisite:** Run notebook 02 first to populate `lake/`.
        """
    )
    return


@app.cell
def __():
    import pathlib

    import duckdb

    return duckdb, pathlib


@app.cell
def __(mo):
    mo.md(
        """
        ## Connecting to DuckDB and Creating a View

        DuckDB can read JSONL directly from disk using `read_json()`.
        We create a **view** — a named SQL query that looks like a table
        but doesn't copy any data. The glob `lake/*/prescribing.jsonl`
        scans all four drug files at once.
        """
    )
    return


@app.cell
def __(duckdb, pathlib):
    LAKE_DIR = pathlib.Path("lake")
    con = duckdb.connect("pipeline.duckdb")

    # Create a view over all JSONL files in the lake
    con.execute(
        """
        CREATE OR REPLACE VIEW prescriptions AS
        SELECT *
        FROM read_json(
            'lake/*/prescribing.jsonl',
            format = 'newline_delimited',
            auto_detect = true
        )
        """
    )

    row_count = con.execute("SELECT COUNT(*) FROM prescriptions").fetchone()[0]
    print("View created: prescriptions")
    print(f"Total rows visible to DuckDB: {row_count:,}")
    print(f"DuckDB file: {pathlib.Path('pipeline.duckdb').resolve()}")
    return LAKE_DIR, con, row_count


@app.cell
def __(mo):
    mo.md(
        """
        ## Query 1 — Total Rows per Drug (Volume)

        How many practice-month records exist for each drug?
        This is our Volume V — each row is one GP practice × one calendar month.
        """
    )
    return


@app.cell
def __(con, row_count):
    q1 = con.execute(
        """
        -- Volume: record counts and percentage share per drug
        SELECT
            drug,
            COUNT(*)                                      AS record_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
        FROM prescriptions
        GROUP BY drug
        ORDER BY record_count DESC
        """
    ).df()

    print("Prescribing records by drug:")
    print(q1.to_string(index=False))
    print(f"\nTotal: {row_count:,} rows across all 4 drugs")
    return (q1,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Query 2 — Monthly Spend Trend for Metformin (Last 12 Months)

        Aggregating £millions of prescribing spend across England over time.
        """
    )
    return


@app.cell
def __(con):
    q2 = con.execute(
        """
        -- Monthly total cost for metformin, most recent 12 months
        SELECT
            date                            AS month,
            ROUND(SUM(actual_cost), 2)      AS total_cost_gbp,
            SUM(items)                      AS total_items
        FROM prescriptions
        WHERE drug = 'metformin'
          AND date >= (
              SELECT STRFTIME(DATE_TRUNC('month', MAX(date::DATE) - INTERVAL 11 MONTH), '%Y-%m-%d')
              FROM prescriptions
              WHERE drug = 'metformin'
          )
        GROUP BY date
        ORDER BY date DESC
        LIMIT 12
        """
    ).df()

    print("Metformin monthly spend (last 12 months):")
    print(q2.to_string(index=False))
    return (q2,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Query 3 — Top 10 Practices by Items for Atorvastatin

        Which GP practices prescribe the most atorvastatin?
        High-volume practices tend to serve larger or older populations.
        """
    )
    return


@app.cell
def __(con):
    q3 = con.execute(
        """
        -- Top 10 practices by total atorvastatin items
        SELECT
            row_id                          AS practice_id,
            SUM(items)                      AS total_items,
            ROUND(SUM(actual_cost), 2)      AS total_cost_gbp
        FROM prescriptions
        WHERE drug = 'atorvastatin'
        GROUP BY row_id
        ORDER BY total_items DESC
        LIMIT 10
        """
    ).df()

    print("Top 10 practices by atorvastatin items:")
    print(q3.to_string(index=False))
    return (q3,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Query 4 — Average Cost per Item by Drug

        NIC (Net Ingredient Cost) per item varies between drugs.
        Can you see which drug costs the most per prescription item?
        """
    )
    return


@app.cell
def __(con):
    q4 = con.execute(
        """
        -- Average cost per item by drug, sorted highest first
        SELECT
            drug,
            ROUND(SUM(actual_cost) / NULLIF(SUM(items), 0), 4) AS avg_nic_per_item_gbp,
            ROUND(SUM(actual_cost), 2)                          AS total_cost_gbp,
            SUM(items)                                          AS total_items
        FROM prescriptions
        GROUP BY drug
        ORDER BY avg_nic_per_item_gbp DESC
        """
    ).df()

    print("Average cost per item by drug:")
    print(q4.to_string(index=False))
    return (q4,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Summary

        You have:
        - Created a DuckDB view directly over raw JSONL lake files — no ETL load step
        - Run four analytical SQL queries across hundreds of thousands of records
        - Seen Volume in the row counts and Velocity in the sub-second query times

        DuckDB's key insight: **the lake is the database**. You don't need to load data
        into a separate database server to query it.

        **Reflection question:** DuckDB reads JSONL from disk on every query.
        What would be the advantage of converting the JSONL to Parquet files?
        When would you _not_ want to do that?

        **→ Next: [04_polars_transform.py](04_polars_transform.py) — lazy transformations and data quality**
        """
    )
    return


if __name__ == "__main__":
    app.run()
