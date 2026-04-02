import marimo

__generated_with = "0.6.0"
app = marimo.App(title="07 · Medallion Architecture — Bronze → Silver → Gold")


@app.cell
def __():
    import marimo as mo

    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 07 — Medallion Architecture

        **Learning objective:** Understand the Bronze → Silver → Gold data lake pattern
        and implement it using DuckDB schemas as the Silver and Gold persistence layer.

        **V's demonstrated:** Veracity (Silver trust boundary), Volume (Gold aggregations)

        **Estimated time:** 10 minutes

        > **Prerequisite:** Run notebook 02 first to populate `lake/` (the Bronze layer).
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## What is the Medallion Architecture?

        The medallion pattern organises a data lake into three named layers.
        Each layer has a clear contract about data quality:

        ```
        ┌────────────────────────────────────────────────────────────────┐
        │  BRONZE  (lake/)                                               │
        │  ─────────────────────────────────────────────────────────     │
        │  Raw, as-received files. Written once, never modified.         │
        │  Format: JSONL + JSON (lake.py writes here)                    │
        │  Guarantee: complete — every byte from the source is here      │
        │  Rule: NEVER transform data in place in Bronze                 │
        └──────────────────────────┬─────────────────────────────────────┘
                                   │  build_silver()
                                   ▼
        ┌────────────────────────────────────────────────────────────────┐
        │  SILVER  (silver.prescribing in pipeline.duckdb)               │
        │  ─────────────────────────────────────────────────────────     │
        │  Cleaned, typed, validated. Safe to hand to an analyst.        │
        │  What changed vs Bronze:                                       │
        │    • dates are DATE, not strings                                │
        │    • numeric fields are DOUBLE / BIGINT, not text              │
        │    • nic_per_item and year_month derived columns added         │
        │    • rows with both cost AND items NULL dropped                 │
        │    • ingested_at timestamp added for audit                     │
        │  Guarantee: trustworthy — no surprises when you query it       │
        └──────────────────────────┬─────────────────────────────────────┘
                                   │  build_gold()
                                   ▼
        ┌────────────────────────────────────────────────────────────────┐
        │  GOLD  (gold.* tables in pipeline.duckdb)                      │
        │  ─────────────────────────────────────────────────────────     │
        │  Aggregated, business-ready. Answers specific questions.       │
        │  Tables:                                                       │
        │    • gold.drug_summary         — top-line KPIs per drug        │
        │    • gold.drug_monthly_spend   — monthly trend data            │
        │    • gold.practice_leaderboard — top 20 practices per drug     │
        │  Guarantee: fast — results load in milliseconds                │
        └────────────────────────────────────────────────────────────────┘
        ```

        > **Key rule:** data flows DOWN (Bronze → Silver → Gold) only.
        > Gold never writes to Silver; Silver never writes to Bronze.
        > If you need to fix a bug, fix it in the transformation code
        > and re-build Silver and Gold from the unchanged Bronze files.
        """
    )
    return


@app.cell
def __():
    import pathlib

    import duckdb

    from pipeline.medallion import build_dim_practice, build_gold, build_silver

    return build_dim_practice, build_gold, build_silver, duckdb, pathlib


@app.cell
def __(pathlib):
    LAKE_DIR = pathlib.Path("lake")
    DB_PATH = pathlib.Path("pipeline.duckdb")
    return DB_PATH, LAKE_DIR


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 1 — Inspect the Bronze Layer

        Bronze is what we already have: raw JSONL and JSON files on disk.
        Nothing has been cleaned or typed — this is the ground truth.
        """
    )
    return


@app.cell
def __(LAKE_DIR):
    from pipeline.lake import lake_summary

    bronze_summary = lake_summary(LAKE_DIR)
    total_bronze_kb = sum(row["size_kb"] for row in bronze_summary)

    print("Bronze layer — raw files on disk:")
    print(f"  {'Drug':<15} {'File':<25} {'Size (KB)':>10}")
    print("  " + "-" * 52)
    for row in bronze_summary:
        print(f"  {row['drug']:<15} {row['file']:<25} {row['size_kb']:>10.1f}")
    print(f"\n  Total Bronze size: {total_bronze_kb:.1f} KB")
    print(f"  Files: {len(bronze_summary)}")
    print("\n  → These files are the ground truth. We will NEVER modify them.")
    return bronze_summary, lake_summary, row, total_bronze_kb


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 2 — Build Silver

        `build_silver()` reads every Bronze JSONL file, applies type casts,
        derives new columns, and writes a clean table to DuckDB.

        Watch the output — it tells you exactly how many rows made it through.
        """
    )
    return


@app.cell
def __(DB_PATH, LAKE_DIR, build_silver):
    silver_rows = build_silver(LAKE_DIR, DB_PATH)
    print(f"\nSilver table ready: {silver_rows:,} rows in silver.prescribing")
    return (silver_rows,)


@app.cell
def __(mo):
    mo.md("## Explore Silver — What Changed?")
    return


@app.cell
def __(DB_PATH, duckdb):
    con = duckdb.connect(str(DB_PATH))

    # Schema: every column and its type
    schema = con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'silver' AND table_name = 'prescribing'
        ORDER BY ordinal_position
        """
    ).fetchall()

    print("silver.prescribing schema:")
    print(f"  {'Column':<20} {'Type':<15}")
    print("  " + "-" * 36)
    for col, dtype in schema:
        marker = (
            " ← NEW" if col in ("nic_per_item", "year_month", "ingested_at") else ""
        )
        print(f"  {col:<20} {dtype:<15}{marker}")
    return col, con, dtype, marker, schema


@app.cell
def __(con):
    # Null counts in Silver vs Bronze
    null_report = con.execute(
        """
        SELECT
            'actual_cost' AS field,
            COUNT(*) FILTER (WHERE actual_cost IS NULL) AS silver_nulls,
            COUNT(*) AS total_rows
        FROM silver.prescribing
        UNION ALL
        SELECT
            'items',
            COUNT(*) FILTER (WHERE items IS NULL),
            COUNT(*)
        FROM silver.prescribing
        UNION ALL
        SELECT
            'nic_per_item',
            COUNT(*) FILTER (WHERE nic_per_item IS NULL),
            COUNT(*)
        FROM silver.prescribing
        """
    ).df()

    print("\nNull counts in Silver (rows where value is missing):")
    print(null_report.to_string(index=False))
    print(
        "\nNote: nic_per_item is NULL when actual_cost or items is NULL — "
        "this is correct behaviour, not a data loss."
    )
    return (null_report,)


@app.cell
def __(con):
    # Sample rows from Silver
    sample = con.execute(
        """
        SELECT date, drug, actual_cost, items, nic_per_item, year_month,
               ingested_at::VARCHAR AS ingested_at
        FROM silver.prescribing
        LIMIT 5
        """
    ).df()

    print("Sample rows from silver.prescribing:")
    print(sample.to_string(index=False))
    print("\nNotice: date is now a DATE type (not a string), nic_per_item is derived.")
    return (sample,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 3 — Build Gold

        `build_gold()` reads Silver and creates three aggregated tables.
        These are what you would connect to Power BI, Tableau, or a REST API.
        """
    )
    return


@app.cell
def __(DB_PATH, build_gold):
    gold_counts = build_gold(DB_PATH)
    print("\nGold layer built:")
    for table, count in gold_counts.items():
        print(f"  {table}: {count:,} rows")
    return gold_counts, table, count


@app.cell
def __(mo):
    mo.md("## Explore Gold — Business-Ready Tables")
    return


@app.cell
def __(con):
    # Gold table 1: top-line KPIs
    summary = con.execute(
        """
        SELECT
            drug,
            total_items,
            total_cost_gbp,
            avg_nic_per_item,
            unique_practices,
            months_of_data
        FROM gold.drug_summary
        ORDER BY total_items DESC
        """
    ).df()

    print("gold.drug_summary — top-line KPIs (the 'Gold dashboard table'):")
    print(summary.to_string(index=False))
    return (summary,)


@app.cell
def __(con):
    # Gold table 2: monthly trend for metformin
    trend = con.execute(
        """
        SELECT year_month, total_items, total_cost_gbp
        FROM gold.drug_monthly_spend
        WHERE drug = 'metformin'
        ORDER BY year_month DESC
        LIMIT 12
        """
    ).df()

    print("gold.drug_monthly_spend — metformin last 12 months:")
    print(trend.to_string(index=False))
    return (trend,)


@app.cell
def __(con):
    # Gold table 3: practice leaderboard
    leaderboard = con.execute(
        """
        SELECT drug, practice_id, total_items, total_cost_gbp, rank
        FROM gold.practice_leaderboard
        WHERE rank <= 5
        ORDER BY drug, rank
        """
    ).df()

    print("gold.practice_leaderboard — top 5 practices per drug:")
    print(leaderboard.to_string(index=False))
    return (leaderboard,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Viewing All Schemas in pipeline.duckdb

        DuckDB schemas act like namespaces.  You can list everything in the file:
        """
    )
    return


@app.cell
def __(con):
    all_tables = con.execute(
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema IN ('silver', 'gold', 'streaming', 'main')
          AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY table_schema, table_name
        """
    ).df()

    print("All tables/views in pipeline.duckdb:")
    print(all_tables.to_string(index=False))
    return (all_tables,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 4 — Slowly Changing Dimensions (SCD Type 2)

        The Gold layer contains *fact* tables (aggregated prescribing numbers).
        But what about the **dimension** data — the GP practices themselves?

        GP practices are **Slowly Changing Dimensions**: they open, close, merge,
        and — crucially — change which commissioning body they belong to.

        ### The NHS context: CCG → ICB reorganisation (July 2022)

        In July 2022 England's **106 Clinical Commissioning Groups (CCGs)** were
        dissolved and replaced by **42 Integrated Care Boards (ICBs)**.  Every GP
        practice that operated before that date now has two valid answers to the
        question "which commissioning body are you in?" depending on when you ask:

        ```
        Practice E84006  (example)
        ──────────────────────────────────────────────────────
        Before July 2022 : CCG = "03V" (NHS Kernow CCG)
        After  July 2022 : CCG = "15N" (NHS Cornwall and Isles of Scilly ICB)
        ```

        A trend query like "how much did ICB X spend on metformin in 2021–22?"
        **will be silently wrong** if you use the current practice → ICB mapping
        for the whole date range.  SCD Type 2 solves this.

        ### SCD Types at a glance

        | Type | Approach | History preserved? | When to use |
        |------|----------|-------------------|-------------|
        | **Type 1** | Overwrite the row | ✗ No | Correcting a data-entry error |
        | **Type 2** | Add a new versioned row | ✓ Full | Attribute changes that matter for point-in-time analysis |
        | **Type 3** | Add a "previous value" column | ✓ One level | Rarely useful; only one change is tracked |

        **Type 2 is the industry default** for audit-required healthcare analytics.
        """
    )
    return


@app.cell
def __(DB_PATH, build_dim_practice):
    dim_rows = build_dim_practice(DB_PATH)
    print(f"\ngold.dim_practice ready: {dim_rows:,} total practice-version rows")
    return (dim_rows,)


@app.cell
def __(mo):
    mo.md("## Explore the Dimension Table")
    return


@app.cell
def __(DB_PATH, duckdb):
    _con = duckdb.connect(str(DB_PATH))

    # Overview: how many practices have more than one version?
    overview = _con.execute(
        """
        SELECT
            COUNT(DISTINCT practice_id)                         AS total_practices,
            COUNT(*)                                            AS total_versions,
            COUNT(*) FILTER (WHERE version_num > 1)            AS changed_versions,
            COUNT(DISTINCT practice_id) FILTER (
                WHERE practice_id IN (
                    SELECT practice_id FROM gold.dim_practice WHERE version_num > 1
                )
            )                                                   AS practices_with_changes
        FROM gold.dim_practice
        """
    ).df()
    print("gold.dim_practice overview:")
    print(overview.to_string(index=False))
    _con.close()
    return (overview,)


@app.cell
def __(DB_PATH, duckdb):
    _con = duckdb.connect(str(DB_PATH))

    # Show practices with multiple versions (attribute changes detected)
    changers = _con.execute(
        """
        SELECT
            practice_id,
            version_num,
            ccg,
            setting,
            valid_from,
            valid_to,
            is_current
        FROM gold.dim_practice
        WHERE practice_id IN (
            SELECT practice_id
            FROM gold.dim_practice
            WHERE version_num > 1
        )
        ORDER BY practice_id, version_num
        LIMIT 20
        """
    ).df()

    if len(changers) > 0:
        print("Practices with attribute changes (SCD Type 2 versioning):")
        print(changers.to_string(index=False))
        print(
            "\nNotice: version 1 has valid_to set (closed), "
            "version N has valid_to = NULL (open / is_current)."
        )
    else:
        print(
            "No attribute changes detected in this dataset.\n"
            "This is expected if the Bronze lake only covers a narrow date range.\n"
            "In a full historical pull (2015–present) you would see CCG → ICB changes."
        )
    _con.close()
    return (changers,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Point-in-Time Query — the SCD Type 2 Payoff

        With the dimension table built, you can answer questions correctly across
        the CCG → ICB boundary.  The pattern is:

        ```sql
        -- "Which practices belonged to CCG 03V in January 2022?"
        SELECT practice_id, ccg, valid_from, valid_to
        FROM gold.dim_practice
        WHERE ccg = '03V'
          AND valid_from <= DATE '2022-01-01'
          AND (valid_to IS NULL OR valid_to > DATE '2022-01-01')
        ```

        Without SCD Type 2 you would apply today's CCG assignment to historical
        records — understating some ICBs' spend and overstating others'.
        """
    )
    return


@app.cell
def __(DB_PATH, duckdb):
    _con = duckdb.connect(str(DB_PATH))

    # Demonstrate point-in-time query
    pit_result = _con.execute(
        """
        -- Point-in-time: what was the CCG assignment for all practices
        -- at the start of the most recent year in our dataset?
        WITH latest_year AS (
            SELECT DATE_TRUNC('year', MAX(valid_from)) AS pit_date
            FROM gold.dim_practice
        )
        SELECT
            d.practice_id,
            d.ccg,
            d.setting,
            d.valid_from,
            d.valid_to,
            d.is_current
        FROM gold.dim_practice d, latest_year l
        WHERE d.valid_from <= l.pit_date
          AND (d.valid_to IS NULL OR d.valid_to > l.pit_date)
        ORDER BY d.practice_id
        LIMIT 10
        """
    ).df()

    print("Point-in-time snapshot — practice CCG assignments at start of latest year:")
    print(pit_result.to_string(index=False))
    _con.close()
    return (pit_result,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Summary

        You have built the full medallion architecture:

        | Layer  | Location                        | Guarantee              |
        |--------|---------------------------------|------------------------|
        | Bronze | `lake/*/prescribing.jsonl`      | Complete (raw as-is)   |
        | Silver | `silver.prescribing` (DuckDB)   | Trustworthy (typed)    |
        | Gold   | `gold.*` tables (DuckDB)        | Fast (pre-aggregated)  |
        | SCD    | `gold.dim_practice` (DuckDB)    | Historically accurate  |

        **Why does this matter in healthcare?**
        - A Bronze record with `actual_cost = null` is preserved — it tells you
          something is missing from the source system.
        - Silver surfaces that null explicitly with a typed schema.
        - Gold ignores it gracefully (NULLIF prevents division-by-zero).
        - Auditors can trace any Gold number back to the Bronze file that produced it.

        **Reflection question:** Imagine a new version of the OpenPrescribing API
        changes the field name `actual_cost` to `cost_gbp`.
        Which layer would you need to update?  Which layers would be unaffected?
        How does keeping Bronze unchanged protect you?

        **→ Next: [08_streaming_simulation.py](08_streaming_simulation.py) — simulated streaming pipeline**
        """
    )
    return


if __name__ == "__main__":
    app.run()
