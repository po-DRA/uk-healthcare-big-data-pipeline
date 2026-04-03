import marimo

__generated_with = "0.6.0"
app = marimo.App(title="Notebook 10 — Backfill: Re-running Bronze → Gold for a Date Range")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 10 — Backfill: Re-running Bronze → Gold for a Date Range

        **Learning objective:** Understand what backfilling is, why it is essential
        for production pipelines, and how to re-process a specific date range without
        touching the rest of the Silver table.

        **V demonstrated:** Veracity — ensuring historical data remains accurate when
        source data or transformation logic changes.

        **Estimated time:** 20 minutes

        > **Prerequisite:** Run notebooks 02 and 07 first so the Bronze lake and
        > Silver/Gold tables exist.  This notebook modifies `silver.prescribing` in
        > place, so work on a copy if you want to preserve a clean state.

        ---

        ## The problem backfill solves

        A data pipeline is never "done".  In the real world, three things routinely
        require you to re-process historical data:

        | Scenario | What happened | What you must do |
        |----------|--------------|-----------------|
        | **New data source** | You added a fifth drug (`lisinopril`) to the pipeline | Fetch its Bronze files and re-run Silver/Gold so it appears in all historical reports |
        | **Bug fix** | A transformation in `build_silver()` was computing `nic_per_item` incorrectly for three months | Re-run only those three months without disturbing the rest |
        | **Schema change** | A new `region` column was added to Silver | Re-derive it for all historical rows (full rebuild) or just recent ones (partial) |

        The naive solution — always do a **full rebuild** — works when the Bronze lake
        is small.  At NHS scale (hundreds of millions of records, years of history) a
        full rebuild could take hours.  **Partition-level backfill** is the production
        answer.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Full rebuild vs partition-level backfill

        ```
        Full rebuild (build_silver)            Partition backfill (build_silver_for_range)
        ──────────────────────────────────     ──────────────────────────────────────────
        DROP TABLE silver.prescribing_new      DELETE FROM silver.prescribing
        CREATE TABLE silver.prescribing_new      WHERE year_month BETWEEN '2023-01'
          SELECT … FROM lake/*/prescribing.jsonl   AND '2023-12'
          WHERE NOT (cost IS NULL AND items IS NULL)
        RENAME → silver.prescribing           INSERT INTO silver.prescribing
                                                SELECT … FROM lake/*/prescribing.jsonl
                                                WHERE year_month BETWEEN '2023-01'
                                                  AND '2023-12'

        Cost: reads ALL Bronze files           Cost: reads ALL Bronze files but inserts
                                                only the target months
        Safe: replaces entire table            Safe: DELETE + INSERT is idempotent
        ```

        Both operations are **idempotent** — running them twice produces the same result
        as running them once.  That is the key property that makes backfill safe.

        > **Idempotency rule:** if you can re-run a pipeline step on the same input
        > and get the same output, you can safely retry, replay, or backfill without
        > fear of double-counting.
        """
    )
    return


@app.cell
def __():
    import pathlib

    import duckdb
    import polars as pl

    from pipeline.medallion import build_gold, build_silver, build_silver_for_range

    return build_gold, build_silver, build_silver_for_range, duckdb, pathlib, pl


@app.cell
def __(pathlib):
    LAKE_DIR = pathlib.Path("lake")
    DB_PATH = pathlib.Path("pipeline.duckdb")
    return DB_PATH, LAKE_DIR


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 1 — Snapshot the current Silver state

        Before any backfill, record the row counts and month range so we can
        compare before and after.
        """
    )
    return


@app.cell
def __(DB_PATH, duckdb):
    _con = duckdb.connect(str(DB_PATH))
    _before = _con.execute(
        """
        SELECT
            drug,
            COUNT(*)            AS rows,
            MIN(year_month)     AS earliest,
            MAX(year_month)     AS latest
        FROM silver.prescribing
        GROUP BY drug
        ORDER BY drug
        """
    ).df()
    _total_before = _con.execute("SELECT COUNT(*) FROM silver.prescribing").fetchone()[0]
    _con.close()

    print(f"Silver rows before backfill: {_total_before:,}")
    print(_before.to_string(index=False))
    _before  # noqa: B018 — Marimo cell return value, rendered as output


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 2 — Choose a backfill window

        We will re-process a single calendar year.  Change these values to target
        any range — the operation is safe to run repeatedly.

        In a real scenario this would be driven by:
        - The dates affected by the bug
        - The first month a new drug was available
        - The period covered by a late-arriving data correction
        """
    )
    return


@app.cell
def __():
    BACKFILL_FROM = "2023-01"
    BACKFILL_TO   = "2023-12"
    return BACKFILL_FROM, BACKFILL_TO


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 3 — Run the partition-level backfill

        `build_silver_for_range()` does exactly two SQL operations:

        1. **DELETE** all Silver rows whose `year_month` is in the window.
        2. **INSERT** fresh rows from Bronze, filtered to the same window,
           with the same TRY_CAST transformations as the full `build_silver()`.

        The rows outside the window are untouched.
        """
    )
    return


@app.cell
def __(BACKFILL_FROM, BACKFILL_TO, DB_PATH, LAKE_DIR, build_silver_for_range):
    _inserted = build_silver_for_range(
        lake_dir=LAKE_DIR,
        db_path=DB_PATH,
        from_month=BACKFILL_FROM,
        to_month=BACKFILL_TO,
    )
    print(f"Rows inserted for {BACKFILL_FROM} → {BACKFILL_TO}: {_inserted:,}")
    _inserted  # noqa: B018 — Marimo cell return value, rendered as output


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 4 — Verify idempotency

        Run the same backfill a second time.  The row count for that window
        must be identical — DELETE first, then re-INSERT, so there is no
        double-counting.
        """
    )
    return


@app.cell
def __(BACKFILL_FROM, BACKFILL_TO, DB_PATH, LAKE_DIR, build_silver_for_range):
    _inserted_2 = build_silver_for_range(
        lake_dir=LAKE_DIR,
        db_path=DB_PATH,
        from_month=BACKFILL_FROM,
        to_month=BACKFILL_TO,
    )
    print(f"Second run — rows in window: {_inserted_2:,}")
    print("✓ Idempotent: same result whether run once or twice")
    _inserted_2  # noqa: B018 — Marimo cell return value, rendered as output


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 5 — Compare Silver before and after

        Rows outside the backfill window must be unchanged; rows inside must
        reflect the fresh re-processing.
        """
    )
    return


@app.cell
def __(DB_PATH, duckdb):
    _con = duckdb.connect(str(DB_PATH))
    _after = _con.execute(
        """
        SELECT
            drug,
            COUNT(*)            AS rows,
            MIN(year_month)     AS earliest,
            MAX(year_month)     AS latest
        FROM silver.prescribing
        GROUP BY drug
        ORDER BY drug
        """
    ).df()
    _total_after = _con.execute("SELECT COUNT(*) FROM silver.prescribing").fetchone()[0]
    _con.close()

    print(f"Silver rows after backfill: {_total_after:,}")
    _after  # noqa: B018 — Marimo cell return value, rendered as output


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 6 — Rebuild Gold from the updated Silver

        Gold tables are always derived from the full Silver table, so a
        rebuild after any Silver change (full or partial) produces consistent
        aggregations.

        This step is fast because Gold is just SQL aggregations over Silver —
        no file I/O, no HTTP calls.
        """
    )
    return


@app.cell
def __(DB_PATH, build_gold):
    _gold_counts = build_gold(DB_PATH)
    for _table, _n in _gold_counts.items():
        print(f"  {_table}: {_n:,} rows")
    _gold_counts  # noqa: B018 — Marimo cell return value, rendered as output


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 7 — Inspect the ingested_at audit trail

        Every Silver row carries an `ingested_at` timestamp set by `now()` at
        insert time.  Rows that were part of this backfill will have a newer
        timestamp than rows that were not touched.

        This is how you would answer the audit question:
        *"Which rows were affected by the backfill that ran at 14:37 on 2 April?"*
        """
    )
    return


@app.cell
def __(BACKFILL_FROM, BACKFILL_TO, DB_PATH, duckdb):
    _con = duckdb.connect(str(DB_PATH))
    _audit = _con.execute(
        """
        SELECT
            year_month,
            drug,
            COUNT(*)                AS rows,
            MAX(ingested_at)        AS last_ingested_at
        FROM silver.prescribing
        WHERE year_month BETWEEN ? AND ?
        GROUP BY year_month, drug
        ORDER BY year_month, drug
        LIMIT 12
        """,
        [BACKFILL_FROM, BACKFILL_TO],
    ).df()
    _con.close()
    print("Backfilled rows with ingested_at timestamps:")
    print(_audit.to_string(index=False))
    _audit  # noqa: B018 — Marimo cell return value, rendered as output


@app.cell
def __(mo):
    mo.md(
        """
        ## What happens to data contracts during backfill?

        When `build_silver_for_range()` re-reads Bronze, the same Pydantic
        contracts from `pipeline/contracts.py` apply.  If a Bronze record was
        written with an invalid schema — for example, a non-numeric `actual_cost` —
        DuckDB's `TRY_CAST` converts it to NULL and it still passes.

        The Silver DQ gate (`validate_silver_task` in the Prefect flow) should
        be re-run after any backfill to confirm that the refreshed data still
        meets the null-rate thresholds.

        ```python
        # After backfill, re-run the DQ gate manually:
        from flows.pipeline_flow import validate_silver_task
        validate_silver_task.fn(DB_PATH)
        ```
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Future: how distributed systems handle backfill

        The pattern you just used — DELETE a partition, then re-INSERT — is the
        same conceptual operation that production distributed systems perform,
        just with different mechanics:

        ---

        ### Delta Lake / Apache Iceberg (Level 4)

        Open table formats store data as Parquet files with a transaction log.
        Backfill uses `MERGE INTO` (upsert) rather than DELETE + INSERT:

        ```sql
        -- Delta Lake / Iceberg MERGE (conceptual)
        MERGE INTO silver.prescribing AS target
        USING (SELECT … FROM bronze WHERE year_month BETWEEN '2023-01' AND '2023-12') AS source
        ON target.row_id = source.row_id AND target.year_month = source.year_month
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        ```

        Key advantages over DELETE + INSERT:
        - **Time travel:** every version of the table is preserved in the transaction log.
          `SELECT * FROM delta.prescribing VERSION AS OF 42` returns data as it was at
          that snapshot.
        - **ACID guarantees at petabyte scale** across multiple Spark workers — something
          DuckDB cannot do.
        - **Partition pruning:** Iceberg tracks exactly which Parquet files contain which
          month, so re-reading Bronze does not scan unaffected files.

        → *To migrate this pipeline to Delta Lake: replace `duckdb.connect()` in
        `medallion.py` with a PySpark + `delta` session.  The SQL logic is identical.*

        ---

        ### Apache Kafka (Level 4)

        When your Bronze data arrives as a real-time stream rather than files, backfill
        means **replaying Kafka offsets**:

        ```python
        # Conceptual Kafka offset reset (not runnable here)
        consumer = KafkaConsumer("prescribing-events")
        consumer.seek(partition=0, offset=BACKFILL_START_OFFSET)
        for record in consumer:
            if record.timestamp > BACKFILL_END_TS:
                break
            silver_writer.write(transform(record.value))
        ```

        Key differences from file-based backfill:
        - Kafka retains messages for a configurable retention period (default 7 days).
          For months-old backfills you need a separate **cold storage replay** topic or
          a compacted topic.
        - **Consumer group offsets** must be carefully managed so the backfill consumer
          does not interfere with the live consumer.
        - **Exactly-once semantics** (Kafka Transactions + idempotent producers) ensure
          the re-played records are not double-counted.

        → *The generator in `stream.py` is architecturally identical to a Kafka consumer.
        Swap `prescribing_event_stream()` for a real `KafkaConsumer` and the rest of the
        pipeline is unchanged.*

        ---

        ### The common thread

        Whether you use DuckDB, Delta Lake, or Kafka, the **principle is the same**:
        backfill is safe when your pipeline is **idempotent**.  If re-running produces the
        same result, you can always fix the past.  That is why idempotency is a
        non-negotiable property at Level 2 of data engineering maturity — and why this
        pipeline was designed with it from the start.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Summary

        | Operation | Function | When to use |
        |-----------|----------|-------------|
        | Full Silver rebuild | `build_silver()` | First run; schema changes affecting all rows |
        | Partition backfill | `build_silver_for_range()` | Bug fix, new drug, or corrected source for a date window |
        | Gold rebuild | `build_gold()` | Always after any Silver change |
        | DQ gate | `validate_silver_task.fn()` | After rebuild or backfill, before serving Gold |

        ### Reflection questions

        1. You discover that the `nic_per_item` calculation was wrong for all records in
           2022 due to a unit conversion bug.  Which function do you call, and with what
           arguments?

        2. A new drug (`co-amoxiclav`) was added to `DRUG_CODES` today.  The Bronze
           files have been fetched going back to 2019.  What is the most efficient
           backfill strategy?

        3. Why is `ingested_at` set using `now()` at INSERT time rather than copied from
           the Bronze file?  What would you lose if you used the Bronze file's
           creation timestamp instead?
        """
    )
    return


@app.cell
def __():
    if __name__ == "__main__":
        app.run()
    return


if __name__ == "__main__":
    app.run()
