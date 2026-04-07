"""
pipeline/medallion.py — Medallion architecture: Bronze → Silver → Gold.

Implements the three-layer data lake pattern using DuckDB as the persistence
store for the Silver and Gold layers:

  Bronze  raw JSONL/JSON files on disk written by lake.py — never modified
  Silver  cleaned, typed, deduplicated DuckDB table (schema: silver)
  Gold    aggregated business-ready DuckDB tables   (schema: gold)

Teaching points
---------------
- Bronze is write-once: you never transform the raw files in place.
  If something goes wrong you can always re-derive Silver/Gold from Bronze.
- Silver is the trust boundary AND a deliberate analytical choice.
  Not every Bronze drug is promoted to Silver — ``SILVER_DRUGS`` defines
  which drugs are in scope for analysis.  The rest stay in Bronze and can
  be promoted later without re-fetching.
- Gold answers specific business questions: these tables are what you'd
  connect to Power BI, Tableau, or a REST API.
- DuckDB schemas (``silver``, ``gold``) give us namespace separation inside
  a single ``pipeline.duckdb`` file — no separate database server required.

Bronze → Silver drug filter
---------------------------
Bronze holds all fetched drugs (7 at time of writing).  Silver is filtered
to ``SILVER_DRUGS`` — a curated set chosen for clinical diversity and
analytical interest:

  metformin     — highest-volume diabetes drug; cost baseline
  semaglutide   — Ozempic/Wegovy; highest cost-per-item; major 2024-25 story
  atorvastatin  — highest-volume cardiovascular statin
  simvastatin   — older statin; contrasts cost profile with atorvastatin
  aspirin       — antiplatelet; very high volume, very low cost-per-item
  lisinopril    — ACE inhibitor; cardiovascular
  lansoprazole  — proton pump inhibitor; gastro-intestinal
  levothyroxine — thyroid hormone; long-term condition management
  salbutamol    — respiratory; clinically distinct category
  amoxicillin   — antibiotic; high seasonal volume (winter)

Drugs left in Bronze only (available for future Silver promotion):
  liraglutide  — older GLP-1, largely superseded by semaglutide
  tirzepatide  — Mounjaro; very new, low volumes in some monthly files

Usage
-----
    from pathlib import Path
    from pipeline.medallion import build_silver, build_gold

    lake_dir = Path("lake")
    db_path  = Path("pipeline.duckdb")

    row_count = build_silver(lake_dir, db_path)
    print(f"Silver rows: {row_count:,}")

    counts = build_gold(db_path)
    for table, n in counts.items():
        print(f"  {table}: {n:,} rows")
"""

from __future__ import annotations

import logging
import pathlib

import duckdb

_log = logging.getLogger(__name__)

# Drugs promoted from Bronze to Silver.
# Bronze holds all fetched drugs; Silver is a deliberate analytical subset.
# To add a drug: fetch its Bronze files then add its name here.
SILVER_DRUGS: tuple[str, ...] = (
    "metformin",
    "semaglutide",
    "atorvastatin",
    "simvastatin",
    "aspirin",
    "lisinopril",
    "lansoprazole",
    "levothyroxine",
    "salbutamol",
    "amoxicillin",
)

# SQL IN clause derived from SILVER_DRUGS — used in build_silver queries.
_SILVER_DRUGS_SQL = ", ".join(f"'{d}'" for d in SILVER_DRUGS)


def _ensure_schemas(con: duckdb.DuckDBPyConnection) -> None:
    """Create silver and gold schemas if they don't already exist."""
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")


def build_silver(
    lake_dir: pathlib.Path,
    db_path: pathlib.Path = pathlib.Path("pipeline.duckdb"),
) -> int:
    """Read Bronze JSONL files and write a cleaned Silver table to DuckDB.

    Demonstrates **Veracity** — Silver is where we make the data trustworthy:
    types are enforced, nulls are surfaced, and unresolvable rows are dropped.

    Silver transformations applied
    --------------------------------
    - ``TRY_CAST`` all numeric fields (surfaces bad values as NULL rather than
      raising an error — graceful degradation)
    - Derive ``nic_per_item`` (cost ÷ items, NULL when either is NULL)
    - Derive ``year_month`` string (``"YYYY-MM"``)
    - Drop rows where *both* ``actual_cost`` AND ``items`` are NULL
      (these rows carry no analytical value)
    - Add ``ingested_at`` timestamp so we know when Silver was built

    Parameters
    ----------
    lake_dir:
        Root of the Bronze lake, e.g. ``Path("lake")``.
    db_path:
        Path to the DuckDB file, e.g. ``Path("pipeline.duckdb")``.

    Returns
    -------
    int
        Number of rows written to ``silver.prescribing``.
    """
    # Bronze is month-partitioned: lake/{drug}/{EPD_YYYYMM}/prescribing.jsonl
    glob_pattern = str(lake_dir / "*" / "*" / "prescribing.jsonl")
    with duckdb.connect(str(db_path)) as con:
        _ensure_schemas(con)

        # Build into a staging table first so concurrent readers of
        # silver.prescribing never see a half-built table.
        # The final RENAME is atomic — readers get the old table or the new
        # table; never an empty or partial one.
        con.execute("DROP TABLE IF EXISTS silver.prescribing_new")

        # TRY_CAST is the Silver superpower: it converts bad values to NULL
        # instead of crashing — we surface the problem rather than hiding it.
        con.execute(
            f"""
            CREATE TABLE silver.prescribing_new AS
            SELECT
                -- Date: cast to proper DATE type for correct sorting and filtering
                TRY_CAST(date AS DATE)                                       AS date,

                -- Numeric fields: TRY_CAST surfaces malformed values as NULL
                TRY_CAST(actual_cost AS DOUBLE)                              AS actual_cost,
                TRY_CAST(items       AS BIGINT)                              AS items,
                TRY_CAST(quantity    AS DOUBLE)                              AS quantity,

                -- Identity fields
                row_id,
                setting,
                ccg,
                drug,

                -- Derived: cost per item — NULL when either input is NULL or items=0
                ROUND(
                    TRY_CAST(actual_cost AS DOUBLE)
                    / NULLIF(TRY_CAST(items AS BIGINT), 0),
                    4
                )                                                            AS nic_per_item,

                -- Derived: "YYYY-MM" partition key for monthly aggregations
                -- NHSBSA date field is YYYYMM (e.g. 202506); test fixtures use
                -- ISO dates (2024-01-01). COALESCE tries YYYYMM first, then ISO.
                COALESCE(
                    STRFTIME(TRY_STRPTIME(CAST(date AS VARCHAR), '%Y%m'), '%Y-%m'),
                    STRFTIME(TRY_CAST(date AS DATE), '%Y-%m')
                )                                                            AS year_month,

                -- Audit: when was this Silver table last rebuilt?
                now()                                                        AS ingested_at

            FROM read_json(
                '{glob_pattern}',
                format      = 'newline_delimited',
                auto_detect = true
            )
            -- Silver drug filter: promote only the curated analytical subset
            WHERE drug IN ({_SILVER_DRUGS_SQL})
            -- Drop rows where both cost AND items are NULL — analytically worthless
            AND NOT (actual_cost IS NULL AND items IS NULL)
            """
        )

        row_count: int = (
            con.execute("SELECT COUNT(*) FROM silver.prescribing_new").fetchone()
            or (0,)
        )[0]

        # Atomic swap: drop old, rename new → readers never see an empty table
        con.execute("DROP TABLE IF EXISTS silver.prescribing")
        con.execute("ALTER TABLE silver.prescribing_new RENAME TO prescribing")

    _log.info("Silver built: %d rows → silver.prescribing in %s", row_count, db_path)
    return row_count


def build_silver_for_range(
    lake_dir: pathlib.Path,
    db_path: pathlib.Path,
    from_month: str,
    to_month: str,
) -> int:
    """Re-process a date-range partition of Silver — the backfill operation.

    This is **partition-level idempotency**: instead of rebuilding the entire
    Silver table, we delete only the rows whose ``year_month`` falls within
    the requested range and re-insert from Bronze.

    Why this matters
    ----------------
    Full-table rebuilds (``build_silver``) are fine when the Bronze lake is
    small.  At scale, re-reading years of history just to fix one month is
    expensive.  Partition-level backfill solves this:

    1. **Delete** only the target months from Silver.
    2. **Re-read** Bronze files, filtering to the same months.
    3. **Insert** the corrected rows.

    The result is identical to a full rebuild for the affected range, but
    leaves untouched months exactly as they were.

    Common backfill scenarios
    -------------------------
    - A new drug is added to ``DRUG_CODES``: fetch its Bronze files, then
      backfill Silver/Gold so the new drug appears in all historical reports.
    - A Silver transformation bug is fixed: backfill the affected month range
      to apply the correct logic without touching other months.
    - A schema change adds a column: a full rebuild is simpler, but a range
      backfill works equally well when the column is derived from existing data.

    Parameters
    ----------
    lake_dir:
        Root of the Bronze lake, e.g. ``Path("lake")``.
    db_path:
        Path to the DuckDB file containing ``silver.prescribing``.
    from_month:
        Start of the backfill window, inclusive, as ``"YYYY-MM"``,
        e.g. ``"2023-01"``.
    to_month:
        End of the backfill window, inclusive, as ``"YYYY-MM"``,
        e.g. ``"2023-12"``.

    Returns
    -------
    int
        Number of rows inserted into ``silver.prescribing`` for the range.

    Raises
    ------
    RuntimeError
        If ``silver.prescribing`` does not yet exist.  Run ``build_silver``
        first to create the table, then use this function for subsequent
        partial updates.

    Example
    -------
    ::

        # Add lisinopril Bronze files, then backfill only 2023
        rows = build_silver_for_range(
            lake_dir=Path("lake"),
            db_path=Path("pipeline.duckdb"),
            from_month="2023-01",
            to_month="2023-12",
        )
        print(f"Backfilled {rows:,} rows for 2023-01 → 2023-12")
    """
    # Bronze is month-partitioned: lake/{drug}/{EPD_YYYYMM}/prescribing.jsonl
    glob_pattern = str(lake_dir / "*" / "*" / "prescribing.jsonl")

    with duckdb.connect(str(db_path)) as con:
        _ensure_schemas(con)

        # Verify Silver table exists — backfill requires a pre-existing table.
        exists: bool = (
            con.execute(
                """
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = 'silver' AND table_name = 'prescribing'
                """
            ).fetchone()
            or (False,)
        )[0]

        if not exists:
            raise RuntimeError(
                "silver.prescribing does not exist. "
                "Run build_silver() first to create the full Silver table, "
                "then use build_silver_for_range() for partial updates."
            )

        # Step 1: Delete existing Silver rows for the target date range.
        # This makes the operation idempotent: running it twice produces
        # the same result as running it once.
        deleted: int = (
            con.execute(
                "DELETE FROM silver.prescribing WHERE year_month BETWEEN ? AND ?",
                [from_month, to_month],
            ).fetchone()
            or (0,)
        )[0]

        _log.info(
            "Backfill: deleted %d Silver rows for %s → %s",
            deleted,
            from_month,
            to_month,
        )

        # Step 2: Re-read Bronze and insert only the rows in the target range.
        # The WHERE clause on year_month filters at read time — DuckDB pushes
        # this predicate down into the JSON scan for efficiency.
        con.execute(
            f"""
            INSERT INTO silver.prescribing
            SELECT
                TRY_CAST(date AS DATE)                                       AS date,
                TRY_CAST(actual_cost AS DOUBLE)                              AS actual_cost,
                TRY_CAST(items       AS BIGINT)                              AS items,
                TRY_CAST(quantity    AS DOUBLE)                              AS quantity,
                row_id,
                setting,
                ccg,
                drug,
                ROUND(
                    TRY_CAST(actual_cost AS DOUBLE)
                    / NULLIF(TRY_CAST(items AS BIGINT), 0),
                    4
                )                                                            AS nic_per_item,
                COALESCE(
                    STRFTIME(TRY_STRPTIME(CAST(date AS VARCHAR), '%Y%m'), '%Y-%m'),
                    STRFTIME(TRY_CAST(date AS DATE), '%Y-%m')
                )                                                            AS year_month,
                now()                                                        AS ingested_at
            FROM read_json(
                '{glob_pattern}',
                format      = 'newline_delimited',
                auto_detect = true
            )
            WHERE
                drug IN ({_SILVER_DRUGS_SQL})
                AND NOT (actual_cost IS NULL AND items IS NULL)
                AND STRFTIME(TRY_CAST(date AS DATE), '%Y-%m')
                    BETWEEN '{from_month}' AND '{to_month}'
            """,
        )

        inserted: int = (
            con.execute(
                "SELECT COUNT(*) FROM silver.prescribing WHERE year_month BETWEEN ? AND ?",
                [from_month, to_month],
            ).fetchone()
            or (0,)
        )[0]

    _log.info(
        "Backfill complete: %d rows inserted into silver.prescribing for %s → %s",
        inserted,
        from_month,
        to_month,
    )
    return inserted


def build_dim_practice(
    db_path: pathlib.Path = pathlib.Path("pipeline.duckdb"),
) -> int:
    """Build a Slowly Changing Dimension (SCD Type 2) table for GP practices.

    Demonstrates **Veracity** — dimension data drifts over time, and tracking
    that drift is essential for accurate point-in-time analysis.

    Background
    ----------
    In July 2022 England's 106 Clinical Commissioning Groups (CCGs) were
    dissolved and replaced by 42 Integrated Care Boards (ICBs).  Every GP
    practice that existed before that date has *two* valid answers to
    "which commissioning body are you in?" depending on the analysis date.
    Without SCD Type 2, a trend query crossing that boundary silently
    attributes old spend to the wrong ICB.

    SCD Types compared
    ------------------
    - **Type 1** (overwrite): update the row in place — simple, but history
      is lost.  Use only for correcting data-entry errors.
    - **Type 2** (versioned rows): add a new row for each change, keeping
      the old row with ``valid_to`` set — full history preserved.  Industry
      default for audit-required healthcare analytics.
    - **Type 3** (add column): add a ``previous_ccg`` column — keeps only
      one level of history.  Rarely used in practice.

    How this works
    --------------
    1. Group Silver records by (practice, setting, ccg) to find distinct
       attribute combinations.
    2. Use ``LEAD()`` to discover when the *next* version of a practice starts
       — that date becomes ``valid_to`` for the current version.
    3. Practices whose attributes never changed have one row with
       ``valid_to = NULL`` and ``is_current = TRUE``.
    4. A point-in-time query uses:
       ``WHERE valid_from <= :date AND (valid_to IS NULL OR valid_to > :date)``

    Parameters
    ----------
    db_path:
        Path to the DuckDB file containing ``silver.prescribing``.

    Returns
    -------
    int
        Total rows in ``gold.dim_practice`` (one row per practice-version).
    """
    with duckdb.connect(str(db_path)) as con:
        _ensure_schemas(con)

        con.execute(
            """
            CREATE OR REPLACE TABLE gold.dim_practice AS
            WITH practice_versions AS (
                -- Step 1: collapse each distinct (practice, setting, ccg) combination
                -- to its earliest and latest observed date.
                -- Each combination = one "version" of the practice.
                SELECT
                    row_id              AS practice_id,
                    setting,
                    ccg,
                    MIN(date)           AS first_seen,
                    MAX(date)           AS last_seen
                FROM silver.prescribing
                GROUP BY row_id, setting, ccg
            ),
            with_successor AS (
                -- Step 2: use LEAD() to find when the next version starts.
                -- That start date becomes valid_to for the current version.
                SELECT
                    practice_id,
                    setting,
                    ccg,
                    first_seen                          AS valid_from,
                    LEAD(first_seen) OVER (
                        PARTITION BY practice_id
                        ORDER BY first_seen
                    )                                   AS valid_to,
                    ROW_NUMBER() OVER (
                        PARTITION BY practice_id
                        ORDER BY first_seen
                    )                                   AS version_num
                FROM practice_versions
            )
            -- Step 3: add is_current flag.
            -- The most recent version has no successor, so valid_to IS NULL.
            SELECT
                practice_id,
                setting,
                ccg,
                valid_from,
                valid_to,
                version_num,
                (valid_to IS NULL)                      AS is_current
            FROM with_successor
            ORDER BY practice_id, valid_from
            """
        )

        row_count: int = (
            con.execute("SELECT COUNT(*) FROM gold.dim_practice").fetchone() or (0,)
        )[0]
        changed: int = (
            con.execute(
                "SELECT COUNT(*) FROM gold.dim_practice WHERE version_num > 1"
            ).fetchone()
            or (0,)
        )[0]

    _log.info(
        "gold.dim_practice: %d rows (%d practice-versions with changed attributes)",
        row_count,
        changed,
    )
    return row_count


def build_gold(
    db_path: pathlib.Path = pathlib.Path("pipeline.duckdb"),
) -> dict[str, int]:
    """Build Gold aggregation tables from Silver.

    Demonstrates **Volume** — Gold tables summarise millions of raw records
    into concise, business-ready results that load instantly.

    Gold tables created
    -------------------
    ``gold.drug_summary``
        One row per drug: total items, total cost, avg NIC per item,
        unique practice count, and months of data.
        **Use case:** top-line KPI dashboard.

    ``gold.drug_monthly_spend``
        One row per drug × month: total items and cost.
        **Use case:** trend charts, spend forecasting.

    ``gold.practice_leaderboard``
        Top 20 practices per drug ranked by total items prescribed.
        **Use case:** identifying high-volume prescribers.

    Parameters
    ----------
    db_path:
        Path to the DuckDB file containing ``silver.prescribing``.

    Returns
    -------
    dict[str, int]
        Mapping of Gold table name → row count.
    """
    with duckdb.connect(str(db_path)) as con:
        _ensure_schemas(con)

        # Build each Gold table into a _new staging table, then atomically
        # swap so concurrent notebook queries never see a partial result.

        # ------------------------------------------------------------------
        # Gold table 1 — drug_summary (top-line KPIs)
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS gold.drug_summary_new")
        con.execute(
            """
            CREATE TABLE gold.drug_summary_new AS
            SELECT
                drug,
                -- Volume V: the scale of NHS prescribing in one number
                SUM(items)                                          AS total_items,
                ROUND(SUM(actual_cost), 2)                         AS total_cost_gbp,
                ROUND(
                    SUM(actual_cost) / NULLIF(SUM(items), 0),
                    4
                )                                                  AS avg_nic_per_item,
                COUNT(DISTINCT row_id)                             AS unique_practices,
                COUNT(DISTINCT year_month)                         AS months_of_data
            FROM silver.prescribing
            GROUP BY drug
            ORDER BY total_items DESC
            """
        )
        con.execute("DROP TABLE IF EXISTS gold.drug_summary")
        con.execute("ALTER TABLE gold.drug_summary_new RENAME TO drug_summary")

        # ------------------------------------------------------------------
        # Gold table 2 — drug_monthly_spend (trend data)
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS gold.drug_monthly_spend_new")
        con.execute(
            """
            CREATE TABLE gold.drug_monthly_spend_new AS
            SELECT
                drug,
                year_month,
                SUM(items)                      AS total_items,
                ROUND(SUM(actual_cost), 2)      AS total_cost_gbp
            FROM silver.prescribing
            GROUP BY drug, year_month
            ORDER BY drug, year_month
            """
        )
        con.execute("DROP TABLE IF EXISTS gold.drug_monthly_spend")
        con.execute(
            "ALTER TABLE gold.drug_monthly_spend_new RENAME TO drug_monthly_spend"
        )

        # ------------------------------------------------------------------
        # Gold table 3 — practice_leaderboard (top prescribers)
        # QUALIFY filters the window function result without a subquery
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS gold.practice_leaderboard_new")
        con.execute(
            """
            CREATE TABLE gold.practice_leaderboard_new AS
            SELECT
                drug,
                row_id                          AS practice_id,
                SUM(items)                      AS total_items,
                ROUND(SUM(actual_cost), 2)      AS total_cost_gbp,
                ROW_NUMBER() OVER (
                    PARTITION BY drug
                    ORDER BY SUM(items) DESC
                )                               AS rank
            FROM silver.prescribing
            GROUP BY drug, row_id
            QUALIFY rank <= 20
            ORDER BY drug, rank
            """
        )
        con.execute("DROP TABLE IF EXISTS gold.practice_leaderboard")
        con.execute(
            "ALTER TABLE gold.practice_leaderboard_new RENAME TO practice_leaderboard"
        )

        counts: dict[str, int] = {}
        for table in (
            "gold.drug_summary",
            "gold.drug_monthly_spend",
            "gold.practice_leaderboard",
        ):
            counts[table] = (
                con.execute(f"SELECT COUNT(*) FROM {table}").fetchone() or (0,)
            )[0]
            _log.info("%s: %d rows", table, counts[table])

    return counts
