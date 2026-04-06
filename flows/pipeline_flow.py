"""
flows/pipeline_flow.py — Prefect orchestration layer.

Wraps the pure pipeline functions from src/pipeline/ as Prefect tasks,
adding automatic retries, structured logging, and a visual DAG in the
Prefect UI (localhost:4200).

Teaching points
---------------
- @task with retries=2 + exponential_backoff demonstrates **Velocity**:
  real-world APIs are unreliable; a production pipeline must handle
  transient failures automatically.
- log_prints=True routes all print() calls to Prefect's log stream,
  making output visible in the UI without changing any pipeline code.
- The @flow orchestrates task dependencies — Prefect infers the DAG
  from Python data-flow, not from explicit wiring.

How to run
----------
Terminal 1 (start the Prefect server + UI):
    uv run prefect server start

Terminal 2 (execute the flow):
    uv run python flows/pipeline_flow.py

Then open http://localhost:4200 to watch the run in real time.
"""

from __future__ import annotations

import pathlib

import duckdb
import polars as pl
from prefect import flow, task
from prefect.tasks import exponential_backoff

from pipeline.contracts import DEFAULT_NULL_THRESHOLDS, SilverDQViolation
from pipeline.fetch import DRUG_CODES, fetch_nhsbsa, fetch_nhs_pages
from pipeline.lake import write_lake
from pipeline.lineage import dataset, lineage_job
from pipeline.medallion import build_dim_practice, build_gold, build_silver
from pipeline.nlp import top_terms
from pipeline.transform import build_prescribing_df, veracity_report

# ---------------------------------------------------------------------------
# Drug catalogue — derived from the single source of truth in pipeline.fetch
# ---------------------------------------------------------------------------
DRUGS: list[tuple[str, str]] = [
    (bnf_code, name) for name, bnf_code in DRUG_CODES.items()
]

LAKE_DIR = pathlib.Path("lake")
DB_PATH = pathlib.Path("pipeline.duckdb")


# ---------------------------------------------------------------------------
# Tasks — each wraps a pure pipeline function
# ---------------------------------------------------------------------------


@task(
    name="Fetch NHSBSA EPD",
    retries=2,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    log_prints=True,
)
def fetch_prescribing_task(bnf_code: str, drug_name: str) -> dict:
    """Fetch structured prescribing records by streaming the NHSBSA EPD CSV.

    Demonstrates **Velocity** — streams the latest monthly file and stops
    early once enough rows per drug are collected (~50MB, ~10s).

    Parameters
    ----------
    bnf_code:
        BNF chemical-substance code.
    drug_name:
        Human-readable drug name.

    Returns
    -------
    dict
        Prescribing payload as returned by ``fetch_nhsbsa()``.
    """
    return fetch_nhsbsa(bnf_code, drug_name)


@task(
    name="Fetch NHS Pages",
    retries=2,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    log_prints=True,
)
def fetch_nhs_task(drug_name: str) -> dict:
    """Fetch and parse NHS.uk clinical prose pages for a drug.

    Demonstrates **Variety** — fetches unstructured HTML and extracts
    structured sections, handled with the same retry safety as the API.

    Parameters
    ----------
    drug_name:
        Lower-case drug name matching the NHS.uk URL slug.

    Returns
    -------
    dict
        NHS pages payload as returned by ``fetch_nhs_pages()``.
    """
    return fetch_nhs_pages(drug_name)


@task(name="Write to Data Lake", log_prints=True)
def write_lake_task(payload: dict, base_dir: pathlib.Path) -> pathlib.Path:
    """Persist a fetched payload to the local data lake.

    Parameters
    ----------
    payload:
        Dict from ``fetch_prescribing_task`` or ``fetch_nhs_task``.
    base_dir:
        Root of the lake directory.

    Returns
    -------
    pathlib.Path
        Path of the file written.
    """
    return write_lake(payload, base_dir)


@task(name="Transform with Polars", log_prints=True)
def transform_task(lake_dir: pathlib.Path) -> pl.DataFrame:
    """Scan the lake lazily, collect, and run the veracity report.

    Demonstrates **Volume** (lazy scan across all drugs) and
    **Veracity** (null quantification).

    Parameters
    ----------
    lake_dir:
        Root of the lake.

    Returns
    -------
    pl.DataFrame
        Veracity report DataFrame.
    """
    lazy = build_prescribing_df(lake_dir)
    print("Query plan (Polars optimiser):")
    print(lazy.explain())

    df = lazy.collect()
    print(f"Total prescribing records collected: {len(df):,}")

    report = veracity_report(df)
    print("\nVeracity report:")
    print(report)
    return report


@task(name="Build Silver Layer", log_prints=True)
def build_silver_task(lake_dir: pathlib.Path, db_path: pathlib.Path) -> int:
    """Promote Bronze JSONL files to a typed, cleaned Silver DuckDB table.

    Demonstrates **Veracity** — Silver is the trust boundary where raw data
    is cast to proper types and unresolvable nulls are dropped.

    Parameters
    ----------
    lake_dir:
        Root of the Bronze lake.
    db_path:
        Path to the DuckDB file.

    Returns
    -------
    int
        Row count written to ``silver.prescribing``.
    """
    with lineage_job(
        "build_silver",
        inputs=[dataset(f"lake/{name}/prescribing.jsonl") for _, name in DRUGS],
        outputs=[dataset("silver.prescribing")],
    ):
        return build_silver(lake_dir, db_path)


@task(name="Validate Silver Quality", log_prints=True)
def validate_silver_task(
    db_path: pathlib.Path,
    thresholds: dict[str, float] | None = None,
) -> dict[str, float]:
    """Gate: fail the pipeline if Silver null-rate thresholds are breached.

    Demonstrates **Veracity** — this is the difference between *measuring*
    data quality (``veracity_report``) and *enforcing* it.  If any field's
    null rate exceeds its threshold the task raises ``SilverDQViolation``
    and the Prefect flow is marked FAILED, preventing bad data from
    propagating into Gold aggregations.

    Parameters
    ----------
    db_path:
        Path to the DuckDB file containing ``silver.prescribing``.
    thresholds:
        Optional override mapping ``field_name → max_null_pct``.  Defaults
        to ``pipeline.contracts.DEFAULT_NULL_THRESHOLDS``.

    Returns
    -------
    dict[str, float]
        Actual null percentages per checked field (for logging / UI display).

    Raises
    ------
    SilverDQViolation
        If any field's null rate exceeds its threshold.
    """
    effective = thresholds if thresholds is not None else DEFAULT_NULL_THRESHOLDS

    with duckdb.connect(str(db_path)) as con:
        total = con.execute("SELECT COUNT(*) FROM silver.prescribing").fetchone()[0]

        if total == 0:
            print("  ⚠ Silver table is empty — skipping DQ gate")
            return {}

        null_pcts: dict[str, float] = {}
        for field in effective:
            null_count = con.execute(
                f"SELECT COUNT(*) FROM silver.prescribing WHERE {field} IS NULL"
            ).fetchone()[0]
            null_pcts[field] = round(null_count * 100.0 / total, 2)

    print("  Silver null-rate check:")
    violations: dict[str, dict[str, float]] = {}
    for field, pct in null_pcts.items():
        threshold = effective[field]
        status = "✗ FAIL" if pct > threshold else "✓ pass"
        print(f"    {status}  {field}: {pct:.2f}% nulls (limit {threshold:.1f}%)")
        if pct > threshold:
            violations[field] = {"null_pct": pct, "threshold": threshold}

    if violations:
        raise SilverDQViolation(violations)

    return null_pcts


@task(name="Build SCD Type 2 Dimension", log_prints=True)
def build_dim_practice_task(db_path: pathlib.Path) -> int:
    """Build the SCD Type 2 GP practice dimension table.

    Demonstrates **Veracity** — tracks how practice attributes (setting, CCG)
    change over time, enabling accurate point-in-time analysis across
    the July 2022 CCG → ICB reorganisation boundary.

    Parameters
    ----------
    db_path:
        Path to the DuckDB file containing ``silver.prescribing``.

    Returns
    -------
    int
        Total rows in ``gold.dim_practice``.
    """
    return build_dim_practice(db_path)


@task(name="Build Gold Layer", log_prints=True)
def build_gold_task(db_path: pathlib.Path) -> dict:
    """Build Gold aggregation tables from Silver.

    Demonstrates **Volume** — Gold tables summarise millions of raw records
    into concise, business-ready results (KPIs, trends, leaderboards).

    Parameters
    ----------
    db_path:
        Path to the DuckDB file containing ``silver.prescribing``.

    Returns
    -------
    dict
        Mapping of Gold table name → row count.
    """
    with lineage_job(
        "build_gold",
        inputs=[dataset("silver.prescribing")],
        outputs=[
            dataset("gold.drug_summary"),
            dataset("gold.drug_monthly_spend"),
            dataset("gold.practice_leaderboard"),
        ],
    ):
        return build_gold(db_path)


@task(name="NLP Term Extraction", log_prints=True)
def nlp_task(drug: str, lake_dir: pathlib.Path) -> pl.DataFrame:
    """Extract top clinical terms for a drug from NHS.uk pages.

    Demonstrates **Variety** — structured term frequencies derived from
    unstructured prose.

    Parameters
    ----------
    drug:
        Drug name.
    lake_dir:
        Root of the lake.

    Returns
    -------
    pl.DataFrame
        Top-terms DataFrame for the drug.
    """
    terms_df = top_terms(drug, lake_dir)
    print(f"Top terms for {drug}:")
    print(terms_df.head(5))
    return terms_df


# ---------------------------------------------------------------------------
# Flow — orchestrates all tasks
# ---------------------------------------------------------------------------


@flow(name="UK Healthcare Big Data Pipeline", log_prints=True)
def run_pipeline(
    base_dir: pathlib.Path = LAKE_DIR,
    db_path: pathlib.Path = DB_PATH,
) -> None:
    """Orchestrate the full UK healthcare big data pipeline.

    Fetches structured prescribing data and unstructured NHS pages for four
    drugs in parallel, writes both to the lake, transforms with Polars, and
    extracts NLP terms from clinical text.

    Demonstrates all 4 V's:
      - Volume    : hundreds of thousands of practice-level records
      - Velocity  : parallel fetch + automatic retries on flaky APIs
      - Variety   : JSONL + JSON + HTML in the same lake
      - Veracity  : null quantification via veracity_report()

    Parameters
    ----------
    base_dir:
        Root directory for the data lake. Defaults to ``lake/``.
    """
    print("=" * 60)
    print("UK Healthcare Big Data Pipeline — starting")
    print(f"Lake directory: {base_dir.resolve()}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Step 1: Parallel fetch — 8 tasks (4 drugs × 2 sources)
    # ------------------------------------------------------------------
    print("\n[Step 1] Fetching data in parallel …")

    # Submit all 8 fetch tasks concurrently using Prefect's native .submit().
    # Prefect tracks each as a separate task in the UI.
    prescribing_futures = [
        fetch_prescribing_task.submit(bnf_code, drug_name)
        for bnf_code, drug_name in DRUGS
    ]
    nhs_futures = [fetch_nhs_task.submit(drug_name) for _, drug_name in DRUGS]

    # ------------------------------------------------------------------
    # Step 2: Write results to lake as they complete (partial-success)
    # A single drug failure is logged and skipped — the rest still run.
    # ------------------------------------------------------------------
    print("\n[Step 2] Writing to data lake …")

    failed_drugs: list[str] = []

    for (_bnf_code, drug_name), future in zip(DRUGS, prescribing_futures, strict=False):
        try:
            payload = future.result()
            write_lake_task(payload, base_dir)
        except Exception as exc:
            print(f"  ✗ Prescribing fetch failed for {drug_name}: {exc} — skipping")
            failed_drugs.append(drug_name)

    for (_, drug_name), future in zip(DRUGS, nhs_futures, strict=False):
        try:
            payload = future.result()
            write_lake_task(payload, base_dir)
        except Exception as exc:
            print(f"  ✗ NHS pages fetch failed for {drug_name}: {exc} — skipping")

    if failed_drugs:
        print(
            f"\n  ⚠ {len(failed_drugs)} drug(s) failed to fetch: "
            f"{', '.join(failed_drugs)}. "
            "Medallion layers will be built from available data."
        )

    # ------------------------------------------------------------------
    # Step 3: Polars transformation + veracity report (Bronze → verify)
    # ------------------------------------------------------------------
    print("\n[Step 3] Transforming with Polars (lazy evaluation) …")
    transform_task(base_dir)

    # ------------------------------------------------------------------
    # Step 4: Medallion — Bronze → Silver → Gold
    # ------------------------------------------------------------------
    print("\n[Step 4] Building Silver layer (typed, cleaned DuckDB table) …")
    silver_rows = build_silver_task(base_dir, db_path)
    print(f"  Silver rows: {silver_rows:,}")

    print("\n[Step 4b] Validating Silver data quality (DQ gate) …")
    validate_silver_task(db_path)

    print("\n[Step 5] Building Gold layer (aggregated business tables) …")
    gold_counts = build_gold_task(db_path)
    for table, count in gold_counts.items():
        print(f"  {table}: {count:,} rows")

    print("\n[Step 5b] Building SCD Type 2 practice dimension …")
    dim_rows = build_dim_practice_task(db_path)
    print(f"  gold.dim_practice: {dim_rows:,} practice-version rows")

    # ------------------------------------------------------------------
    # Step 6: NLP on unstructured NHS text (one task per drug)
    # ------------------------------------------------------------------
    print("\n[Step 6] Extracting NLP terms from NHS pages …")
    for _, drug_name in DRUGS:
        nlp_task(drug_name, base_dir)

    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print(f"Lake populated at: {base_dir.resolve()}")
    print("Open http://localhost:4200 to review the run in Prefect UI")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
