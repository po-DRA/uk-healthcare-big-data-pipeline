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
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
from prefect import flow, task
from prefect.tasks import exponential_backoff

from pipeline.fetch import fetch_nhs_pages, fetch_openprescribing
from pipeline.lake import write_lake
from pipeline.nlp import top_terms
from pipeline.transform import build_prescribing_df, veracity_report

# ---------------------------------------------------------------------------
# Drug catalogue — (bnf_code, drug_name) pairs
# ---------------------------------------------------------------------------
DRUGS: list[tuple[str, str]] = [
    ("0601023A0", "metformin"),
    ("0212000B0", "atorvastatin"),
    ("0205051R0", "lisinopril"),
    ("0206020A0", "amlodipine"),
]

LAKE_DIR = pathlib.Path("lake")


# ---------------------------------------------------------------------------
# Tasks — each wraps a pure pipeline function
# ---------------------------------------------------------------------------


@task(
    name="Fetch OpenPrescribing",
    retries=2,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    log_prints=True,
)
def fetch_prescribing_task(bnf_code: str, drug_name: str) -> dict:
    """Fetch structured prescribing records from the OpenPrescribing API.

    Demonstrates **Velocity** — retries with exponential back-off handle
    transient API timeouts gracefully.

    Parameters
    ----------
    bnf_code:
        BNF chemical-substance code.
    drug_name:
        Human-readable drug name.

    Returns
    -------
    dict
        Prescribing payload as returned by ``fetch_openprescribing()``.
    """
    return fetch_openprescribing(bnf_code, drug_name)


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
def run_pipeline(base_dir: pathlib.Path = LAKE_DIR) -> None:
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

    # Submit all 8 fetch tasks concurrently using ThreadPoolExecutor.
    # Prefect tracks each as a separate task in the UI.
    prescribing_futures = [
        fetch_prescribing_task.submit(bnf_code, drug_name)
        for bnf_code, drug_name in DRUGS
    ]
    nhs_futures = [
        fetch_nhs_task.submit(drug_name)
        for _, drug_name in DRUGS
    ]

    # ------------------------------------------------------------------
    # Step 2: Write results to lake as they complete
    # ------------------------------------------------------------------
    print("\n[Step 2] Writing to data lake …")

    for future in prescribing_futures:
        payload = future.result()
        write_lake_task(payload, base_dir)

    for future in nhs_futures:
        payload = future.result()
        write_lake_task(payload, base_dir)

    # ------------------------------------------------------------------
    # Step 3: Polars transformation + veracity report
    # ------------------------------------------------------------------
    print("\n[Step 3] Transforming with Polars (lazy evaluation) …")
    transform_task(base_dir)

    # ------------------------------------------------------------------
    # Step 4: NLP on unstructured NHS text (one task per drug)
    # ------------------------------------------------------------------
    print("\n[Step 4] Extracting NLP terms from NHS pages …")
    for _, drug_name in DRUGS:
        nlp_task(drug_name, base_dir)

    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print(f"Lake populated at: {base_dir.resolve()}")
    print("Open http://localhost:4200 to review the run in Prefect UI")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
