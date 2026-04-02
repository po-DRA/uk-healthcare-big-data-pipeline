import marimo

__generated_with = "0.6.0"
app = marimo.App(title="00 · Introduction — UK Healthcare Big Data Pipeline")


@app.cell
def __():
    import marimo as mo

    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # UK Healthcare Big Data Pipeline
        ## Notebook 00 — Introduction

        **Learning objective:** Understand _why_ data pipelines matter in healthcare,
        what the 4 V's of big data mean in practice, and how this course is structured.

        **V's demonstrated:** All four — introduced conceptually here.

        **Estimated time:** 10 minutes (reading + discussion)
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Why Healthcare Students Need to Understand Data Pipelines

        - **Scale demands automation.** NHS England processes over 1 billion prescription items
          per year. No human analyst can wrangle this manually — pipelines make it repeatable,
          auditable, and fast.
        - **Evidence-based decisions require clean, joined data.** Prescribing cost dashboards,
          pharmacovigilance signals, and NICE guidance reviews all depend on reliably ingesting
          and transforming heterogeneous data sources (structured records _and_ clinical prose).
        - **Reproducibility is patient safety.** A pipeline that is tested, version-controlled,
          and documented can be re-run by a colleague six months later and produce the same result.
          Ad-hoc spreadsheet analysis cannot.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## The 4 V's of Big Data — in This Pipeline

        | V | Definition | Where you see it here | When you see it |
        |---|---|---|---|
        | **Volume** | Data too large for a single spreadsheet | ~1 million practice-level prescribing records across 4 drugs | Notebook 01 — total row count printed after fetch |
        | **Velocity** | Data generated or needed rapidly; systems must respond in near-real time | Monthly NHS BSA data refresh; parallel fetch cuts retrieval time; Prefect retries handle API flakiness | Notebook 01 — parallel vs sequential timing |
        | **Variety** | Multiple incompatible formats and sources | JSONL (structured) + JSON (semi-structured NHS pages) + raw HTML prose — coexisting in the same lake | Notebook 02 — two file formats side by side |
        | **Veracity** | Uncertainty about data quality and trustworthiness | Null costs, missing item counts, inconsistent setting codes | Notebook 04 — veracity report table |
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Data Sources

        ### 1. OpenPrescribing API — Structured
        - **URL:** https://openprescribing.net/api/
        - **Provider:** Bennett Institute for Applied Data Science, University of Oxford
        - **Licence:** Open Government Licence v3.0
        - **What it contains:** Monthly NHS prescribing spend and item counts at GP practice level,
          derived from NHS Business Services Authority (BSA) data.
        - **No API key required.** Just a BNF code in the query string.

        ### 2. NHS.uk Medicines Pages — Unstructured
        - **URL:** https://www.nhs.uk/medicines/{drug}/
        - **Provider:** NHS England
        - **Licence:** Open Government Licence v3.0
        - **What it contains:** Clinical prose — side effects, contraindications, drug interactions —
          written for patients but containing rich medical terminology.
        - **No API key required.** Public web pages scraped with httpx + BeautifulSoup.

        ### The Four Drugs Used Throughout
        | Drug | BNF Code | Indication |
        |------|----------|-----------|
        | Metformin | 0601023A0 | Type 2 diabetes |
        | Atorvastatin | 0212000B0 | Hypercholesterolaemia |
        | Lisinopril | 0205051R0 | Hypertension / Heart failure |
        | Amlodipine | 0206020A0 | Hypertension / Angina |
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Pipeline Architecture — Medallion Data Lake

        ```
        ┌─────────────────────────┐     ┌──────────────────────────────┐
        │  OpenPrescribing API    │     │  NHS.uk Medicines Pages      │
        │  (structured JSONL)     │     │  (unstructured HTML prose)   │
        └────────────┬────────────┘     └───────────────┬──────────────┘
                     │                                   │
                     └──────────┬────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │  ThreadPoolExecutor   │   ← Velocity (parallel fetch)
                    │  (8 concurrent tasks) │
                    └───────────┬───────────┘
                                │
        ╔═══════════════════════▼═══════════════════════════╗
        ║  BRONZE LAYER  (lake/)                            ║  ← Variety
        ║  lake/*/prescribing.jsonl  (raw JSONL)            ║
        ║  lake/*/nhs_pages.json     (raw JSON)             ║
        ║  Write-once — never modified                      ║
        ╚═══════════════════════╦═══════════════════════════╝
                                ║  build_silver()
                    ┌───────────▼───────────┐
                    │  Polars LazyFrame     │   ← Volume + Veracity
                    │  (type cast, verify)  │
                    └───────────┬───────────┘
                                │
        ╔═══════════════════════▼═══════════════════════════╗
        ║  SILVER LAYER  (silver.prescribing in DuckDB)     ║  ← Veracity
        ║  Typed, cleaned, null-checked                     ║
        ║  + nic_per_item + year_month + ingested_at        ║
        ╚═══════════════════════╦═══════════════════════════╝
                                ║  build_gold()
        ╔═══════════════════════▼═══════════════════════════╗
        ║  GOLD LAYER  (gold.* tables in DuckDB)            ║  ← Volume
        ║  gold.drug_summary         — KPI dashboard        ║
        ║  gold.drug_monthly_spend   — trend charts         ║
        ║  gold.practice_leaderboard — top prescribers      ║
        ╚═══════════════════════╦═══════════════════════════╝
                                ║
              ┌─────────────────╩──────────────────────┐
              │                                        │
        ┌─────▼──────┐                      ┌──────────▼──────────┐
        │  DuckDB    │                      │  BeautifulSoup NLP  │  ← Variety
        │  JOIN +    │                      │  + Counter terms    │
        │  Parquet   │                      └──────────┬──────────┘
        └─────┬──────┘                                 │
              └───────────────┬────────────────────────┘
                              │
                  ┌───────────▼───────────┐
                  │    matplotlib 2×2     │   ← Clinical insights
                  │    outputs/           │
                  └───────────────────────┘

        Orchestration: Prefect @flow + @task (retries, exponential backoff, Prefect UI)
        Streaming sim: notebook 08 — replay Bronze as micro-batches via generator
        ```
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## How to Run This Course

        Run notebooks in order from 00 to 06. Each notebook builds on the outputs
        of the previous one.

        ```bash
        # Open a specific notebook
        uv run marimo edit notebooks/00_introduction.py

        # Or run the full pipeline end-to-end with Prefect
        # Terminal 1:
        uv run prefect server start

        # Terminal 2:
        uv run python flows/pipeline_flow.py
        ```

        | Notebook | Topic | V's |
        |----------|-------|-----|
        | 00 | Introduction | All (conceptual) |
        | 01 | Parallel fetch | Volume, Velocity |
        | 02 | Raw data lake (Bronze) | Variety |
        | 03 | DuckDB SQL | Volume |
        | 04 | Polars transform | Volume, Veracity |
        | 05 | NLP on NHS text | Variety |
        | 06 | Join + visualise | All four |
        | 07 | Medallion architecture (Bronze → Silver → Gold) | Veracity, Volume |
        | 08 | Streaming simulation (generators + DuckDB) | Velocity |
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Further Reading

        - [Pessini's Pipeline Guide](https://pessini.medium.com/building-end-to-end-data-pipelines-a-hands-on-guide-for-data-scientists-part-1-adcdc7bce22a) — the article that inspired this course
        - [Polars User Guide](https://docs.pola.rs/) — lazy evaluation, expressions, and performance
        - [DuckDB Documentation](https://duckdb.org/docs/) — in-process SQL analytics
        - [OpenPrescribing About](https://openprescribing.net/about/) — context on the prescribing data
        - [OpenPrescribing API Docs](https://openprescribing.net/api/) — query parameters and endpoints
        - [NHS BSA Open Data Portal](https://opendata.nhsbsa.net/) — broader NHS open data catalogue
        - [The 4 V's of Big Data (IBM)](https://www.ibm.com/think/topics/4-vs-of-big-data) — conceptual overview
        - [Bennett Institute for Applied Data Science](https://www.bennett.ox.ac.uk/) — builders of OpenPrescribing

        ---

        ## Summary

        You now understand:
        - Why data pipelines are essential in NHS/healthcare settings
        - What each of the 4 V's means and where you will encounter them in this pipeline
        - The two data sources and the four drugs used throughout

        **Reflection question:** Before running notebook 01, write down your estimate:
        how many GP practice-level prescribing records do you think exist for metformin
        across all of England? We'll check your guess in the next notebook.

        **→ Next: [01_parallel_fetch.py](01_parallel_fetch.py) — fetch all data in parallel**
        """
    )
    return


if __name__ == "__main__":
    app.run()
