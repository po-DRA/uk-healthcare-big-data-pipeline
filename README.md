# UK Healthcare Big Data Pipeline

A hands-on, self-contained course for healthcare students demonstrating all **4 V's of big data** — Volume, Velocity, Variety, and Veracity — using two real UK open data sources, a modern Python toolchain, and Prefect orchestration.

You will build a working pipeline that fetches NHS prescribing records and clinical prose, stores them in a local data lake, queries them with DuckDB SQL, transforms them with Polars lazy evaluation, extracts NLP signals from patient-facing text, and visualises everything in a 2×2 clinical insight chart.

---

## Who This Is For

- Healthcare informatics students (BSc, MSc, PhD)
- Pharmacy, nursing, and clinical data science students
- Anyone with basic Python familiarity who wants to understand data engineering concepts in a healthcare context
- No prior data engineering experience required

---

## What You Will Learn

- **The 4 V's of big data** — Volume, Velocity, Variety, Veracity — and where each appears in NHS data
- **Parallel fetching** with `concurrent.futures.ThreadPoolExecutor` for I/O-bound workloads
- **DuckDB** — in-process SQL analytics querying raw files directly (no database server required)
- **Polars lazy evaluation** — reading and transforming large datasets without loading everything into memory
- **NLP on clinical text** — tokenisation and term-frequency analysis on NHS.uk patient information pages
- **Prefect orchestration** — wrapping a pipeline in `@flow` / `@task` with automatic retries, logging, and a visual DAG UI
- **Modern Python project tooling** — `uv`, `ruff`, `mypy`, `pre-commit`, GitHub Actions CI

---

## Data Sources

| Source | URL | Format | Licence |
|--------|-----|--------|---------|
| OpenPrescribing API | https://openprescribing.net/api/ | JSON (REST API) | Open Government Licence v3.0 |
| NHS.uk Medicines Pages | https://www.nhs.uk/medicines/ | HTML (scraped) | Open Government Licence v3.0 |

No API keys required for either source.

### The Four Drugs Used Throughout

| Drug | BNF Code | Indication |
|------|----------|-----------|
| Metformin | 0601023A0 | Type 2 diabetes (most prescribed drug in England) |
| Atorvastatin | 0212000B0 | High cholesterol |
| Lisinopril | 0205051R0 | Hypertension / Heart failure |
| Amlodipine | 0206020A0 | Hypertension / Angina |

---

## Setup

### Prerequisites

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) — fast Python package manager
- `git`

### Install

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/uk-healthcare-big-data-pipeline.git
cd uk-healthcare-big-data-pipeline

# Install all dependencies (creates .venv automatically)
uv sync --all-extras

# Install pre-commit hooks
uv run pre-commit install
```

### Open the First Notebook

```bash
uv run marimo edit notebooks/00_introduction.py
```

Your browser will open at `http://localhost:2718`. Run notebooks in order 00 → 06.

---

## Running the Full Pipeline with Prefect

After completing the notebooks, run the production-grade Prefect orchestration:

```bash
# Terminal 1 — start Prefect server and UI
uv run prefect server start

# Terminal 2 — execute the full pipeline
uv run python flows/pipeline_flow.py
```

Open **http://localhost:4200** in your browser to watch the pipeline run in real time, see task retries, and inspect logs.

---

## Project Structure

```
uk-healthcare-big-data-pipeline/
├── .devcontainer/
│   └── devcontainer.json        # GitHub Codespaces config
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions: lint + type-check + test
├── .pre-commit-config.yaml      # ruff, mypy, trailing-whitespace
├── pyproject.toml               # uv project, ruff/mypy/pytest configuration
├── .gitignore
├── src/
│   └── pipeline/
│       ├── fetch.py             # HTTP acquisition (httpx + BeautifulSoup)
│       ├── lake.py              # Lake read/write (JSONL + JSON)
│       ├── transform.py         # Polars lazy transforms + veracity report
│       ├── nlp.py               # Tokenisation + term frequency (Polars)
│       └── visualise.py         # matplotlib chart functions
├── flows/
│   └── pipeline_flow.py         # Prefect @flow + @task with retries
├── notebooks/
│   ├── 00_introduction.py       # Why pipelines? The 4 V's. Data sources.
│   ├── 01_parallel_fetch.py     # ThreadPoolExecutor, Volume + Velocity
│   ├── 02_raw_lake.py           # Write lake, Variety made visible
│   ├── 03_duckdb_query.py       # SQL on raw JSONL, Volume
│   ├── 04_polars_transform.py   # Lazy eval, Veracity report
│   ├── 05_nlp_unstructured.py   # Clinical text NLP, Variety
│   └── 06_join_and_visualise.py # DuckDB JOIN + matplotlib 2×2
├── tests/
│   ├── test_fetch.py
│   ├── test_transform.py
│   ├── test_nlp.py
│   └── test_pipeline.py         # @pytest.mark.slow — live API tests
└── lake/                        # Created at runtime, gitignored
```

---

## The 4 V's in This Pipeline

**Volume** — The OpenPrescribing API returns practice-level monthly prescribing records going back years. Across four drugs you will fetch hundreds of thousands of rows representing millions of NHS prescriptions. No spreadsheet can handle this; a pipeline can.

**Velocity** — NHS BSA publishes prescribing data monthly. Our `ThreadPoolExecutor` fetches all 8 sources in parallel, cutting acquisition time several-fold compared to sequential calls. The Prefect `@task(retries=2, retry_delay_seconds=exponential_backoff(...))` decorator handles API flakiness automatically — a production pipeline must tolerate transient failures.

**Variety** — The pipeline ingests two completely different data formats from two completely different sources: structured JSON records from a REST API, and unstructured HTML prose scraped from NHS.uk patient information pages. Both are persisted in the same `lake/` directory tree (as JSONL and JSON respectively), and later joined with a single DuckDB SQL query.

**Veracity** — Real NHS data has nulls, missing item counts, and inconsistent codes. The `veracity_report()` function quantifies exactly how many nulls exist in each key field, turning an implicit data-quality assumption into an explicit, measurable fact. Trustworthy analysis starts with knowing what you don't trust.

---

## Running the Tests

```bash
# Run all unit tests (fast, no internet required)
uv run pytest tests/ -v -m "not slow"

# Run the slow integration tests (requires internet, hits real APIs)
uv run pytest tests/ -v -m slow

# Run linting
uv run ruff check src/ flows/

# Run type checking
uv run mypy src/
```

---

## GitHub Codespaces

This repository includes a `.devcontainer/devcontainer.json` configuration.
Click **"Open in Codespaces"** on GitHub and the environment will be set up automatically.

Ports forwarded:
- **2718** — Marimo notebook UI
- **4200** — Prefect orchestration UI

---

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

## Licence

- **Code**: MIT Licence — see [LICENSE](LICENSE)
- **Data**: Open Government Licence v3.0 — NHS BSA and NHS England open data

## Citation

> Inspired by: Pessini, L. (2025). *Building End-to-End Data Pipelines: A Hands-On Guide for Data Scientists*. Medium.
