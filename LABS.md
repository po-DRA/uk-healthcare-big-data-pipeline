# Lab Guide — UK Healthcare Big Data Pipeline

Work through these labs in order. Each one builds on the previous — later labs read files that earlier labs write. By the end you will have built a complete, production-grade data pipeline using real NHS open data.

**Time:** ~4–5 hours total, or split across sessions.

---

## Before You Start

**Recommended: GitHub Codespaces (no install needed)**

1. Fork this repository to your own GitHub account
2. Click **Code → Codespaces → Create codespace on master**
3. Wait ~2 minutes for the environment to build
4. In the terminal, verify everything works:

```bash
uv run pytest
```

You should see `193 passed`. You are ready.

**Local setup (alternative)**

```bash
git clone https://github.com/po-DRA/uk-healthcare-big-data-pipeline.git
cd uk-healthcare-big-data-pipeline
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync --all-groups
uv run pytest
```

---

## Lab 00 — Why Pipelines? The 4 V's of Big Data

```bash
uv run marimo edit notebooks/00_introduction.py
```

**What you will do**

Read through the notebook cells. No code to write — this lab is conceptual.

**What you will learn**

The NHS generates data at a scale that makes manual analysis impossible. This lab introduces the four properties of "big data" — Volume, Velocity, Variety, and Veracity — through NHS examples:

| V | NHS example |
|---|-------------|
| Volume | ~1 billion prescriptions per year across England |
| Velocity | NHS 111 call surges must be detected in minutes, not the next morning |
| Variety | Structured prescribing CSVs + unstructured clinical prose on NHS.uk |
| Veracity | A GP practice that changed its name after the 2022 CCG → ICB reorganisation — is it the same practice? |

**Reflection:** Which V do you think causes the most problems in your own work or organisation?

---

## Lab 01 — Fetching Data in Parallel

```bash
uv run marimo edit notebooks/01_parallel_fetch.py
```

**What you will do**

Run the fetch cells. Watch the timing output. The notebook fetches data for four drugs (metformin, atorvastatin, lisinopril, salbutamol) concurrently and shows you how long sequential vs parallel fetching takes.

**What you will learn**

- How to call a REST API (`OpenPrescribing`) to get prescribing records
- How to scrape clinical content from `NHS.uk` pages
- How `ThreadPoolExecutor` fetches multiple drugs in parallel instead of one-at-a-time
- Why parallelism matters at NHS scale: fetching 50 drugs sequentially at 1 second each = 50 seconds; in parallel = ~2 seconds

**Check:** You should see timing output showing parallel is faster than sequential.

---

## Lab 02 — The Bronze Data Lake

```bash
uv run marimo edit notebooks/02_raw_lake.py
```

**What you will do**

Run all cells. After the notebook completes, look at the files that were created:

```bash
ls lake/
ls lake/metformin/
```

**What you will learn**

- The **Bronze layer**: raw data stored exactly as received, never modified
- **JSONL** (one JSON object per line) for prescribing records — good for streaming and incremental appends
- **JSON** for NHS.uk clinical pages — preserves the original structure
- Why Variety is a real engineering challenge: the two formats require completely different parsers

**Check:** You should see `lake/metformin/prescribing.jsonl` and `lake/metformin/nhs_pages.json` (and three more drug directories).

---

## Lab 03 — Querying the Lake with DuckDB

```bash
uv run marimo edit notebooks/03_duckdb_query.py
```

**What you will do**

Run the SQL query cells. Try changing the `WHERE` clause to filter for a different drug or date.

**What you will learn**

- **DuckDB** can query JSONL files directly — no loading into pandas, no database server, no schema setup
- Glob patterns let you query all four drug files in one SQL statement: `read_ndjson('lake/*/prescribing.jsonl')`
- This is the same pattern used in cloud data lakes (S3 + Athena, GCS + BigQuery) — DuckDB is just doing it locally

**Check:** You should see a result table with prescribing cost and items across all drugs.

---

## Lab 04 — Transforming with Polars

```bash
uv run marimo edit notebooks/04_polars_transform.py
```

**What you will do**

Run all cells. Read the veracity report output carefully — it shows what percentage of each field is null.

**What you will learn**

- **Polars lazy evaluation**: `scan_ndjson()` describes the computation without reading any data yet; `collect()` executes it
- Why lazy evaluation matters: Polars (and Spark) can push filters down to the source and skip columns you don't need — crucial at NHS scale
- The **veracity report**: a simple null-percentage table that makes data quality visible before it causes silent errors downstream

**Check:** You should see a veracity report table showing null percentages per field. Some nulls are expected and documented — the report makes them explicit.

---

## Lab 05 — NLP on NHS Clinical Text

```bash
uv run marimo edit notebooks/05_nlp_unstructured.py
```

**What you will do**

Run all cells. Look at the term-frequency tables for each drug. Notice which clinical terms appear most often.

**What you will learn**

- How unstructured clinical prose (NHS.uk patient pages) can be turned into structured signals
- Basic NLP: tokenisation, stop-word removal, term frequency
- Why Variety is hard: the prescribing data has clean columns; the clinical text is free prose that needs parsing before any analysis is possible
- A real clinical insight: metformin's top terms cluster around "diabetes/blood sugar"; salbutamol's cluster around "breathing/inhaler"

**Check:** You should see a frequency table per drug with recognisable clinical terminology.

---

## Lab 06 — Joining Structured and Unstructured Data

```bash
uv run marimo edit notebooks/06_join_and_visualise.py
```

**What you will do**

Run all cells. Three charts will be produced. Export one to PNG using the button in the notebook.

**What you will learn**

- How to join prescribing cost data (structured) with NLP terms (unstructured) in a single DuckDB SQL query
- **Cost per item** by drug — a real NHS efficiency metric
- **Top clinical terms** by drug — connecting cost data to clinical context
- Exporting to Parquet: a columnar format that downstream tools (Gold layer, Spark, BigQuery) can read efficiently

**Check:** You should see three matplotlib charts and a `outputs/` directory with a Parquet file.

---

## Lab 07 — Medallion Architecture + Slowly Changing Dimensions

```bash
uv run marimo edit notebooks/07_medallion_architecture.py
```

**What you will do**

Run all cells in order. After Silver is built, run a query on `silver.prescribing`. After Gold is built, query `gold.drug_summary`.

**What you will learn**

- **Bronze → Silver → Gold**: why you layer transformations instead of transforming raw data in place
  - Bronze: immutable raw files (never touch these)
  - Silver: cleaned, typed, deduplicated — the "single source of truth"
  - Gold: aggregated for a specific analytical use case
- **Idempotency**: running `build_silver()` twice gives the same result as running it once — essential for re-runs and backfills
- **SCD Type 2**: the 2022 NHS reorganisation merged 135 CCGs into 42 ICBs. A GP practice now has two rows in `dim_practice` — one for before July 2022, one for after. Without SCD2, your historical cost reports would be wrong.

**Check:** Query `gold.dim_practice` and find a practice with `version_num = 2` — that practice changed its CCG/ICB in July 2022.

---

## Lab 08 — Streaming Simulation

```bash
uv run marimo edit notebooks/08_streaming_simulation.py
```

**What you will do**

Run all cells. Adjust the `batch_size` slider and re-run to see how it affects micro-batch latency.

**What you will learn**

- The difference between **batch** (process everything once at midnight) and **streaming** (process each record as it arrives)
- **Micro-batch processing**: a middle ground — process small chunks continuously, balancing latency and throughput
- A Python generator has the same API as a Kafka consumer: `for record in stream` works whether `stream` is a generator or a Kafka topic
- The `batch_size` trade-off: smaller batches = lower latency; larger batches = higher throughput. The same parameter exists in Spark Structured Streaming and Kafka Streams.

**NHS context:** NHS 111 call volume spikes during winter. A batch job running at midnight would miss a surge at 8pm. A streaming window of 5 minutes can detect it in time for a clinical response.

**Check:** You should see DuckDB row counts incrementing micro-batch by micro-batch.

---

## Lab 09 — Interactive Dashboard

```bash
uv run marimo edit notebooks/09_dashboard_report.py
```

**What you will do**

Use the drug selector and date range controls to filter the dashboard. Click "Export PDF". Check the filename — it encodes the filter state and timestamp.

**What you will learn**

- **Reactive notebooks**: in Marimo, changing a widget automatically re-runs all cells that depend on it — no manual "run all" needed
- **Audit trails in filenames**: `report_metformin_2021-01_to_2024-06_generated_20240403T143022.pdf` tells a clinical governance team exactly what the report contains and when it was generated
- Why this matters in healthcare: a clinician must be able to reproduce any report they showed at a board meeting six months ago

**Check:** Export a report and verify the filename contains the drug name, date range, and a timestamp.

---

## Lab 10 — Backfill

```bash
uv run marimo edit notebooks/10_backfill.py
```

**What you will do**

Run all cells. The notebook will delete and re-insert Silver rows for a specific date window, then rebuild Gold. Run it twice to verify the idempotency proof cell.

**What you will learn**

- **Why backfill exists**: three common triggers — a new drug is added (need historical data), a bug is found in the transform logic (need to re-derive), a source API corrects historical records (need to re-ingest)
- **Full rebuild vs partition backfill**: rebuilding all of Silver costs O(all data); a partition backfill costs O(affected months only) — at NHS scale with years of data, this is the difference between minutes and hours
- **Idempotency in backfill**: `DELETE WHERE year_month BETWEEN ? AND ?` then `INSERT` — run it 10 times, get the same result every time
- **`ingested_at` audit column**: shows exactly when each row was (re-)processed — useful for debugging and compliance

**Check:** Run the notebook twice. The row count in the target window should be identical both times.

---

## After the Labs — Run the Full Production Pipeline

Now that you understand each step individually, run the whole pipeline end-to-end with Prefect orchestration, data contracts, and lineage tracking:

```bash
# Terminal 1 — start Prefect server
uv run prefect server start

# Terminal 2 — run the pipeline (open http://localhost:4200 to watch it)
uv run python flows/pipeline_flow.py
```

What happens automatically:

1. Fetch data from OpenPrescribing API and NHS.uk (with retries)
2. Validate API responses against Pydantic data contracts
3. Write Bronze files to `lake/`
4. Build Silver with data quality gate (pipeline fails loudly if null rates exceed thresholds)
5. Emit OpenLineage events so every Gold table has a recorded provenance chain
6. Build Gold aggregation tables

Open the Prefect UI at `http://localhost:4200` to see the task dependency graph, retry history, and run logs.

---

## Run the Tests

```bash
uv run pytest
```

193 tests covering every module — contracts, lineage, lake, medallion, fetch, transform, streaming, NLP, and visualisation.

---

## What Next?

You have completed a Level 0–2 data pipeline. The patterns you have used here — medallion architecture, idempotency, SCD Type 2, data contracts, lineage, backfill — are identical to what you would use with:

| This course | Production equivalent |
|---|---|
| DuckDB | Snowflake, BigQuery, Redshift |
| Polars lazy frames | Spark (same lazy DAG concept) |
| Python generator | Kafka / Redpanda topic |
| Prefect `@task` | Airflow DAG task |
| Bronze/Silver/Gold DuckDB schemas | Delta Lake / Apache Iceberg on S3 |
| OpenLineage log output | OpenMetadata with full UI |

The next step is to run one of these tools against the same data — the pipeline logic does not change, only the execution engine.
