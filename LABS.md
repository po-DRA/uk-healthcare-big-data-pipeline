# Lab Guide — UK Healthcare Big Data Pipeline

Work through these labs in order. Each one builds on the previous — later scripts read files that earlier scripts write. By the end you will have built a complete, production-grade data pipeline using real NHS open data.

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

You should see all tests passing. You are ready.

**Local setup (alternative)**

```bash
git clone https://github.com/po-DRA/uk-healthcare-big-data-pipeline.git
cd uk-healthcare-big-data-pipeline
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync --all-groups
uv run pytest
```

---

## Lab 01 — Fetching Data in Parallel

**Script:** [scripts/01_fetch.py](scripts/01_fetch.py)

```bash
uv run python scripts/01_fetch.py
```

**What you will do**

Watch the output as data is fetched for seven drugs (metformin, liraglutide, semaglutide, tirzepatide, atorvastatin, lisinopril, salbutamol) concurrently from NHSBSA and NHS.uk.

**What you will learn**

- How to stream a large CSV file (NHSBSA English Prescribing Dataset — 6.9 GB/month) with early-exit filtering — only ~46 MB is read to collect 500 rows per drug
- How to scrape clinical content from `NHS.uk` pages
- How `ThreadPoolExecutor` fetches multiple drugs in parallel instead of one-at-a-time
- Why parallelism matters at NHS scale: fetching 50 drugs sequentially at 1 second each = 50 seconds; in parallel = ~2 seconds

**Check:** You should see a lake summary table listing `prescribing.jsonl` and `nhs_pages.json` for each of the four drugs.

---

## Lab 02 — Querying the Bronze Lake with DuckDB

**Script:** [scripts/02_query_bronze.py](scripts/02_query_bronze.py)

```bash
uv run python scripts/02_query_bronze.py
```

**What you will do**

Run SQL queries directly on the JSONL files that Lab 01 wrote. Try changing the `WHERE` clause in the script to filter for a different drug or ICB.

**What you will learn**

- **DuckDB** can query JSONL files directly — no loading into pandas, no database server, no schema setup
- Glob patterns let you query all four drug files in one SQL statement: `read_ndjson('lake/*/prescribing.jsonl')`
- This is the same pattern used in cloud data lakes (S3 + Athena, GCS + BigQuery) — DuckDB is just doing it locally
- **The Bronze layer**: raw data stored exactly as received, never modified

**Check:** You should see row counts and a top-ICBs-by-cost table across all four drugs.

---

## Lab 03 — Transforming with Polars

**Script:** [scripts/03_transform.py](scripts/03_transform.py)

```bash
uv run python scripts/03_transform.py
```

**What you will do**

Run the script and read the veracity report output carefully — it shows what percentage of each field is null.

**What you will learn**

- **Polars lazy evaluation**: `scan_ndjson()` describes the computation without reading any data yet; `collect()` executes it
- Why lazy evaluation matters: Polars (and Spark) can push filters down to the source and skip columns you don't need — crucial at NHS scale
- The **veracity report**: a simple null-percentage table that makes data quality visible before it causes silent errors downstream

**Check:** You should see a veracity report table showing null percentages per field. Some nulls are expected and documented — the report makes them explicit.

---

## Lab 04 — Medallion Architecture + Slowly Changing Dimensions

**Script:** [scripts/04_medallion.py](scripts/04_medallion.py)

```bash
uv run python scripts/04_medallion.py
```

**What you will do**

Run all steps in order. After Silver is built, the script queries `silver.prescribing`. After Gold is built, it queries `gold.drug_summary`.

**What you will learn**

- **Bronze → Silver → Gold**: why you layer transformations instead of transforming raw data in place
  - Bronze: immutable raw files (never touch these)
  - Silver: cleaned, typed, deduplicated — the "single source of truth"
  - Gold: aggregated for a specific analytical use case
- **Idempotency**: running `build_silver()` twice gives the same result as running it once — essential for re-runs and backfills
- **SCD Type 2**: the 2022 NHS reorganisation merged 135 CCGs into 42 ICBs. A GP practice now has two rows in `dim_practice` — one for before July 2022, one for after. Without SCD2, your historical cost reports would be wrong.

**Check:** The script prints row counts for Silver and each Gold table, then proves idempotency by running twice and comparing.

---

## Lab 05 — NLP on NHS Clinical Text

**Script:** [scripts/05_nlp.py](scripts/05_nlp.py)

```bash
uv run python scripts/05_nlp.py
```

**What you will do**

Run the script and look at the term-frequency tables for each drug. Notice which clinical terms appear most often.

**What you will learn**

- How unstructured clinical prose (NHS.uk patient pages) can be turned into structured signals
- Basic NLP: tokenisation, stop-word removal, term frequency
- Why Variety is hard: the prescribing data has clean columns; the clinical text is free prose that needs parsing before any analysis is possible
- A real clinical insight: metformin's top terms cluster around "diabetes/blood sugar"; salbutamol's around "breathing/inhaler"

**Check:** You should see a frequency table per drug with recognisable clinical terminology.

---

## Lab 06 — Visualisation from Gold

**Script:** [scripts/06_visualise.py](scripts/06_visualise.py)

```bash
uv run python scripts/06_visualise.py
```

**What you will do**

Run the script. Charts are saved to `outputs/`. Open them to see prescribing cost trends and drug comparisons.

**What you will learn**

- How to query Gold tables and turn them into charts in a few lines of code
- **Cost per item** by drug — a real NHS efficiency metric
- Why the Gold layer exists: it pre-computes expensive aggregations once so dashboards and reports can query cheaply

**Check:** You should see PNG files in `outputs/` — one per chart.

---

## Lab 07 — Streaming Simulation

**Script:** [scripts/07_stream.py](scripts/07_stream.py)

```bash
uv run python scripts/07_stream.py
```

**What you will do**

Run the script. Edit `BATCH_SIZE` at the top of the file and re-run to see how it affects micro-batch behaviour.

**What you will learn**

- The difference between **batch** (process everything once at midnight) and **streaming** (process each record as it arrives)
- **Micro-batch processing**: a middle ground — process small chunks continuously, balancing latency and throughput
- A Python generator has the same API as a Kafka consumer: `for record in stream` works whether `stream` is a generator or a Kafka topic
- The `BATCH_SIZE` trade-off: smaller = lower latency; larger = higher throughput. The same parameter exists in Spark Structured Streaming.

**NHS context:** NHS 111 call volume spikes during winter. A batch job running at midnight would miss a surge at 8pm. A streaming window of 5 minutes can detect it in time for a clinical response.

**Check:** You should see DuckDB row counts incrementing micro-batch by micro-batch, with per-batch timing.

---

## Lab 08 — Backfill

**Script:** [scripts/08_backfill.py](scripts/08_backfill.py)

```bash
uv run python scripts/08_backfill.py
```

**What you will do**

Run the script. It will delete and re-insert Silver rows for a specific date window, then rebuild Gold. Run it twice to verify the idempotency proof.

**What you will learn**

- **Why backfill exists**: three common triggers — a new drug is added (need historical data), a bug is found in the transform logic (need to re-derive), a source API corrects historical records (need to re-ingest)
- **Full rebuild vs partition backfill**: rebuilding all of Silver costs O(all data); a partition backfill costs O(affected months only) — at NHS scale with years of data, this is the difference between minutes and hours
- **Idempotency in backfill**: `DELETE WHERE year_month BETWEEN ? AND ?` then `INSERT` — run it 10 times, get the same result every time
- **`ingested_at` audit column**: shows exactly when each row was (re-)processed — useful for debugging and compliance

**Check:** Run the script twice. The row count in the target window should be identical both times.

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

1. Fetch data from NHSBSA EPD API and NHS.uk (with retries)
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

Tests cover every module — contracts, lineage, lake, medallion, fetch, transform, streaming, NLP, and visualisation.

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
