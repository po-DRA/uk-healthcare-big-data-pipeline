# Lab Guide - UK Healthcare Big Data Pipeline

Work through these labs in order. Each one builds on the previous - later scripts read files that earlier scripts write. By the end you will have built a complete, production-grade data pipeline using real NHS open data.

**Time:** ~4-5 hours total, or split across sessions.

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

## Lab 01 - Fetching Data in Parallel

**Script:** [scripts/01_fetch.py](scripts/01_fetch.py)

```bash
uv run python scripts/01_fetch.py
```

**What you will do**

Watch the output as data is fetched for 12 drugs (including GLP-1 weight-loss drugs semaglutide/Ozempic and liraglutide/Saxenda) concurrently from NHSBSA and NHS.uk. Each drug's prescribing records land in a month-partitioned Bronze directory.

**What you will learn**

- How to stream a large CSV file (NHSBSA English Prescribing Dataset - 6.9 GB/month) without downloading it - only matching rows are kept
- How to scrape clinical content from `NHS.uk` pages
- How `ThreadPoolExecutor` fetches multiple drugs in parallel instead of one-at-a-time
- Why Bronze is partitioned by month - each run adds data without overwriting history

**Controlling volume**

Two environment variables let you trade off fetch time against data volume:

| Variable | Default | Use case |
|----------|---------|----------|
| `NHSBSA_MONTHS` | `1` | Number of monthly EPD files to fetch |
| `NHSBSA_ROWS_PER_DRUG` | _(all)_ | Cap rows per drug - useful for quick tests |

```bash
# Quick smoke test - 500 rows per drug, ~5 MB, ~30 seconds
NHSBSA_ROWS_PER_DRUG=500 uv run python scripts/01_fetch.py

# Default - latest month, all rows, ~500 MB, ~10-20 minutes
uv run python scripts/01_fetch.py

# 2 months of history - good for demos, ~1 GB Bronze
NHSBSA_MONTHS=2 uv run python scripts/01_fetch.py

# Maximum local volume - 6 months, ~3+ GB Bronze
NHSBSA_MONTHS=6 uv run python scripts/01_fetch.py
```

Silver automatically reads across all month partitions - you do not need to change anything in Labs 02-08 after fetching more months.

**Check:** You should see output like `OK metformin/EPD_202506: 68,432 rows [EPD_202506]` for each drug, and a lake directory listing showing `metformin/EPD_202506/`, `atorvastatin/EPD_202506/` etc.

---

## Lab 02 - Querying the Bronze Lake with DuckDB

**Script:** [scripts/02_query_bronze.py](scripts/02_query_bronze.py)

```bash
uv run python scripts/02_query_bronze.py
```

**What you will do**

Run SQL queries directly on the JSONL files that Lab 01 wrote. Try changing the `WHERE` clause in the script to filter for a different drug or ICB.

**What you will learn**

- **DuckDB** can query JSONL files directly - no loading into pandas, no database server, no schema setup
- Glob patterns let you query all drug files in one SQL statement: `read_ndjson('lake/*/*/prescribing.jsonl')`
- This is the same pattern used in cloud data lakes (S3 + Athena, GCS + BigQuery) - DuckDB is just doing it locally
- **The Bronze layer**: raw data stored exactly as received, never modified

**Check:** You should see row counts and a top-ICBs-by-cost table across all drugs in your Bronze lake.

---

## Lab 03 - Transforming with Polars

**Script:** [scripts/03_transform.py](scripts/03_transform.py)

```bash
uv run python scripts/03_transform.py
```

**What you will do**

Run the script and read the veracity report output carefully - it shows what percentage of each field is null.

**What you will learn**

- **Polars lazy evaluation**: `scan_ndjson()` describes the computation without reading any data yet; `collect()` executes it
- Why lazy evaluation matters: Polars (and Spark) can push filters down to the source and skip columns you don't need - crucial at NHS scale
- The **veracity report**: a simple null-percentage table that makes data quality visible before it causes silent errors downstream

**Check:** You should see a veracity report table showing null percentages per field. Some nulls are expected and documented - the report makes them explicit.

---

## Lab 04 - Medallion Architecture + Slowly Changing Dimensions

**Script:** [scripts/04_medallion.py](scripts/04_medallion.py)

```bash
uv run python scripts/04_medallion.py
```

**What you will do**

Run all steps in order. After Silver is built, the script queries `silver.prescribing`. After Gold is built, it queries `gold.drug_summary`.

**What you will learn**

- **Bronze → Silver → Gold**: why you layer transformations instead of transforming raw data in place
  - Bronze: immutable raw files (never touch these)
  - Silver: cleaned, typed, deduplicated - the "single source of truth"
  - Gold: aggregated for a specific analytical use case
- **Idempotency**: running `build_silver()` twice gives the same result as running it once - essential for re-runs and backfills
- **SCD Type 2**: the 2022 NHS reorganisation merged 106 CCGs into 42 ICBs. A GP practice now has two rows in `dim_practice` - one for before July 2022, one for after. Without SCD2, your historical cost reports would be wrong.

**Check:** The script prints row counts for Silver and each Gold table, then proves idempotency by running twice and comparing.

**Verifying SCD Type 2**

Query the dimension to see how many practice versions exist:

```bash
uv run python -c "
import duckdb
con = duckdb.connect('pipeline.duckdb')

print('--- All practice versions (current) ---')
con.sql('''
    SELECT
        COUNT(*)                        AS total_versions,
        COUNT(DISTINCT practice_id)     AS unique_practices,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions,
        SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS historical_versions
    FROM gold.dim_practice
''').show()
"
```

You will see `historical_versions = 0`. This is expected - the data you fetched is from 2025, after the July 2022 CCG → ICB reorganisation. Every practice already exists in its current ICB form, so no practice has two versions.

To see SCD Type 2 actually create a historical row, inject a synthetic old version of a practice directly into Silver and rebuild the dimension:

```bash
uv run python -c "
import duckdb, pathlib
con = duckdb.connect('pipeline.duckdb')

# Pick a real row_id and its current CCG from Silver
# (row_id is used as practice_id in the SCD2 dimension)
row = con.sql('SELECT DISTINCT row_id, ccg FROM silver.prescribing LIMIT 1').fetchone()
row_id, current_ccg = row
old_ccg = 'OLD001'  # fictional previous CCG

print(f'Practice {row_id}: injecting old CCG {old_ccg} -> current CCG {current_ccg}')

# Insert a synthetic historical record - same row_id, different CCG, earlier date
con.execute('''
    INSERT INTO silver.prescribing
    SELECT
        DATE '2021-06-01' AS date,
        actual_cost,
        items,
        quantity,
        row_id,
        setting,
        ? AS ccg,
        drug,
        nic_per_item,
        '2021-06' AS year_month,
        NOW() AS ingested_at
    FROM silver.prescribing
    WHERE row_id = ?
    LIMIT 1
''', [old_ccg, row_id])

# Rebuild the SCD2 dimension from updated Silver
from pipeline.medallion import build_dim_practice
rows = build_dim_practice(pathlib.Path('pipeline.duckdb'))
print(f'dim_practice now has {rows} rows')

# Show both versions of this practice
rows = con.execute(
    'SELECT practice_id, ccg, valid_from, valid_to, is_current '
    'FROM gold.dim_practice WHERE practice_id = ? ORDER BY valid_from',
    [row_id]
).fetchall()
for r in rows:
    print(r)
"
```

You should see **two rows** for that practice: the historical version (`is_current = FALSE`, `valid_to` set to when the new CCG took over) and the current version (`is_current = TRUE`, `valid_to = NULL`). That is SCD Type 2 working correctly.

Re-run `scripts/04_medallion.py` afterwards to rebuild Silver and Gold cleanly from Bronze.

**Customising the tables for your reporting needs**

The medallion architecture is designed to be changed - but changes flow downward only:

```
Bronze  ← never touch (raw source of truth)
  ↓
Silver  ← change types, add derived columns, change drug filter
  ↓
Gold    ← add/remove tables, change aggregations freely
```

To add or remove a Silver column, edit `build_silver()` in [src/pipeline/medallion.py](src/pipeline/medallion.py) and re-run this script. Gold rebuilds automatically from the updated Silver.

To add a new Gold table (e.g. a cost-per-ICB summary for a Power BI report), add a `CREATE TABLE gold.my_table AS SELECT ...` block inside `build_gold()` in the same file. No other changes needed.

To change which drugs appear in Silver, update `SILVER_DRUGS` at the top of `medallion.py` - any drug already in Bronze (fetched in Lab 01) can be promoted instantly.

**What is idempotency and why does it matter?**

Idempotency means: running the pipeline twice produces exactly the same result as running it once. Re-running never corrupts, duplicates, or deletes data that should not be touched.

This matters in three real scenarios:

1. **A run fails halfway through.** Without idempotency you have a partially-built Silver table in an unknown state. With idempotency you simply re-run - the pipeline rebuilds Silver from scratch and you end up with a correct table as if nothing went wrong.

2. **You fix a bug in the transform logic.** You update the SQL, re-run, and Silver/Gold are rebuilt correctly. You do not need to manually clean up the old state first.

3. **A scheduled job runs twice** (e.g. a retry after a timeout). With idempotency the second run produces the same output as the first - no duplicate rows, no inflated counts.

How it works here: `build_silver()` uses an atomic table swap - it builds `silver.prescribing_new` completely, then renames it to `silver.prescribing` in one operation. The old table is dropped. Readers either see the old table or the new one - never an empty or partial table. `build_gold()` drops and recreates each Gold table from Silver on every run.

Try it yourself - run Lab 04 twice and compare the row counts:

```bash
uv run python scripts/04_medallion.py
uv run python scripts/04_medallion.py  # identical output
```

**Exploring the DuckDB file directly**

After Lab 04 you have a `pipeline.duckdb` file with all Silver and Gold tables. You can query it at any time without re-running the script:

```bash
# List all tables
uv run python -c "import duckdb; duckdb.connect('pipeline.duckdb').sql('SHOW ALL TABLES').show()"

# See the drug cost summary
uv run python -c "import duckdb; duckdb.connect('pipeline.duckdb').sql('SELECT * FROM gold.drug_summary').show()"

# Monthly spend trend
uv run python -c "import duckdb; duckdb.connect('pipeline.duckdb').sql('SELECT * FROM gold.drug_monthly_spend ORDER BY drug, year_month').show()"

# Top prescribing practices
uv run python -c "import duckdb; duckdb.connect('pipeline.duckdb').sql('SELECT * FROM gold.practice_leaderboard LIMIT 10').show()"
```

Or start an interactive session for free-form SQL:

```bash
uv run python
>>> import duckdb
>>> con = duckdb.connect("pipeline.duckdb")
>>> con.sql("SELECT drug, ROUND(avg_nic_per_item, 2) AS cost_per_item FROM gold.drug_summary ORDER BY cost_per_item DESC").show()
```

Note: `.show()` prints formatted tables directly - no pandas or pyarrow needed.

---

## Lab 05 - NLP on NHS Clinical Text

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
- A real clinical insight: metformin's top terms cluster around "sugar/blood/vitamin"; amoxicillin's around "skin/allergic/serious" - terms that reflect real prescribing risk signals

**Check:** You should see a frequency table per drug with recognisable clinical terminology.

---

## Lab 06 - Visualisation from Gold

**Script:** [scripts/06_visualise.py](scripts/06_visualise.py)

```bash
uv run python scripts/06_visualise.py
```

**What you will do**

Run the script. Charts are saved to `outputs/`. Open them to see prescribing cost trends and drug comparisons.

**What you will learn**

- How to query Gold tables and turn them into charts in a few lines of code
- **Cost per item** by drug - a real NHS efficiency metric
- Why the Gold layer exists: it pre-computes expensive aggregations once so dashboards and reports can query cheaply

**Check:** You should see PNG files in `outputs/` - one per chart.

---

## Lab 07 - Streaming Simulation

**Script:** [scripts/07_stream.py](scripts/07_stream.py)

```bash
uv run python scripts/07_stream.py
```

**What you will do**

Run the script. Edit `BATCH_SIZE` at the top of the file and re-run to see how it affects micro-batch behaviour.

**What you will learn**

- The difference between **batch** (process everything once at midnight) and **streaming** (process each record as it arrives)
- **Micro-batch processing**: a middle ground - process small chunks continuously, balancing latency and throughput
- A Python generator has the same API as a Kafka consumer: `for record in stream` works whether `stream` is a generator or a Kafka topic
- The `BATCH_SIZE` trade-off: smaller = lower latency; larger = higher throughput. The same parameter exists in Spark Structured Streaming.

**NHS context:** NHS 111 call volume spikes during winter. A batch job running at midnight would miss a surge at 8pm. A streaming window of 5 minutes can detect it in time for a clinical response.

**Check:** You should see DuckDB row counts incrementing micro-batch by micro-batch, with per-batch timing.

Query the streaming table before and after to confirm rows were inserted:

```bash
# Before running - table does not exist yet (will error) or shows 0 rows from a prior run
uv run python -c "
import duckdb
con = duckdb.connect('pipeline.duckdb')
try:
    con.sql('SELECT COUNT(*) AS rows_before FROM streaming.live_prescribing').show()
except Exception:
    print('streaming.live_prescribing does not exist yet - run the script first')
"

# Run the stream
uv run python scripts/07_stream.py

# After running - confirm total rows and how many batches were written
uv run python -c "
import duckdb
con = duckdb.connect('pipeline.duckdb')
con.sql('''
    SELECT
        COUNT(*)                    AS total_rows,
        COUNT(DISTINCT batch_num)   AS total_batches,
        MIN(arrived_at)             AS first_arrived,
        MAX(arrived_at)             AS last_arrived
    FROM streaming.live_prescribing
''').show()
"
```

You should see `total_rows` match the printed summary and `total_batches` equal `total_rows / BATCH_SIZE` (rounded up).

---

## Lab 08 - Backfill

**Script:** [scripts/08_backfill.py](scripts/08_backfill.py)

```bash
uv run python scripts/08_backfill.py
```

**What you will do**

Run the script. It will delete and re-insert Silver rows for a specific date window, then rebuild Gold. Run it twice to verify the idempotency proof.

**What you will learn**

- **Why backfill exists**: three common triggers - a new drug is added (need historical data), a bug is found in the transform logic (need to re-derive), a source API corrects historical records (need to re-ingest)
- **Full rebuild vs partition backfill**: rebuilding all of Silver costs O(all data); a partition backfill costs O(affected months only) - at NHS scale with years of data, this is the difference between minutes and hours
- **Idempotency in backfill**: `DELETE WHERE year_month BETWEEN ? AND ?` then `INSERT` - run it 10 times, get the same result every time
- **`ingested_at` audit column**: shows exactly when each row was (re-)processed - useful for debugging and compliance

**Check:** Run the script twice. The row count in the target window should be identical both times.

---

## After the Labs - Run the Full Production Pipeline

Now that you understand each step individually, run the whole pipeline end-to-end with Prefect orchestration, data contracts, and lineage tracking:

```bash
# Terminal 1 - start Prefect server
uv run prefect server start

# Terminal 2 - run the pipeline (open http://localhost:4200 to watch it)
export PREFECT_API_URL=http://127.0.0.1:4200/api
uv run python flows/pipeline_flow.py
```

**Codespaces users:** the Prefect UI needs one extra step to connect correctly. Stop the server, then restart with your Codespace URL:

```bash
PREFECT_UI_API_URL=https://<your-codespace-name>-4200.app.github.dev/api \
  uv run prefect server start
```

Replace `<your-codespace-name>` with the hostname shown in your browser's address bar (e.g. `zany-barnacle-wrwpwq4pp77c5jw9`).

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

Tests cover every module - contracts, lineage, lake, medallion, fetch, transform, streaming, NLP, and visualisation.

---

## What Next?

You have completed a Level 0-2 data pipeline. The patterns you have used here - medallion architecture, idempotency, SCD Type 2, data contracts, lineage, backfill - are identical to what you would use with:

| This course | Production equivalent |
|---|---|
| DuckDB | Snowflake, BigQuery, Redshift, **Databricks SQL Warehouse** |
| Polars lazy frames | Spark / PySpark (same lazy DAG concept) - runs natively on **Databricks** |
| Python generator | Kafka / Redpanda topic |
| Prefect `@task` | Airflow DAG task / **Databricks Workflows** |
| Bronze/Silver/Gold DuckDB schemas | Delta Lake / Apache Iceberg on S3 - **Databricks Delta Live Tables** |
| `build_silver()` DuckDB SQL | PySpark SQL on Databricks - same ANSI SQL, same medallion logic |
| OpenLineage log output | OpenMetadata with full UI / **Databricks Unity Catalog** (automatic lineage) |
| Docker container | Databricks cluster / container runtime |

The next step is to run one of these tools against the same data - the pipeline logic does not change, only the execution engine.

---

## Going Further - Scheduling, Logging, and Error Handling

These are three things you can add to this pipeline yourself. Each is a small, self-contained change that teaches a real production pattern.

---

### 1. Write logs to a file

Currently all pipeline logs go to the terminal only. Adding a log file lets you inspect what happened after a run, compare runs over time, and diagnose failures without re-running.

Open `scripts/01_fetch.py` and replace line 57:

```python
# Before
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

# After - write to terminal AND a log file
import pathlib
pathlib.Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                        # terminal output unchanged
        logging.FileHandler("logs/pipeline.log"),      # also write to file
    ],
)
```

The same change applies to any other script you want to capture logs from.

Run it and inspect the output:

```bash
mkdir -p logs
uv run python scripts/01_fetch.py

# Read the full log after the run
cat logs/pipeline.log

# Or watch it live while the script is running (open a second terminal)
tail -f logs/pipeline.log
```

The log file will contain timestamped entries like:

```
2026-04-07 08:01:23 | INFO | pipeline.fetch | metformin: 50,000 rows collected
2026-04-07 08:01:45 | WARNING | pipeline.fetch | 404 for https://www.nhs.uk/... - skipping
2026-04-07 08:02:01 | INFO | pipeline.lake | Wrote 50,000 records to lake/metformin/EPD_202506/prescribing.jsonl
```

To prevent the log file growing indefinitely across many runs, swap `FileHandler` for `RotatingFileHandler`:

```python
from logging.handlers import RotatingFileHandler

logging.basicConfig(
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("logs/pipeline.log", maxBytes=5_000_000, backupCount=3),
    ],
)
```

This keeps the last 3 rotated files (up to 5 MB each) and deletes older ones automatically.

---

### 2. Schedule the pipeline to run automatically

#### Option A - GitHub Actions (recommended for Codespaces)

Create a new file `.github/workflows/schedule.yml`:

```yaml
name: Monthly Pipeline Run

on:
  schedule:
    - cron: "0 8 1 * *"   # 08:00 UTC on the 1st of every month
  workflow_dispatch:        # also allow manual trigger from GitHub UI

jobs:
  run-pipeline:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v6

      - name: Install uv
        uses: astral-sh/setup-uv@v7
        with:
          version: "latest"

      - name: Set up Python
        run: uv python install 3.11

      - name: Install dependencies
        run: uv sync --all-extras

      - name: Run pipeline
        env:
          NHSBSA_ROWS_PER_DRUG: "50000"   # cap rows for CI - remove for full run
        run: uv run python flows/pipeline_flow.py
```

Push this file to your repository. GitHub will automatically run the pipeline every month and show the logs in the **Actions** tab. You can also trigger it manually from the GitHub UI using `workflow_dispatch`.

**Why this is good for learning:** you can see the full run history, download logs as artifacts, and get email notifications on failure - all without keeping a server running.

#### Option B - Prefect schedule (if running a dedicated server)

Add one line to `flows/pipeline_flow.py`:

```python
from prefect.schedules import CronSchedule

@flow(
    name="UK Healthcare Big Data Pipeline",
    log_prints=True,
    schedules=[CronSchedule(cron="0 8 1 * *", timezone="Europe/London")],
)
def run_pipeline(...):
```

Then deploy it so Prefect tracks the schedule:

```bash
# Terminal 1 - keep the server running
uv run prefect server start

# Terminal 2 - register the deployment
uv run prefect deploy flows/pipeline_flow.py:run_pipeline --name monthly-nhs --pool default-agent-pool
```

The flow will now appear in the Prefect UI with a next-run countdown.

#### Option C - System cron (Linux/Mac local setup)

```bash
crontab -e
# Add this line - runs at 08:00 on 1st of every month:
0 8 1 * * cd /path/to/uk-healthcare-big-data-pipeline && uv run python flows/pipeline_flow.py >> logs/pipeline.log 2>&1
```

Replace `/path/to/uk-healthcare-big-data-pipeline` with the actual path from `pwd`.

---

### 3. Error handling - what is already built in

You do not need to add error handling from scratch. The pipeline already handles the most common failure modes:

| What could go wrong | How it is handled |
|---|---|
| NHSBSA API is down or slow | `tenacity` retries 3 times with exponential backoff (2-10 seconds between attempts) |
| One drug fails to fetch | Logged and skipped - the other 11 drugs continue normally |
| Malformed value in source data | `TRY_CAST` converts bad values to NULL instead of crashing |
| Silver null rate too high | `SilverDQViolation` halts the pipeline before bad data reaches Gold |
| Bronze file write interrupted | Atomic tmp-file rename - no half-written files are ever left behind |
| Lineage backend unreachable | Warning is logged, pipeline continues - lineage is non-blocking |
| Prefect task fails transiently | `@task(retries=2)` retries automatically with exponential backoff |

**What you could add yourself:**

- **Slack or email alert on failure**: Prefect supports notification blocks. Add `send_slack_webhook` to `validate_silver_task` so you get a message when the DQ gate fails.
- **Dead letter logging**: write failed drug payloads to `logs/failed/` so you can replay them later rather than losing them silently.

Both are two-to-five line additions to `flows/pipeline_flow.py`.

---

## Running with Docker

The repository includes a `Dockerfile` that packages the entire pipeline into a self-contained container. The Docker image is built and validated on every push to master via GitHub Actions CI - so the build is always verified.

**Why use Docker?**
- No local Python or uv install needed
- Identical environment on any machine (laptop, server, cloud VM)
- Easy to hand to a colleague or deploy to a cloud container service

### Build the image

```bash
docker build -t uk-healthcare-pipeline .
```

This takes 2-3 minutes the first time (downloading Python and dependencies). Subsequent builds are fast because Docker caches the dependency layer.

### Test the image works

```bash
# Verify the pipeline package is importable (same check as the health check)
docker run uk-healthcare-pipeline uv run python -c "import pipeline; print('OK')"

# Run the unit tests inside the container
docker run uk-healthcare-pipeline uv run pytest tests/ -v -m "not slow"
```

Both should complete without errors.

### Run the pipeline

```bash
# Run the full pipeline - lake/ is mounted so Bronze data persists on your machine
docker run -v "$(pwd)/lake:/app/lake" uk-healthcare-pipeline

# Run with a row cap (faster, good for testing)
docker run -v "$(pwd)/lake:/app/lake" \
  -e NHSBSA_ROWS_PER_DRUG=500 \
  uk-healthcare-pipeline

# Run a single script instead of the full flow
docker run -v "$(pwd)/lake:/app/lake" \
  uk-healthcare-pipeline uv run python scripts/01_fetch.py
```

The `-v "$(pwd)/lake:/app/lake"` flag mounts your local `lake/` directory into the container so Bronze files are written to your machine and survive after the container stops.

### Run with the Prefect UI

The container exposes port 4200. To use the Prefect UI you need two terminals:

```bash
# Terminal 1 - start the Prefect server inside a container
docker run -p 4200:4200 uk-healthcare-pipeline \
  uv run prefect server start --host 0.0.0.0

# Terminal 2 - run the pipeline, connecting to the server
docker run --network host \
  -v "$(pwd)/lake:/app/lake" \
  -e PREFECT_API_URL=http://127.0.0.1:4200/api \
  uk-healthcare-pipeline
```

Then open `http://localhost:4200` to see the run in the Prefect UI.

### What the Dockerfile does

- Starts from `python:3.11-slim` (minimal base image)
- Copies `uv` from the official uv image for fast, reproducible installs
- Installs all dependencies with `uv sync --frozen` (exact versions from `uv.lock`)
- Runs as a non-root user (`appuser`) - required for Kubernetes deployments
- Health check: `import pipeline` - confirms the package installed correctly
- Default command: `uv run python flows/pipeline_flow.py`
