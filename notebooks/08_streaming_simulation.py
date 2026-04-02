import marimo

__generated_with = "0.6.0"
app = marimo.App(title="08 · Streaming Simulation — Velocity")


@app.cell
def __():
    import marimo as mo

    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 08 — Streaming Simulation

        **Learning objective:** Understand what streaming data means, why it matters
        for healthcare analytics, and how to implement the streaming pattern
        using Python generators and DuckDB — **no message broker required**.

        **V demonstrated:** Velocity (continuous data arrival, incremental processing)

        **Estimated time:** 10 minutes

        > **Prerequisite:** Run notebook 02 first to populate `lake/` (Bronze layer).
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Batch vs Streaming — What's the Difference?

        | | Batch pipeline | Streaming pipeline |
        |---|---|---|
        | **When does data arrive?** | All at once (e.g. monthly dump) | Continuously (e.g. per prescription issued) |
        | **When do you process it?** | Scheduled (e.g. nightly job) | As it arrives (or in micro-batches) |
        | **How much is in memory?** | The whole dataset | One batch at a time |
        | **Latency to insight** | Hours / days | Seconds / minutes |
        | **NHS example** | Monthly BSA prescribing export | Live A&E admissions, bed occupancy feed |

        **Our pipeline so far is batch** — we fetch all records for a drug at once,
        then transform the whole dataset.

        **This notebook simulates streaming** — we replay those same records
        as if they were arriving one micro-batch at a time from a live system.

        ### What would stream in reality?
        - GP practice management systems sending a record each time a prescription is issued
        - Hospital bed management systems updating occupancy every few minutes
        - NHS 111 call centres emitting triage records in real time

        ### Why not use Kafka / RabbitMQ for this course?
        Because the *concept* is what matters, and a Python generator teaches it with
        zero server configuration.  Once you understand the pattern here, swapping the
        generator for a real Kafka consumer is **a one-function change**.
        """
    )
    return


@app.cell
def __():
    import pathlib

    import duckdb

    from pipeline.stream import prescribing_event_stream, stream_into_duckdb

    return duckdb, pathlib, prescribing_event_stream, stream_into_duckdb


@app.cell
def __(pathlib):
    LAKE_DIR = pathlib.Path("lake")
    DB_PATH = pathlib.Path("pipeline.duckdb")
    DRUG = "metformin"
    return DB_PATH, DRUG, LAKE_DIR


@app.cell
def __(mo):
    mo.md(
        """
        ## The Generator — Python's Built-in Stream Abstraction

        A Python generator is a function that **yields** one item at a time,
        pausing between yields.  It produces values on demand without loading
        everything into memory first.

        This is conceptually identical to a Kafka consumer's `poll()` call:
        both give you a chunk of data, then wait for you to ask for more.

        Let's watch the first 3 micro-batches arrive:
        """
    )
    return


@app.cell
def __(DRUG, LAKE_DIR, prescribing_event_stream):
    print(f"Streaming {DRUG} prescribing records in micro-batches of 200:\n")

    _stream = prescribing_event_stream(LAKE_DIR, DRUG, batch_size=200)

    for _batch_num, _batch in enumerate(_stream, start=1):
        _first = _batch[0]
        _last = _batch[-1]
        print(
            f"  Batch {_batch_num:>2} | {len(_batch):>4} records | "
            f"dates {_first['date']} … {_last['date']} | "
            f"drug: {_first['drug']}"
        )
        if _batch_num >= 3:
            print("  ... (stopping after 3 batches for display)")
            break

    print(
        "\nKey insight: the generator paused after each yield — the next batch "
        "was not read from disk until we asked for it."
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Streaming Into DuckDB — Incremental Writes

        Now we stream ALL batches into DuckDB.  After every batch, the
        `streaming.live_prescribing` table grows — you can query it mid-stream
        to see the current state, just like querying a Kafka Streams state store.

        We use `batch_size=500` here (no artificial delay for speed).
        In a live demo, set `delay_seconds=0.1` to watch the table grow.
        """
    )
    return


@app.cell
def __(DB_PATH, DRUG, LAKE_DIR, stream_into_duckdb):
    print(f"Streaming {DRUG} into DuckDB (streaming.live_prescribing):\n")

    result = stream_into_duckdb(
        lake_dir=LAKE_DIR,
        drug=DRUG,
        db_path=DB_PATH,
        batch_size=500,
        delay_seconds=0.0,  # set to 0.1 for a live demo
    )

    print(f"\nStreaming complete for {result['drug']}:")
    print(f"  Micro-batches processed : {result['total_batches']:>6,}")
    print(f"  Total records streamed  : {result['total_records']:>6,}")
    print(f"  Total items             : {result['total_items']:>6,}")
    print(f"  Total cost (£)          : {result['total_cost_gbp']:>10,.2f}")
    return (result,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Query the Streaming Table

        The `streaming.live_prescribing` table is a standard DuckDB table.
        You can run any SQL against it — exactly like querying a real-time
        data warehouse materialised view.
        """
    )
    return


@app.cell
def __(DB_PATH, DRUG, duckdb):
    _con = duckdb.connect(str(DB_PATH))

    # How many records arrived in each batch?
    batch_sizes = _con.execute(
        """
        SELECT
            batch_num,
            COUNT(*)                    AS records_in_batch,
            SUM(COUNT(*)) OVER (
                ORDER BY batch_num
                ROWS UNBOUNDED PRECEDING
            )                           AS running_total
        FROM streaming.live_prescribing
        WHERE drug = ?
        GROUP BY batch_num
        ORDER BY batch_num
        LIMIT 10
        """,
        [DRUG],
    ).df()

    print(f"Batch arrival log for {DRUG} (first 10 batches):")
    print(batch_sizes.to_string(index=False))
    print(
        "\nrunning_total shows the table state after each batch arrived — "
        "this is what a streaming dashboard would update in near-real time."
    )
    _con.close()
    return (batch_sizes,)


@app.cell
def __(DB_PATH, DRUG, duckdb):
    _con = duckdb.connect(str(DB_PATH))

    # Running aggregate — the "live KPI"
    live_kpi = _con.execute(
        """
        SELECT
            COUNT(*)                                            AS total_records,
            SUM(items)                                          AS total_items,
            ROUND(SUM(actual_cost), 2)                         AS total_cost_gbp,
            ROUND(SUM(actual_cost) / NULLIF(SUM(items), 0), 4) AS avg_nic_per_item,
            COUNT(DISTINCT batch_num)                          AS batches_received
        FROM streaming.live_prescribing
        WHERE drug = ?
        """,
        [DRUG],
    ).df()

    print(f"Live KPI dashboard — {DRUG} (after all batches processed):")
    print(live_kpi.to_string(index=False))
    _con.close()
    return (live_kpi,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Windowed Aggregation — The Streaming Superpower

        In batch pipelines you aggregate over the whole dataset.
        In streaming you aggregate over **time windows** — e.g.
        "what happened in the last 5 batches (last 5 minutes)?"

        This is the query pattern that powers NHS 111 surge detection,
        A&E breach alerts, and real-time sepsis screening dashboards.
        """
    )
    return


@app.cell
def __(DB_PATH, DRUG, duckdb):
    _con = duckdb.connect(str(DB_PATH))

    # Sliding window: running total per batch, showing last 5 batches
    window_agg = _con.execute(
        """
        WITH batch_agg AS (
            SELECT
                batch_num,
                SUM(items)                      AS batch_items,
                ROUND(SUM(actual_cost), 2)       AS batch_cost_gbp,
                SUM(SUM(items)) OVER (
                    ORDER BY batch_num
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                )                               AS rolling_5_batch_items
            FROM streaming.live_prescribing
            WHERE drug = ?
            GROUP BY batch_num
        )
        SELECT * FROM batch_agg
        ORDER BY batch_num DESC
        LIMIT 8
        """,
        [DRUG],
    ).df()

    print(f"Windowed aggregation — {DRUG} (most recent 8 batches):")
    print(window_agg.to_string(index=False))
    print(
        "\nrolling_5_batch_items = sum of the current batch + previous 4 batches."
        "\nIn production: replace 'batch_num' with a timestamp and '4 PRECEDING'"
        "\nwith a time interval (e.g. INTERVAL '5 minutes')."
    )
    _con.close()
    return (window_agg,)


@app.cell
def __(mo):
    mo.md(
        """
        ## HTTP Streaming — `httpx.stream()`

        There is a second type of streaming you will encounter: **HTTP-level streaming**.
        Instead of loading an entire API response into memory, you process it
        chunk-by-chunk as bytes arrive over the network.

        This matters when the response is very large (hundreds of MB) and you
        want to start processing before the download completes.

        ```python
        import httpx
        import json

        url = "https://openprescribing.net/api/1.0/spending_by_org/?org_type=practice&code=0601023A0"

        with httpx.stream("GET", url, timeout=60) as response:
            # Process the response body as it arrives — never loads fully into memory
            for chunk in response.iter_bytes(chunk_size=4096):
                # In practice you'd accumulate chunks and parse JSON when complete
                print(f"Received chunk: {len(chunk):,} bytes")
        ```

        **Note:** OpenPrescribing returns a single JSON array, so you would accumulate
        all chunks and parse at the end.  HTTP streaming shines with APIs that return
        newline-delimited JSON (NDJSON) — one record per line — because you can parse
        and process each line as it arrives, before the response is complete.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## The Production Path — Swapping the Generator

        The only code you need to change to go from simulation to production
        streaming is the **source generator**.  Everything else stays the same:

        ```
        SIMULATION (this notebook)          PRODUCTION
        ──────────────────────────          ──────────────────────────────────
        prescribing_event_stream()    →     Kafka consumer:
          reads JSONL from Bronze             consumer = Consumer({...})
          yields list[dict]                   consumer.subscribe(["prescribing"])
                                              while True:
                                                msg = consumer.poll(1.0)
                                                yield json.loads(msg.value())

        stream_into_duckdb()          →     Same function, no changes needed
          executemany INSERT                 (or swap DuckDB for a cloud warehouse)

        DuckDB streaming table        →     ClickHouse / BigQuery / Redshift
          query with SQL                    (same SQL window functions)
        ```

        The pattern — **source → generator → sink** — is identical regardless
        of whether the source is a file, a Kafka topic, an SSE feed, or a WebSocket.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Summary

        You have:
        - Understood the difference between batch and streaming pipelines
        - Used a Python generator as a stream source (no message broker needed)
        - Streamed prescribing records into DuckDB one micro-batch at a time
        - Queried the streaming table to see running totals and windowed aggregations
        - Seen how `httpx.stream()` handles HTTP-level streaming
        - Learned the one-function swap to move from simulation to Kafka

        **Reflection question:** The `batch_size` parameter controls the trade-off
        between throughput (large batches) and latency (small batches).
        If an NHS 111 dashboard needs to update every 30 seconds and the system
        receives ~60 new prescription records per minute, what batch_size would
        you choose?  What would change if the volume increased to 600 per minute?

        ---

        **Congratulations — you have completed the full course!**

        You can now run the entire pipeline end-to-end with Prefect orchestration:

        ```bash
        # Terminal 1:
        uv run prefect server start

        # Terminal 2:
        uv run python flows/pipeline_flow.py
        ```

        The Prefect flow now includes medallion steps — watch the DAG at
        http://localhost:4200 as Bronze → Silver → Gold builds automatically.
        """
    )
    return


if __name__ == "__main__":
    app.run()
