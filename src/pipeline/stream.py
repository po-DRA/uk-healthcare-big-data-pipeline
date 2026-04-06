"""
pipeline/stream.py — Simulated streaming pipeline using Python generators.

Demonstrates the **Velocity** V by replaying Bronze prescribing records as if
they are arriving in real-time from a GP practice management system.

No message broker (Kafka, RabbitMQ, Pulsar) is required.  We simulate the
stream with a Python generator — this is the *same conceptual pattern* used
in every production streaming system:

  Production streaming          This teaching simulation
  ─────────────────────         ───────────────────────────────────────
  Kafka consumer.poll()    ←→   prescribing_event_stream() generator
  Kafka topic partition    ←→   a single JSONL file in the lake
  Kafka offset commit      ←→   batch_num counter
  Stream processing sink   ←→   DuckDB streaming.live_prescribing table
  Micro-batch trigger      ←→   batch_size parameter

Teaching points
---------------
1. **A generator IS a stream.**  ``yield`` produces one item (or one batch)
   on demand without loading everything into memory first.  This is exactly
   what a Kafka consumer does — it gives you records as they arrive.

2. **Micro-batching is the industry norm.**  Spark Structured Streaming,
   Flink, and Kafka Streams all process records in configurable windows
   (time- or count-based).  Our ``batch_size`` parameter teaches this.

3. **The sink decides the latency.**  Writing every record to DuckDB one
   at a time would be correct but slow.  Batching ``executemany`` calls
   shows the throughput–latency trade-off that every streaming engineer
   must manage.

4. **Swap the source, keep the logic.**  To go to production, replace
   ``prescribing_event_stream()`` with a Kafka consumer:

       from confluent_kafka import Consumer
       consumer = Consumer({...})
       consumer.subscribe(["prescribing-events"])
       while True:
           msg = consumer.poll(1.0)
           yield json.loads(msg.value())

   Everything else (DuckDB insert, running aggregate) stays identical.

Usage
-----
    from pathlib import Path
    from pipeline.stream import prescribing_event_stream, stream_into_duckdb

    lake_dir = Path("lake")
    db_path  = Path("pipeline.duckdb")

    # Inspect the generator concept
    for batch in prescribing_event_stream(lake_dir, "metformin", batch_size=200):
        print(f"Received {len(batch)} records")

    # Or stream directly into DuckDB
    result = stream_into_duckdb(lake_dir, "metformin", db_path, batch_size=500)
    print(result)
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from collections.abc import Generator

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Generator — the stream source
# ---------------------------------------------------------------------------


def prescribing_event_stream(
    lake_dir: pathlib.Path,
    drug: str,
    batch_size: int = 500,
    delay_seconds: float = 0.0,
) -> Generator[list[dict], None, None]:
    """Yield micro-batches of prescribing records from the Bronze lake.

    Demonstrates **Velocity** — each batch represents records that "arrived"
    in one time window, the same concept as a Kafka consumer ``poll()`` or
    a Spark Structured Streaming micro-batch trigger.

    Parameters
    ----------
    lake_dir:
        Root of the Bronze lake, e.g. ``Path("lake")``.
    drug:
        Drug name, e.g. ``"metformin"``.  Must match a subdirectory in the lake.
    batch_size:
        Number of records per micro-batch.  Smaller = lower latency, higher
        overhead.  Larger = higher throughput, higher latency.
    delay_seconds:
        Simulated inter-batch pause in seconds.  Set to ``0.0`` for tests
        and CI; use ``0.05``–``0.5`` for live notebook demos.

    Yields
    ------
    list[dict]
        One micro-batch of raw prescribing records (dicts from JSONL).

    Raises
    ------
    FileNotFoundError
        If the drug's JSONL file does not exist in the lake.

    Notes
    -----
    This is a *replay* stream — it reads existing Bronze data.  In production,
    replace this generator with a Kafka consumer or an SSE reader.
    """
    path = lake_dir / drug / "prescribing.jsonl"
    if not path.exists():
        raise FileNotFoundError(
            f"Bronze lake file not found: {path}. "
            "Run the fetch pipeline first (scripts/01_fetch.py or pipeline_flow.py)."
        )

    batch: list[dict] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            batch.append(json.loads(line))
            if len(batch) >= batch_size:
                yield batch
                batch = []
                if delay_seconds > 0:
                    time.sleep(delay_seconds)

    if batch:  # flush the final partial batch
        yield batch


# ---------------------------------------------------------------------------
# Streaming sink — writes batches into DuckDB incrementally
# ---------------------------------------------------------------------------


def stream_into_duckdb(
    lake_dir: pathlib.Path,
    drug: str,
    db_path: pathlib.Path = pathlib.Path("pipeline.duckdb"),
    batch_size: int = 500,
    delay_seconds: float = 0.0,
) -> dict:
    """Stream Bronze records into a DuckDB table, one micro-batch at a time.

    Demonstrates **Velocity** — the DuckDB table grows incrementally as each
    batch arrives, rather than loading all records in a single bulk operation.

    The ``streaming.live_prescribing`` table is updated after every batch.
    You can query it mid-stream to see the running state — exactly like
    querying a Kafka Streams state store or a Flink changelog table.

    Parameters
    ----------
    lake_dir:
        Root of the Bronze lake.
    drug:
        Drug name to stream.
    db_path:
        Path to the DuckDB file.  Defaults to ``pipeline.duckdb``.
    batch_size:
        Records per micro-batch.
    delay_seconds:
        Optional inter-batch pause (set > 0 for live notebook demos).

    Returns
    -------
    dict with keys:
        - ``drug``           : str
        - ``total_batches``  : int
        - ``total_records``  : int
        - ``total_items``    : int | None
        - ``total_cost_gbp`` : float | None
    """
    import duckdb

    total_batches = 0
    total_records = 0
    summary = None

    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS streaming")

        # Create the streaming sink table (idempotent)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS streaming.live_prescribing (
                date        VARCHAR,
                actual_cost DOUBLE,
                items       BIGINT,
                quantity    DOUBLE,
                row_id      VARCHAR,
                setting     VARCHAR,
                ccg         VARCHAR,
                drug        VARCHAR,
                batch_num   INTEGER,        -- which micro-batch this record arrived in
                arrived_at  TIMESTAMP DEFAULT now()
            )
            """
        )
        # Clear previous run for this drug so re-runs are idempotent
        con.execute("DELETE FROM streaming.live_prescribing WHERE drug = ?", [drug])

        for batch in prescribing_event_stream(
            lake_dir, drug, batch_size, delay_seconds
        ):
            total_batches += 1

            # Convert each record to a tuple matching the table schema.
            # executemany is faster than individual INSERT statements and
            # avoids pulling pandas/polars into this module.
            rows = [
                (
                    r.get("date", ""),
                    float(r["actual_cost"])
                    if r.get("actual_cost") is not None
                    else None,
                    int(r["items"]) if r.get("items") is not None else None,
                    float(r["quantity"]) if r.get("quantity") is not None else None,
                    r.get("row_id", ""),
                    str(r.get("setting", "")),
                    r.get("ccg", ""),
                    r.get("drug", drug),
                    total_batches,
                )
                for r in batch
            ]
            con.executemany(
                """
                INSERT INTO streaming.live_prescribing
                    (date, actual_cost, items, quantity, row_id,
                     setting, ccg, drug, batch_num)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            total_records += len(batch)
            _log.info(
                "Batch %3d | +%5d records | running total: %7d",
                total_batches,
                len(batch),
                total_records,
            )

        # Final running aggregate from the streaming table
        summary = con.execute(
            """
            SELECT
                COUNT(*)                    AS total_records,
                SUM(items)                  AS total_items,
                ROUND(SUM(actual_cost), 2)  AS total_cost_gbp
            FROM streaming.live_prescribing
            WHERE drug = ?
            """,
            [drug],
        ).fetchone()

    return {
        "drug": drug,
        "total_batches": total_batches,
        "total_records": total_records,
        "total_items": summary[1] if summary else None,
        "total_cost_gbp": summary[2] if summary else None,
    }
