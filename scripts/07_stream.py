"""
Script 07 — Streaming simulation with Python generators and DuckDB

What this does
--------------
Replays Bronze prescribing records as a simulated real-time stream.
Records arrive in micro-batches (configurable size) and are inserted into
DuckDB incrementally — the table grows with each batch.

This demonstrates the Velocity V: instead of loading all records at once,
data is processed as it "arrives".

Generator = Kafka consumer
~~~~~~~~~~~~~~~~~~~~~~~~~~
The streaming source in production would be a Kafka or Redpanda topic.
The Python generator here has the same interface:

  Production                    This simulation
  ─────────────────────────     ───────────────────────────────────
  consumer.poll(timeout=1.0)    next(prescribing_event_stream())
  Kafka partition offset        batch_num counter
  Consumer group rebalance      (not simulated — single consumer)
  At-least-once delivery        (not simulated — replay only)

Micro-batch trade-off
~~~~~~~~~~~~~~~~~~~~~
  BATCH_SIZE = 10    low latency (table updates often), high overhead
  BATCH_SIZE = 500   higher throughput, higher latency per update

This is the same parameter as Spark's trigger(processingTime="30s") or
Kafka Streams' commit.interval.ms.

NHS context
~~~~~~~~~~~
NHS 111 handles ~80,000 calls/day.  A 5-minute rolling window on a stream
of call records can detect demand surges in near real-time — impossible with
a nightly batch job.  Sepsis screening on vital-signs streams, A&E breach
alerting, and bed capacity management all follow this pattern.

Run
---
    uv run python scripts/07_stream.py

    # Smaller batches (see latency vs throughput trade-off)
    BATCH_SIZE=50 uv run python scripts/07_stream.py

Prerequisites
-------------
    Run scripts/01_fetch.py first to populate lake/
"""

from __future__ import annotations

import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

import duckdb

from pipeline.stream import stream_into_duckdb

LAKE_DIR = pathlib.Path("lake")


def _print_result(rel: duckdb.DuckDBPyRelation) -> None:
    cols = [d[0] for d in rel.description]
    rows = rel.fetchall()
    col_widths = [
        max(len(c), max((len(str(r[i])) for r in rows), default=0))
        for i, c in enumerate(cols)
    ]
    header = "  ".join(c.ljust(col_widths[i]) for i, c in enumerate(cols))
    print(header)
    print("  ".join("-" * w for w in col_widths))
    for row in rows:
        print("  ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)))


DB_PATH = pathlib.Path("pipeline.duckdb")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
STREAM_DRUG = "metformin"  # stream one drug for the demo


def main() -> None:
    jsonl_path = LAKE_DIR / STREAM_DRUG / "prescribing.jsonl"
    if not jsonl_path.exists():
        print(f"Bronze data not found for {STREAM_DRUG}.")
        print("Run scripts/01_fetch.py first.")
        return

    print(f"\nStreaming {STREAM_DRUG} prescribing records into DuckDB")
    print(f"Batch size: {BATCH_SIZE} records per micro-batch")
    print(f"Database: {DB_PATH.resolve()}\n")

    result = stream_into_duckdb(LAKE_DIR, STREAM_DRUG, DB_PATH, batch_size=BATCH_SIZE)

    print("\nStream complete:")
    print(f"  Drug:         {result['drug']}")
    print(f"  Batches:      {result['total_batches']}")
    print(f"  Total rows:   {result['total_records']:,}")
    print(
        f"  Total items:  {result['total_items']:,}"
        if result["total_items"]
        else "  Total items:  N/A"
    )
    print(
        f"  Total cost:   £{result['total_cost_gbp']:,.2f}"
        if result["total_cost_gbp"]
        else "  Total cost:   N/A"
    )

    # Show the streaming table mid-snapshot
    print("\nStreaming table snapshot (streaming.live_prescribing):")
    with duckdb.connect(str(DB_PATH)) as con:
        df = con.execute(f"""
            SELECT
                batch_num,
                COUNT(*)                   AS records_in_batch,
                ROUND(SUM(actual_cost), 2) AS batch_cost_gbp,
                MIN(arrived_at)            AS first_arrived
            FROM streaming.live_prescribing
            WHERE drug = '{STREAM_DRUG}'
            GROUP BY batch_num
            ORDER BY batch_num
            LIMIT 10
        """)
        _print_result(df)
        total_batches = result["total_batches"]
        if total_batches > 10:
            print(f"  ... ({total_batches - 10} more batches not shown)")

    print("\nTo swap this generator for a real Kafka consumer:")
    print("  from confluent_kafka import Consumer")
    print("  consumer = Consumer({'bootstrap.servers': 'localhost:9092', ...})")
    print("  consumer.subscribe(['prescribing-events'])")
    print("  # Replace prescribing_event_stream() with consumer.poll()")


if __name__ == "__main__":
    main()
