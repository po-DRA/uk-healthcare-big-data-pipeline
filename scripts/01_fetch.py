"""
Script 01 — Fetch data from NHSBSA EPD and NHS.uk

What this does
--------------
Fetches two types of data for 12 drugs and writes them to the Bronze lake:

  1. Structured prescribing records from the NHSBSA English Prescribing Dataset
     (EPD) — streamed from one or more monthly CSV files.

  2. Unstructured clinical prose from NHS.uk medicines pages — three sub-pages
     per drug (side effects, contraindications, interactions), parsed from HTML.

Multi-month Bronze
------------------
Each EPD month is written as a separate partition:
    lake/{drug}/{EPD_YYYYMM}/prescribing.jsonl

Set NHSBSA_MONTHS to fetch multiple months of history.  Each monthly run adds
a new partition without overwriting previous months — Bronze is write-once per
partition.  Silver automatically reads across all partitions.

This mirrors a scheduled job pattern: running this script monthly via a cron
or Prefect schedule accumulates a growing Bronze lake.

Why this matters
----------------
This is the Extract step of ETL.  Two completely different data structures
(tabular CSV vs HTML prose) are fetched and written to a single lake directory.
This is the Variety V in action.  Parallelism (ThreadPoolExecutor across drugs
× months × sources) demonstrates the Velocity V.

Run
---
    uv run python scripts/01_fetch.py

Environment variables
---------------------
    NHSBSA_MONTHS=3            months of history to fetch (default: 1 = latest)
    NHSBSA_ROWS_PER_DRUG=500   rows per drug per month (default: all rows)
    HTTP_TIMEOUT=20            seconds before HTTP request is abandoned
"""

from __future__ import annotations

import logging
import pathlib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path so pipeline package is importable
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from pipeline.fetch import DRUG_CODES, _get_epd_urls, fetch_nhs_pages, fetch_nhsbsa
from pipeline.lake import write_lake

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
_log = logging.getLogger(__name__)

LAKE_DIR = pathlib.Path("lake")


def main() -> None:
    import os

    LAKE_DIR.mkdir(exist_ok=True)
    drugs = list(DRUG_CODES.items())  # [(name, bnf_code), ...]
    n_months = int(os.environ.get("NHSBSA_MONTHS", "1"))

    print(f"\nResolving last {n_months} EPD month(s)...")
    epd_months = _get_epd_urls(n_months)
    print(f"  → {', '.join(r for _, r in epd_months)}")
    print(
        f"\nFetching {len(drugs)} drugs × {len(epd_months)} month(s) × 2 sources in parallel..."
    )
    print(f"Lake directory: {LAKE_DIR.resolve()}\n")

    failed: list[str] = []

    with ThreadPoolExecutor(
        max_workers=min(32, len(drugs) * len(epd_months) * 2)
    ) as pool:
        futures = {}
        for drug_name, bnf_code in drugs:
            for csv_url, resource_name in epd_months:
                label = f"{drug_name}/{resource_name}"
                futures[
                    pool.submit(
                        fetch_nhsbsa,
                        bnf_code,
                        drug_name,
                        csv_url=csv_url,
                        resource_name=resource_name,
                    )
                ] = label
            # NHS pages are not monthly — fetch once per drug
            futures[pool.submit(fetch_nhs_pages, drug_name)] = f"{drug_name}/nhs_pages"

        for future in as_completed(futures):
            label = futures[future]
            try:
                payload = future.result()
                write_lake(payload, LAKE_DIR)
                if "nhs_pages" in label:
                    n = len(payload.get("pages", []))
                    print(f"  OK  {label}: {n} sections")
                else:
                    n = payload.get("total_rows", 0)
                    resource = payload.get("resource", "")
                    print(f"  OK  {label}: {n:,} rows [{resource}]")
            except Exception as exc:
                print(f"  FAIL {label}: {exc}")
                failed.append(label)

    print("\nLake contents:")
    for drug_dir in sorted(LAKE_DIR.iterdir()):
        if not drug_dir.is_dir():
            continue
        partitions = [p.name for p in sorted(drug_dir.iterdir()) if p.is_dir()]
        flat_files = [f.name for f in drug_dir.iterdir() if f.is_file()]
        parts_str = f"[{', '.join(partitions)}]" if partitions else ""
        files_str = ", ".join(flat_files)
        print(f"  {drug_dir.name}/: {parts_str} {files_str}".rstrip())

    if failed:
        print(f"\nWARNING: {len(failed)} source(s) failed: {', '.join(failed)}")
        print("Downstream scripts will work with whatever was fetched successfully.")
    else:
        print("\nAll sources fetched successfully.")


if __name__ == "__main__":
    main()
