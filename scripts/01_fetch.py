"""
Script 01 — Fetch data from NHSBSA EPD and NHS.uk

What this does
--------------
Fetches two types of data for four drugs and writes them to the Bronze lake:

  1. Structured prescribing records from the NHSBSA English Prescribing Dataset
     (EPD) — streamed from the latest monthly CSV file (~50 MB read, ~10 s).

  2. Unstructured clinical prose from NHS.uk medicines pages — three sub-pages
     per drug (side effects, contraindications, interactions), parsed from HTML.

Why this matters
----------------
This is the Extract step of ETL.  Two completely different data structures
(tabular CSV vs HTML prose) are fetched and written to a single lake directory.
This is the Variety V in action.

The fetch is parallelised with ThreadPoolExecutor — four drugs × two sources
run concurrently rather than sequentially.  This demonstrates the Velocity V:
parallelism reduces total fetch time even when volume is high.

Run
---
    uv run python scripts/01_fetch.py

Environment variables
---------------------
    NHSBSA_ROWS_PER_DRUG=500   rows per drug from NHSBSA (default: 500)
    HTTP_TIMEOUT=20            seconds before HTTP request is abandoned
"""

from __future__ import annotations

import logging
import pathlib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path so pipeline package is importable
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from pipeline.fetch import DRUG_CODES, fetch_nhsbsa, fetch_nhs_pages
from pipeline.lake import write_lake

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
_log = logging.getLogger(__name__)

LAKE_DIR = pathlib.Path("lake")


def main() -> None:
    LAKE_DIR.mkdir(exist_ok=True)
    drugs = list(DRUG_CODES.items())  # [(name, bnf_code), ...]

    print(f"\nFetching {len(drugs)} drugs × 2 sources in parallel...")
    print(f"Lake directory: {LAKE_DIR.resolve()}\n")

    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=len(drugs) * 2) as pool:
        futures = {}
        for drug_name, bnf_code in drugs:
            futures[pool.submit(fetch_nhsbsa, bnf_code, drug_name)] = f"{drug_name}/prescribing"
            futures[pool.submit(fetch_nhs_pages, drug_name)] = f"{drug_name}/nhs_pages"

        for future in as_completed(futures):
            label = futures[future]
            try:
                payload = future.result()
                write_lake(payload, LAKE_DIR)
                if "prescribing" in label:
                    n = payload.get("total_rows", 0)
                    resource = payload.get("resource", "")
                    print(f"  OK  {label}: {n:,} rows from {resource}")
                else:
                    n = len(payload.get("pages", []))
                    print(f"  OK  {label}: {n} sections")
            except Exception as exc:
                print(f"  FAIL {label}: {exc}")
                failed.append(label)

    print(f"\nLake contents:")
    for drug_dir in sorted(LAKE_DIR.iterdir()):
        if drug_dir.is_dir():
            files = [f.name for f in drug_dir.iterdir()]
            print(f"  {drug_dir.name}/: {', '.join(files)}")

    if failed:
        print(f"\nWARNING: {len(failed)} source(s) failed: {', '.join(failed)}")
        print("Downstream scripts will work with whatever was fetched successfully.")
    else:
        print("\nAll sources fetched successfully.")


if __name__ == "__main__":
    main()
