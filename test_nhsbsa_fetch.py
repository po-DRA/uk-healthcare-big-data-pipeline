"""
Temporary test script — run in Codespaces to verify streaming CSV approach.
Delete this file after confirmed working.

Usage:
    uv run python test_nhsbsa_fetch.py
"""
import csv
import io
import json
import time
import urllib.request

# BNF codes for our four drugs (confirmed correct)
TARGET_DRUGS = {
    "0601022B0": "metformin",
    "0212000B0": "atorvastatin",
    "0205051L0": "lisinopril",
    "0301011R0": "salbutamol",
}

ROWS_PER_DRUG = 500  # stop once we have this many rows for every drug


def get_latest_csv_url() -> str:
    """Get the download URL for the most recent EPD CSV file."""
    url = "https://opendata.nhsbsa.net/api/3/action/package_show?id=english-prescribing-data-epd"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    resources = [res for res in d["result"]["resources"] if res["name"].startswith("EPD_2")]
    latest = sorted(resources, key=lambda x: x["name"])[-1]
    print(f"  Latest file: {latest['name']}")
    return latest["url"], latest["name"]


def stream_and_filter(csv_url: str, resource_name: str) -> dict[str, list[dict]]:
    """
    Stream the EPD CSV and collect up to ROWS_PER_DRUG rows per drug.
    Stops as soon as all drugs have enough data — no need to read the full 6.9GB.
    """
    collected: dict[str, list[dict]] = {name: [] for name in TARGET_DRUGS.values()}
    needed = set(TARGET_DRUGS.values())  # drugs still needing more rows

    req = urllib.request.Request(
        csv_url,
        headers={"User-Agent": "uk-healthcare-pipeline/1.0 (+https://github.com/po-DRA)"},
    )

    bytes_read = 0
    lines_read = 0
    start = time.time()
    buf = ""

    with urllib.request.urlopen(req, timeout=60) as r:
        header = None
        while needed:
            chunk = r.read(512 * 1024)  # 512KB chunks
            if not chunk:
                break
            bytes_read += len(chunk)
            buf += chunk.decode("utf-8", errors="replace")
            lines = buf.split("\n")
            buf = lines[-1]  # keep incomplete last line

            for line in lines[:-1]:
                lines_read += 1
                if header is None:
                    header = line.strip().split(",")
                    continue
                if not line.strip():
                    continue

                # Fast pre-filter: check BNF code before full CSV parse
                matched_code = next(
                    (code for code in TARGET_DRUGS if code in line), None
                )
                if not matched_code:
                    continue

                drug_name = TARGET_DRUGS[matched_code]
                if drug_name not in needed:
                    continue

                # Full parse only for matching rows
                try:
                    row = next(csv.DictReader([line], fieldnames=header))
                except Exception:
                    continue

                if row.get("BNF_CHEMICAL_SUBSTANCE") != matched_code:
                    continue
                if row.get("UNIDENTIFIED", "").lower() == "true":
                    continue

                collected[drug_name].append({
                    "date":        row.get("YEAR_MONTH", ""),
                    "actual_cost": row.get("ACTUAL_COST"),
                    "items":       row.get("ITEMS"),
                    "quantity":    row.get("QUANTITY"),
                    "row_id":      row.get("PRACTICE_CODE", ""),
                    "setting":     "4",
                    "ccg":         row.get("ICB_CODE", ""),
                    "icb_name":    row.get("ICB_NAME", ""),
                    "drug":        drug_name,
                })

                if len(collected[drug_name]) >= ROWS_PER_DRUG:
                    needed.discard(drug_name)
                    elapsed = time.time() - start
                    print(f"  {drug_name}: {ROWS_PER_DRUG} rows collected "
                          f"({bytes_read/1e6:.1f}MB read, {elapsed:.1f}s)")

    elapsed = time.time() - start
    print(f"\n  Total: {bytes_read/1e6:.1f}MB read, {lines_read:,} lines scanned, {elapsed:.1f}s")
    return collected


if __name__ == "__main__":
    print("=" * 60)
    print("NHSBSA EPD — streaming CSV filter test")
    print(f"Target: {ROWS_PER_DRUG} rows per drug, early-exit when all done")
    print("=" * 60)

    print("\nGetting latest EPD file URL...")
    try:
        csv_url, resource_name = get_latest_csv_url()
    except Exception as exc:
        print(f"FAIL: {exc}")
        raise SystemExit(1)

    print(f"\nStreaming {resource_name} and filtering for {len(TARGET_DRUGS)} drugs...")
    try:
        results = stream_and_filter(csv_url, resource_name)
    except Exception as exc:
        print(f"FAIL: {exc}")
        raise SystemExit(1)

    print("\nResults:")
    all_ok = True
    for drug, rows in results.items():
        if rows:
            s = rows[0]
            print(f"  {drug}: {len(rows)} rows — sample date={s['date']}, "
                  f"cost={s['actual_cost']}, items={s['items']}, icb={s['icb_name'][:25]}")
        else:
            print(f"  {drug}: 0 rows — PROBLEM")
            all_ok = False

    print("\n" + "=" * 60)
    if all_ok:
        print("SUCCESS — streaming approach works, safe to proceed with full refactor")
    else:
        print("PARTIAL — some drugs missing, investigate")
    print("=" * 60)
