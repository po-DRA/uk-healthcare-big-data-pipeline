"""
Temporary test script — run in Codespaces to verify NHSBSA EPD API works.
Delete this file after the fetch is confirmed working.

Usage:
    uv run python test_nhsbsa_fetch.py
"""
import json
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# NHSBSA EPD API config
# ---------------------------------------------------------------------------
# Monthly prescribing data — one resource per month, named EPD_YYYYMM.
# We fetch the most recent complete month available.
NHSBSA_API = "https://opendata.nhsbsa.net/api/3/action/datastore_search"

# Confirmed BNF codes (BNF_CHEMICAL_SUBSTANCE field in EPD dataset)
DRUGS = {
    "metformin":    "0601022B0",
    "atorvastatin": "0212000B0",
    "lisinopril":   "0205051L0",
    "salbutamol":   "0301011R0",
}

# Latest available month — update as new EPD files are published
MONTHS = ["EPD_202503", "EPD_202502", "EPD_202501"]


def get_latest_resource_id() -> str:
    """Return the most recent EPD resource_id available on NHSBSA Open Data."""
    url = "https://opendata.nhsbsa.net/api/3/action/package_show?id=english-prescribing-data-epd"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    resources = d["result"]["resources"]
    # Resources are named EPD_YYYYMM — take the last one
    epd = [res for res in resources if res["name"].startswith("EPD_2")]
    latest = sorted(epd, key=lambda x: x["name"])[-1]
    print(f"Latest EPD resource: {latest['name']} (id={latest['id'][:8]}...)")
    return latest["id"]


def fetch_drug(resource_id: str, drug_name: str, bnf_code: str, limit: int = 1000) -> list[dict]:
    """Fetch prescribing records for one drug from one monthly EPD file."""
    params = urllib.parse.urlencode({
        "resource_id": resource_id,
        "limit": limit,
        "filters": json.dumps({"BNF_CHEMICAL_SUBSTANCE": bnf_code}),
    })
    url = f"{NHSBSA_API}?{params}"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)

    if not d.get("success"):
        raise RuntimeError(f"NHSBSA API error: {d.get('error')}")

    result = d["result"]
    records = result["records"]
    total = result["total"]
    print(f"  {drug_name}: {len(records)} records fetched (total in month: {total:,})")

    # Normalise to the shape the pipeline expects
    normalised = [
        {
            "date":        str(rec["YEAR_MONTH"]),
            "actual_cost": rec.get("ACTUAL_COST"),
            "items":       rec.get("ITEMS"),
            "quantity":    rec.get("QUANTITY"),
            "row_id":      rec.get("PRACTICE_CODE", ""),
            "setting":     "4",  # EPD is primary care — equivalent to setting=4
            "ccg":         rec.get("ICB_CODE", ""),
            "icb_name":    rec.get("ICB_NAME", ""),
            "drug":        drug_name,
        }
        for rec in records
        if not rec.get("UNIDENTIFIED", False)  # skip unidentified practices
    ]
    print(f"  {drug_name}: {len(normalised)} rows after removing UNIDENTIFIED")
    return normalised


if __name__ == "__main__":
    print("=" * 60)
    print("NHSBSA EPD API — connectivity test")
    print("=" * 60)

    try:
        resource_id = get_latest_resource_id()
    except Exception as exc:
        print(f"FAIL — could not get resource list: {exc}")
        raise SystemExit(1)

    all_ok = True
    for drug_name, bnf_code in DRUGS.items():
        print(f"\nFetching {drug_name} ({bnf_code})...")
        try:
            records = fetch_drug(resource_id, drug_name, bnf_code, limit=500)
            if records:
                sample = records[0]
                print(f"  Sample record: date={sample['date']}, "
                      f"cost={sample['actual_cost']}, items={sample['items']}, "
                      f"icb={sample['icb_name'][:30]}")
            else:
                print(f"  WARNING: 0 records after filtering UNIDENTIFIED")
                all_ok = False
        except Exception as exc:
            print(f"  FAIL: {exc}")
            all_ok = False

    print("\n" + "=" * 60)
    if all_ok:
        print("ALL DRUGS FETCHED SUCCESSFULLY — safe to proceed with full refactor")
    else:
        print("SOME DRUGS FAILED — investigate before proceeding")
    print("=" * 60)
