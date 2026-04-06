"""
Temporary test script — run in Codespaces to find a working NHSBSA fetch approach.
Delete this file after confirmed working.

Usage:
    uv run python test_nhsbsa_fetch.py
"""
import gzip
import io
import json
import urllib.parse
import urllib.request
import zipfile

NHSBSA_API = "https://opendata.nhsbsa.net/api/3/action/datastore_search"
METFORMIN_BNF = "0601022B0"


def test_unfiltered(resource_id: str) -> None:
    """Baseline: no filter, small limit — should return 200."""
    print("\n[Test 1] Unfiltered request (known-good baseline)...")
    params = urllib.parse.urlencode({"resource_id": resource_id, "limit": 2})
    with urllib.request.urlopen(f"{NHSBSA_API}?{params}", timeout=20) as r:
        d = json.load(r)
    print(f"  status: {'OK' if d['success'] else 'FAIL'}, total rows: {d['result']['total']:,}")


def test_filter_get(resource_id: str) -> None:
    """Filter via GET with JSON-encoded filters param."""
    print("\n[Test 2] GET with filters=JSON...")
    params = urllib.parse.urlencode({
        "resource_id": resource_id,
        "limit": 5,
        "filters": json.dumps({"BNF_CHEMICAL_SUBSTANCE": METFORMIN_BNF}),
    })
    try:
        with urllib.request.urlopen(f"{NHSBSA_API}?{params}", timeout=20) as r:
            d = json.load(r)
        print(f"  status: {'OK' if d['success'] else 'FAIL'}, records: {len(d['result']['records'])}, total: {d['result']['total']:,}")
    except Exception as exc:
        print(f"  FAIL: {exc}")


def test_filter_post(resource_id: str) -> None:
    """Filter via POST with JSON body."""
    print("\n[Test 3] POST with JSON body filters...")
    body = json.dumps({
        "resource_id": resource_id,
        "limit": 5,
        "filters": {"BNF_CHEMICAL_SUBSTANCE": METFORMIN_BNF},
    }).encode()
    req = urllib.request.Request(
        NHSBSA_API,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.load(r)
        print(f"  status: {'OK' if d['success'] else 'FAIL'}, records: {len(d['result']['records'])}, total: {d['result']['total']:,}")
    except Exception as exc:
        print(f"  FAIL: {exc}")


def test_csv_download() -> None:
    """Download the PCA (Prescriptions Cost Analysis) CSV — smaller aggregated dataset."""
    print("\n[Test 4] NHSBSA Prescriptions Cost Analysis CSV download (2024)...")
    # PCA is aggregated annual data, much smaller than EPD
    url = "https://www.nhsbsa.nhs.uk/sites/default/files/2024-09/pca-summary-tables-2023-24-v2.xlsx"
    req = urllib.request.Request(url, headers={"User-Agent": "uk-healthcare-pipeline/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            size = len(r.read())
        print(f"  OK — downloaded {size:,} bytes")
    except Exception as exc:
        print(f"  FAIL: {exc}")


def test_fingertips_data() -> None:
    """Fingertips OHID API — diabetes prevalence by ICB."""
    print("\n[Test 5] Fingertips — diabetes indicator data by ICB...")
    # Indicator 241 = Diabetes: QOF prevalence (all ages)
    # Area type 221 = Sub-ICB
    url = "https://fingertips.phe.org.uk/api/all_data/json/by_indicator_id?indicator_ids=241&area_type_id=221&parent_area_type_id=220"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            raw = r.read()
        d = json.loads(raw)
        print(f"  OK — {len(d):,} records")
        if d:
            sample = d[0]
            print(f"  Sample: area={sample.get('AreaName','')}, year={sample.get('Year','')}, value={sample.get('Value','')}")
    except Exception as exc:
        print(f"  FAIL: {exc}")


def test_fingertips_indicator_list() -> None:
    """Fingertips — list indicators in the diabetes profile."""
    print("\n[Test 6] Fingertips — diabetes profile indicator list...")
    # Profile 71 = Diabetes
    url = "https://fingertips.phe.org.uk/api/indicator_metadata/all/by_profile_id?profile_ids=71"
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            d = json.load(r)
        print(f"  OK — {len(d)} indicators in diabetes profile")
        # Print first 5 indicator names
        for k, v in list(d.items())[:5]:
            name = v.get("Descriptive", {}).get("Name", "")
            print(f"    {k}: {name}")
    except Exception as exc:
        print(f"  FAIL: {exc}")


def get_latest_resource_id() -> str:
    url = "https://opendata.nhsbsa.net/api/3/action/package_show?id=english-prescribing-data-epd"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    epd = [res for res in d["result"]["resources"] if res["name"].startswith("EPD_2")]
    latest = sorted(epd, key=lambda x: x["name"])[-1]
    print(f"Latest EPD resource: {latest['name']}")
    return latest["id"]


if __name__ == "__main__":
    print("=" * 60)
    print("NHSBSA / Fingertips API — approach testing")
    print("=" * 60)

    resource_id = get_latest_resource_id()

    test_unfiltered(resource_id)
    test_filter_get(resource_id)
    test_filter_post(resource_id)
    test_csv_download()
    test_fingertips_data()
    test_fingertips_indicator_list()

    print("\n" + "=" * 60)
    print("Done — paste output back to decide which approach to use")
    print("=" * 60)
