"""
Temporary test script — run in Codespaces to find a working data source.
Delete this file after confirmed working.

Usage:
    uv run python test_nhsbsa_fetch.py
"""
import csv
import io
import json
import urllib.parse
import urllib.request
import zipfile

METFORMIN_BNF = "0601022B0"
ATORVASTATIN_BNF = "0212000B0"
TARGET_CODES = {METFORMIN_BNF, ATORVASTATIN_BNF}


def run(label: str, fn):
    print(f"\n{label}")
    try:
        fn()
    except Exception as exc:
        print(f"  FAIL: {type(exc).__name__}: {exc}")


def test_get_download_urls():
    """Get the actual file download URLs from package metadata."""
    print("\n[Test 1] Get CSV download URLs from package metadata...")
    url = "https://opendata.nhsbsa.net/api/3/action/package_show?id=english-prescribing-data-epd"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    resources = [res for res in d["result"]["resources"] if res["name"].startswith("EPD_2")]
    latest = sorted(resources, key=lambda x: x["name"])[-3:]  # last 3 months
    for res in latest:
        print(f"  {res['name']}: url={res.get('url', 'NO URL')[:80]}, format={res.get('format','')}")
    return latest[-1].get("url", "")


def test_head_csv(url: str):
    """Check if the CSV download URL is accessible (HEAD request)."""
    print(f"\n[Test 2] HEAD request to CSV download URL...")
    print(f"  URL: {url[:80]}")
    req = urllib.request.Request(url, method="HEAD",
                                  headers={"User-Agent": "uk-healthcare-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        size = r.headers.get("Content-Length", "unknown")
        content_type = r.headers.get("Content-Type", "unknown")
        print(f"  OK — status={r.status}, size={size} bytes, type={content_type}")


def test_stream_csv(url: str):
    """Stream first 2MB of CSV, count metformin/atorvastatin rows."""
    print(f"\n[Test 3] Stream CSV and filter for 2 drugs (first 2MB)...")
    req = urllib.request.Request(url, headers={"User-Agent": "uk-healthcare-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        chunk = r.read(2 * 1024 * 1024)  # read first 2MB only

    print(f"  Downloaded {len(chunk):,} bytes")

    # Try parsing as CSV directly
    try:
        text = chunk.decode("utf-8", errors="replace")
        lines = text.splitlines()
        print(f"  Lines in chunk: {len(lines)}")
        if lines:
            print(f"  Header: {lines[0][:120]}")
        # Count matching rows
        matches = [l for l in lines[1:] if METFORMIN_BNF in l or ATORVASTATIN_BNF in l]
        print(f"  Rows matching metformin/atorvastatin: {len(matches)}")
        if matches:
            print(f"  Sample match: {matches[0][:120]}")
    except Exception as exc:
        # Try as zip
        print(f"  Not plain CSV ({exc}), trying ZIP...")
        try:
            with zipfile.ZipFile(io.BytesIO(chunk)) as z:
                print(f"  ZIP contents: {z.namelist()}")
        except Exception as exc2:
            print(f"  Not ZIP either: {exc2}")


def test_nhsbsa_unfiltered_fields():
    """Confirm field names and one data row from unfiltered call."""
    print("\n[Test 4] NHSBSA datastore unfiltered — confirm fields + sample row...")
    params = urllib.parse.urlencode({"resource_id": "EPD_202503", "limit": 5})
    url = f"https://opendata.nhsbsa.net/api/3/action/datastore_search?{params}"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    fields = [f["id"] for f in d["result"]["fields"]]
    print(f"  Fields: {fields}")
    rec = d["result"]["records"][0]
    print(f"  Sample: BNF={rec.get('BNF_CHEMICAL_SUBSTANCE')}, "
          f"desc={rec.get('CHEMICAL_SUBSTANCE_BNF_DESCR','')}, "
          f"items={rec.get('ITEMS')}, cost={rec.get('ACTUAL_COST')}")


if __name__ == "__main__":
    print("=" * 60)
    print("NHSBSA CSV download approach test")
    print("=" * 60)

    run("[Test 4] Unfiltered datastore + field check", test_nhsbsa_unfiltered_fields)

    csv_url = ""
    try:
        csv_url = test_get_download_urls()
    except Exception as exc:
        print(f"  FAIL: {exc}")

    if csv_url:
        run("[Test 2] HEAD request on download URL", lambda: test_head_csv(csv_url))
        run("[Test 3] Stream 2MB + filter for drugs", lambda: test_stream_csv(csv_url))
    else:
        print("\n  No download URL found — cannot run tests 2 and 3")

    print("\n" + "=" * 60)
    print("Paste full output back")
    print("=" * 60)
