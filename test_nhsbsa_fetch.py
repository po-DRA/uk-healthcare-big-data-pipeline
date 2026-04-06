"""
Temporary test script — run in Codespaces to find a working data source.
Delete this file after confirmed working.

Usage:
    uv run python test_nhsbsa_fetch.py
"""
import json
import urllib.parse
import urllib.request

NHSBSA_API = "https://opendata.nhsbsa.net/api/3/action/datastore_search"


def run(label: str, fn):
    print(f"\n{label}")
    try:
        fn()
    except Exception as exc:
        print(f"  FAIL: {exc}")


# ---------------------------------------------------------------------------
# NHSBSA tests
# ---------------------------------------------------------------------------

def test_nhsbsa_unfiltered():
    params = urllib.parse.urlencode({"resource_id": "EPD_202503", "limit": 2})
    with urllib.request.urlopen(f"{NHSBSA_API}?{params}", timeout=20) as r:
        d = json.load(r)
    print(f"  OK — total rows: {d['result']['total']:,}, records: {len(d['result']['records'])}")


def test_nhsbsa_package_show():
    url = "https://opendata.nhsbsa.net/api/3/action/package_show?id=english-prescribing-data-epd"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    resources = [res["name"] for res in d["result"]["resources"] if res["name"].startswith("EPD_2")]
    print(f"  OK — {len(resources)} monthly files, latest: {sorted(resources)[-1]}")


# ---------------------------------------------------------------------------
# Fingertips OHID tests
# ---------------------------------------------------------------------------

def test_fingertips_profiles():
    url = "https://fingertips.phe.org.uk/api/profiles"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    print(f"  OK — {len(d)} profiles available")


def test_fingertips_diabetes_indicators():
    # Profile 71 = Diabetes
    url = "https://fingertips.phe.org.uk/api/indicator_metadata/all/by_profile_id?profile_ids=71"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    print(f"  OK — {len(d)} indicators in diabetes profile")
    for k, v in list(d.items())[:5]:
        print(f"    {k}: {v.get('Descriptive', {}).get('Name', '')}")


def test_fingertips_data_by_icb():
    # Indicator 241 = Diabetes: QOF prevalence (all ages), area type 221 = Sub-ICB
    url = ("https://fingertips.phe.org.uk/api/all_data/json/by_indicator_id"
           "?indicator_ids=241&area_type_id=221&parent_area_type_id=220")
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.load(r)
    print(f"  OK — {len(d):,} data points")
    if d:
        s = d[0]
        print(f"  Sample: area={s.get('AreaName','')}, year={s.get('Year','')}, value={s.get('Value','')}, sex={s.get('Sex',{}).get('Name','')}")


def test_fingertips_area_types():
    url = "https://fingertips.phe.org.uk/api/area_types"
    with urllib.request.urlopen(url, timeout=20) as r:
        d = json.load(r)
    icb_types = [a for a in d if "ICB" in a.get("Name", "") or "Sub ICB" in a.get("Name", "")]
    print(f"  OK — {len(d)} area types, ICB-related: {[(a['Id'], a['Name']) for a in icb_types]}")


def test_fingertips_multiple_indicators():
    # Indicator 93015 = Hospital admissions for diabetes, 241 = Diabetes prevalence
    # 91282 = Diabetes: patients with controlled HbA1c
    url = ("https://fingertips.phe.org.uk/api/all_data/json/by_indicator_id"
           "?indicator_ids=93015,91282&area_type_id=221")
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.load(r)
    print(f"  OK — {len(d):,} data points across 2 indicators")
    indicators = set(p.get("IndicatorName", "") for p in d[:100])
    for name in indicators:
        print(f"    - {name}")


if __name__ == "__main__":
    print("=" * 60)
    print("Data source connectivity tests")
    print("=" * 60)

    run("[NHSBSA 1] package_show (resource listing)", test_nhsbsa_package_show)
    run("[NHSBSA 2] datastore_search unfiltered", test_nhsbsa_unfiltered)
    run("[Fingertips 1] profiles list", test_fingertips_profiles)
    run("[Fingertips 2] diabetes profile indicators", test_fingertips_diabetes_indicators)
    run("[Fingertips 3] area types (find ICB IDs)", test_fingertips_area_types)
    run("[Fingertips 4] diabetes prevalence data by Sub-ICB", test_fingertips_data_by_icb)
    run("[Fingertips 5] multiple indicators (admissions + HbA1c)", test_fingertips_multiple_indicators)

    print("\n" + "=" * 60)
    print("Paste full output back to decide which approach to use")
    print("=" * 60)
