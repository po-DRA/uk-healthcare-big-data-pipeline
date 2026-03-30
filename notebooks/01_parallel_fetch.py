import marimo

__generated_with = "0.6.0"
app = marimo.App(title="01 · Parallel Fetch — Volume & Velocity")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 01 — Parallel Fetch

        **Learning objective:** Fetch prescribing records and NHS clinical pages for
        four drugs concurrently using `ThreadPoolExecutor`, and see how parallelism
        demonstrates the **Velocity** V.

        **V's demonstrated:** Volume (total record counts), Velocity (parallel timing)

        **Estimated time:** 8 minutes (includes live network calls ~60 s)
        """
    )
    return


@app.cell
def __():
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from pipeline.fetch import DRUG_CODES, fetch_nhs_pages, fetch_openprescribing
    return (
        DRUG_CODES,
        ThreadPoolExecutor,
        as_completed,
        fetch_nhs_pages,
        fetch_openprescribing,
        time,
    )


@app.cell
def __(mo):
    mo.md(
        """
        ## The Drug Catalogue

        We'll fetch data for four of the most prescribed drugs in England.
        These are all first-line treatments for very common chronic conditions —
        which is exactly why their prescribing volumes are so large.
        """
    )
    return


@app.cell
def __(DRUG_CODES):
    DRUGS = [
        (bnf_code, drug_name)
        for drug_name, bnf_code in DRUG_CODES.items()
    ]
    print("Drugs to fetch:")
    for code, name in DRUGS:
        print(f"  {name:20s}  BNF: {code}")
    return (DRUGS,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Sequential Baseline (simulated)

        Before running in parallel, let's estimate how long a sequential approach
        would take. We'll time one fetch and multiply by 8 (4 drugs × 2 sources).
        """
    )
    return


@app.cell
def __(DRUGS, fetch_openprescribing, time):
    # Time one fetch to establish a sequential baseline
    _bnf, _name = DRUGS[0]
    _t0 = time.perf_counter()
    _sample = fetch_openprescribing(_bnf, _name)
    _one_fetch_seconds = time.perf_counter() - _t0

    sequential_estimate = _one_fetch_seconds * 8
    print(f"Single fetch time:          {_one_fetch_seconds:.2f} s")
    print(f"Sequential estimate (×8):   {sequential_estimate:.2f} s")
    return sequential_estimate,


@app.cell
def __(mo):
    mo.md(
        """
        ## Parallel Fetch — ThreadPoolExecutor

        Now fetch **all 8 sources simultaneously** using 8 worker threads.
        `ThreadPoolExecutor` is ideal here because each fetch is I/O-bound
        (waiting for HTTP responses) — the GIL does not limit us.
        """
    )
    return


@app.cell
def __(
    DRUGS,
    ThreadPoolExecutor,
    as_completed,
    fetch_nhs_pages,
    fetch_openprescribing,
    time,
):
    _t0 = time.perf_counter()

    fetch_jobs = []
    # Build a list of (callable, args) pairs — one per source per drug
    for _bnf_code, _drug_name in DRUGS:
        fetch_jobs.append((fetch_openprescribing, (_bnf_code, _drug_name)))
        fetch_jobs.append((fetch_nhs_pages, (_drug_name,)))

    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {
            executor.submit(fn, *args): (fn.__name__, args)
            for fn, args in fetch_jobs
        }
        for future in as_completed(future_map):
            fn_name, args = future_map[future]
            try:
                payload = future.result()
                results.append(payload)
            except Exception as exc:
                print(f"  ERROR in {fn_name}({args}): {exc}")

    parallel_time = time.perf_counter() - _t0
    print(f"\nParallel fetch complete in {parallel_time:.2f} s ({len(results)} payloads)")
    return fetch_jobs, future_map, parallel_time, results


@app.cell
def __(mo, parallel_time, sequential_estimate):
    speedup = sequential_estimate / parallel_time if parallel_time > 0 else 0
    mo.md(
        f"""
        ## Velocity Result

        | Mode | Time |
        |------|------|
        | Sequential estimate | {sequential_estimate:.1f} s |
        | Parallel actual | {parallel_time:.1f} s |
        | **Speedup** | **{speedup:.1f}×** |

        This speedup is **Velocity** in practice — the same data, acquired faster
        by overlapping I/O wait times across threads.
        """
    )
    return (speedup,)


@app.cell
def __(mo):
    mo.md("## Volume — Total Prescribing Records")
    return


@app.cell
def __(results):
    prescribing_payloads = [r for r in results if r.get("type") == "openprescribing"]
    total_rows = sum(p["total_rows"] for p in prescribing_payloads)

    print("OpenPrescribing record counts by drug:")
    for p in sorted(prescribing_payloads, key=lambda x: x["total_rows"], reverse=True):
        print(f"  {p['drug']:20s}  {p['total_rows']:>10,} records")

    print(f"\nTotal prescribing records across all 4 drugs: {total_rows:,}")
    print(f"At ~150 bytes per record, that's ~{total_rows * 150 / 1_000_000:.0f} MB of raw data")
    return prescribing_payloads, total_rows


@app.cell
def __(mo):
    mo.md("## Variety — NHS Pages Character Counts")
    return


@app.cell
def __(results):
    nhs_payloads = [r for r in results if r.get("type") == "nhs_pages"]

    print("NHS.uk pages fetched:")
    total_chars = 0
    for payload in sorted(nhs_payloads, key=lambda x: x["drug"]):
        pages = payload.get("pages", [])
        chars = sum(
            len(p.get("text", "")) + sum(len(b) for b in p.get("bullets", []))
            for p in pages
        )
        total_chars += chars
        print(f"  {payload['drug']:20s}  {len(pages):3d} sections   {chars:>8,} chars")

    print(f"\nTotal clinical text extracted: {total_chars:,} characters")
    print("Format: structured JSON containing unstructured prose — this is Variety")
    return nhs_payloads, total_chars


@app.cell
def __(mo):
    mo.md(
        """
        ## Summary

        You have:
        - Fetched 8 data sources in parallel using `ThreadPoolExecutor`
        - Seen the Volume V: hundreds of thousands of practice-level records
        - Seen the Velocity V: parallel fetch beats sequential by several times
        - Seen the Variety V: two completely different data shapes from two sources

        **Reflection question:** The parallel speedup is significant but not 8×.
        Why not? What are the limiting factors in parallel HTTP I/O?
        _(Hint: think about server-side rate limiting, DNS resolution, and TCP handshakes.)_

        **→ Next: [02_raw_lake.py](02_raw_lake.py) — write both formats to the data lake**
        """
    )
    return


if __name__ == "__main__":
    app.run()
