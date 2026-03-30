import marimo

__generated_with = "0.6.0"
app = marimo.App(title="02 · Raw Data Lake — Variety")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 02 — Raw Data Lake

        **Learning objective:** Persist both data sources to the lake in their
        native formats (JSONL and JSON), and see **Variety** made physically visible
        as two completely different file structures under the same directory tree.

        **V's demonstrated:** Variety (JSONL vs JSON coexisting in the lake)

        **Estimated time:** 5 minutes

        > **Prerequisite:** Run notebook 01 first so that `results` are in memory,
        > _or_ re-fetch here if starting fresh.
        """
    )
    return


@app.cell
def __():
    import json
    import pathlib
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from pipeline.fetch import DRUG_CODES, fetch_nhs_pages, fetch_openprescribing
    from pipeline.lake import lake_summary, read_lake, write_lake
    return (
        DRUG_CODES,
        ThreadPoolExecutor,
        as_completed,
        fetch_nhs_pages,
        fetch_openprescribing,
        json,
        lake_summary,
        pathlib,
        read_lake,
        write_lake,
    )


@app.cell
def __(DRUG_CODES, ThreadPoolExecutor, as_completed, fetch_nhs_pages, fetch_openprescribing):
    DRUGS = [(bnf, name) for name, bnf in DRUG_CODES.items()]

    fetch_jobs = []
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
            try:
                results.append(future.result())
            except Exception as exc:
                fn_name, args = future_map[future]
                print(f"  ERROR in {fn_name}({args}): {exc}")

    print(f"Fetched {len(results)} payloads")
    return DRUGS, fetch_jobs, future_map, results


@app.cell
def __(mo):
    mo.md(
        """
        ## Writing to the Lake

        `write_lake()` uses the payload's `type` field to decide the format:
        - `openprescribing` → **JSONL** (one JSON object per line, no array wrapper)
        - `nhs_pages` → **JSON** (full nested dict with pages list)

        This mirrors how real data lakes work: the format matches the access pattern.
        JSONL is optimised for streaming reads (DuckDB can scan it line-by-line);
        JSON preserves the page hierarchy for NLP processing.
        """
    )
    return


@app.cell
def __(pathlib, results, write_lake):
    LAKE_DIR = pathlib.Path("lake")

    written_paths = []
    for payload in results:
        path = write_lake(payload, LAKE_DIR)
        written_paths.append(path)

    print(f"\nLake populated: {len(written_paths)} files written")
    return LAKE_DIR, path, payload, written_paths


@app.cell
def __(mo):
    mo.md("## Lake Directory Structure and File Sizes")
    return


@app.cell
def __(LAKE_DIR, lake_summary):
    summary = lake_summary(LAKE_DIR)

    print(f"{'Drug':<20} {'File':<25} {'Size (KB)':>10}")
    print("-" * 58)
    total_kb = 0
    for row in summary:
        print(f"{row['drug']:<20} {row['file']:<25} {row['size_kb']:>10.1f}")
        total_kb += row["size_kb"]
    print("-" * 58)
    print(f"{'Total':<46} {total_kb:>10.1f}")
    return row, summary, total_kb


@app.cell
def __(mo):
    mo.md(
        """
        ## Variety — Two Formats Side by Side

        ### JSONL (prescribing data)
        One JSON object per line — no outer array, no commas between records.
        DuckDB reads this with `read_json(..., format='newline_delimited')`.
        """
    )
    return


@app.cell
def __(LAKE_DIR):
    jsonl_path = LAKE_DIR / "metformin" / "prescribing.jsonl"
    with jsonl_path.open("r", encoding="utf-8") as fh:
        first_lines = [fh.readline().strip() for _ in range(3)]

    print("First 3 lines of lake/metformin/prescribing.jsonl:")
    print()
    for line in first_lines:
        print(line)
    return first_lines, fh, jsonl_path


@app.cell
def __(mo):
    mo.md(
        """
        ### JSON (NHS pages data)
        A nested dict — `pages` is a list of sections, each with heading, text, bullets.
        This preserves hierarchy for BeautifulSoup / NLP extraction in notebook 05.
        """
    )
    return


@app.cell
def __(LAKE_DIR, json):
    json_path = LAKE_DIR / "metformin" / "nhs_pages.json"
    with json_path.open("r", encoding="utf-8") as fh2:
        nhs_payload = json.load(fh2)

    print(f"lake/metformin/nhs_pages.json — top-level keys: {list(nhs_payload.keys())}")
    print(f"Number of page sections: {len(nhs_payload.get('pages', []))}")
    print()
    if nhs_payload.get("pages"):
        first_page = nhs_payload["pages"][0]
        print("First section:")
        print(f"  page_type : {first_page['page_type']}")
        print(f"  heading   : {first_page['heading']}")
        print(f"  text      : {first_page['text'][:120]}…")
        print(f"  bullets   : {first_page['bullets'][:2]}")
    return fh2, first_page, json_path, nhs_payload


@app.cell
def __(mo):
    mo.md(
        """
        ## Reading Back from the Lake

        `read_lake()` abstracts the format difference — callers just specify
        the drug name and data type.
        """
    )
    return


@app.cell
def __(LAKE_DIR, read_lake):
    metformin_records = read_lake("metformin", "openprescribing", LAKE_DIR)
    metformin_pages = read_lake("metformin", "nhs_pages", LAKE_DIR)

    print(f"Prescribing records read back: {len(metformin_records):,}")
    print(f"NHS page sections read back:   {len(metformin_pages)}")
    return metformin_pages, metformin_records


@app.cell
def __(mo):
    mo.md(
        """
        ## Summary

        You have:
        - Written 8 fetched payloads to `lake/` as JSONL and JSON files
        - Seen the directory structure and file sizes
        - Compared the two formats side by side — this is **Variety** made tangible
        - Read records back via `read_lake()` to confirm round-trip integrity

        **Reflection question:** Why store raw data in a "lake" before transforming it,
        rather than transforming on ingest? Think about what happens if your
        transformation logic has a bug — can you fix it without re-fetching?

        **→ Next: [03_duckdb_query.py](03_duckdb_query.py) — query the lake directly with SQL**
        """
    )
    return


if __name__ == "__main__":
    app.run()
