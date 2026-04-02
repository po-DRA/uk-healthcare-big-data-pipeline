import marimo

__generated_with = "0.6.0"
app = marimo.App(title="05 · NLP on Clinical Text — Variety")


@app.cell
def __():
    import marimo as mo

    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 05 — NLP on Unstructured Clinical Text

        **Learning objective:** Extract clinical signal from NHS.uk prose using
        regex tokenisation and term-frequency analysis, and compare terminology
        patterns across drugs.

        **V's demonstrated:** Variety (unstructured → structured via NLP)

        **Estimated time:** 8 minutes

        > **Prerequisite:** Run notebook 02 first to populate `lake/`.
        """
    )
    return


@app.cell
def __():
    import json
    import pathlib

    import polars as pl

    from pipeline.nlp import STOP_WORDS, extract_sections, tokenise, top_terms

    return STOP_WORDS, extract_sections, json, pathlib, pl, tokenise, top_terms


@app.cell
def __(mo):
    mo.md(
        """
        ## Variety in Action — Raw HTML vs Extracted Text

        The NHS.uk pages are written as clinical patient information HTML.
        BeautifulSoup extracts the headings and paragraphs during the fetch step
        and saves them to JSON. Let's compare what the raw and extracted data look like.
        """
    )
    return


@app.cell
def __(json, pathlib):
    LAKE_DIR = pathlib.Path("lake")
    nhs_json_path = LAKE_DIR / "metformin" / "nhs_pages.json"

    with nhs_json_path.open("r", encoding="utf-8") as fh:
        nhs_payload = json.load(fh)

    # Pick the first real section with non-empty text
    first_section = next(
        (p for p in nhs_payload["pages"] if p.get("text")),
        nhs_payload["pages"][0] if nhs_payload["pages"] else {},
    )

    print("=== RAW NHS SECTION (as stored in the lake) ===")
    print(f"Page type : {first_section.get('page_type', '')}")
    print(f"Heading   : {first_section.get('heading', '')}")
    print(f"Text      : {first_section.get('text', '')[:300]}")
    print(f"Bullets   : {first_section.get('bullets', [])[:3]}")
    return LAKE_DIR, fh, first_section, nhs_json_path, nhs_payload


@app.cell
def __(STOP_WORDS, first_section, tokenise):
    raw_text = " ".join(
        [first_section.get("heading", ""), first_section.get("text", "")]
        + first_section.get("bullets", [])
    )

    tokens = tokenise(raw_text, STOP_WORDS)

    print("=== AFTER TOKENISATION ===")
    print(f"Raw text length : {len(raw_text)} characters")
    print(f"Token count     : {len(tokens)}")
    print(f"Unique tokens   : {len(set(tokens))}")
    print(f"First 20 tokens : {tokens[:20]}")
    return raw_text, tokens


@app.cell
def __(mo):
    mo.md(
        """
        ## Top 15 Terms per Drug

        Let's compute term frequencies across _all_ pages for each drug.
        """
    )
    return


@app.cell
def __(LAKE_DIR, top_terms):
    DRUGS = ["metformin", "atorvastatin", "lisinopril", "amlodipine"]
    drug_terms = {}

    for drug in DRUGS:
        try:
            df = top_terms(drug, LAKE_DIR, n=15)
            drug_terms[drug] = df
            print(f"\nTop 15 terms — {drug}:")
            print(df.select(["term", "frequency", "page_type"]).to_string())
        except FileNotFoundError:
            print(f"  Skipping {drug} — nhs_pages.json not found in lake")
    return DRUGS, df, drug, drug_terms


@app.cell
def __(mo):
    mo.md(
        """
        ## Comparing Terms Across Drugs

        Let's print the top 8 terms for each drug side by side for a quick comparison.
        """
    )
    return


@app.cell
def __(drug_terms, pl):
    # Build a wide comparison table
    comparison_rows = []
    for drug_name, terms_df in drug_terms.items():
        top8 = terms_df.sort("frequency", descending=True).head(8)["term"].to_list()
        comparison_rows.append({"drug": drug_name, "top_terms": ", ".join(top8)})

    comparison_df = pl.DataFrame(comparison_rows)
    print("Top 8 terms per drug:")
    for row in comparison_df.iter_rows(named=True):
        print(f"\n  {row['drug']:15s}: {row['top_terms']}")
    return comparison_df, comparison_rows, drug_name, row, terms_df, top8


@app.cell
def __(mo):
    mo.md(
        """
        ## Clinical Discussion

        Look at the term lists above and consider:

        1. **Shared terms**: Terms like "blood", "pressure", "heart" appear across
           multiple drugs because they treat related cardiovascular/metabolic conditions.

        2. **Drug-specific terms**: Metformin's list should show terms like "lactic",
           "acidosis", "kidney" (its main safety concern). Atorvastatin may show
           "muscle", "liver", "statins".

        3. **Absence as signal**: If a drug's contraindication page has very few
           sections extracted, this may indicate the NHS page structure changed
           (a Veracity issue) rather than the drug having few contraindications.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## Term Frequency by Page Type

        Side effects, contraindications, and interactions use different vocabulary.
        Let's see this breakdown for metformin.
        """
    )
    return


@app.cell
def __(drug_terms, pl):
    if "metformin" in drug_terms:
        met_terms = drug_terms["metformin"]
        print("Metformin terms by page type:")
        for pt in met_terms["page_type"].unique().to_list():
            pt_df = (
                met_terms.filter(pl.col("page_type") == pt)
                .sort("frequency", descending=True)
                .head(5)
            )
            print(f"\n  {pt}:")
            for row in pt_df.iter_rows(named=True):
                print(f"    {row['term']:20s} {row['frequency']}")
    return met_terms, pt, pt_df, row


@app.cell
def __(mo):
    mo.md(
        """
        ## Summary

        You have:
        - Compared raw NHS section JSON with its tokenised form — **Variety** in action
        - Computed term frequencies across all three page types for each drug
        - Identified clinically meaningful differences in terminology between drugs

        **Reflection question:** The term extractor uses only `re.findall(r'[a-z]{4,}', text)`.
        What clinically important terms might this miss?
        _(Hint: think about drug names, dosage units, and hyphenated terms like "long-term".)_

        **→ Next: [06_join_and_visualise.py](06_join_and_visualise.py) — join structured + unstructured, then visualise**
        """
    )
    return


if __name__ == "__main__":
    app.run()
