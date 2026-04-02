import marimo

__generated_with = "0.6.0"
app = marimo.App(title="06 · Join & Visualise — All 4 V's")


@app.cell
def __():
    import marimo as mo

    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 06 — Join & Visualise

        **Learning objective:** Join the structured prescribing data with the unstructured
        NLP term data using DuckDB SQL, export to Parquet, and produce a final
        clinical insight visualisation with matplotlib.

        **V's demonstrated:** All four (final synthesis)

        **Estimated time:** 10 minutes

        > **Prerequisite:** Run notebooks 02 and 05 first.
        """
    )
    return


@app.cell
def __():
    import json
    import pathlib

    import duckdb
    import matplotlib.pyplot as plt
    import polars as pl

    from pipeline.nlp import top_terms
    from pipeline.visualise import (
        plot_cost_per_item,
        plot_items_by_drug,
        plot_top_terms,
        save_figure,
    )

    return (
        duckdb,
        json,
        pathlib,
        pl,
        plt,
        plot_cost_per_item,
        plot_items_by_drug,
        plot_top_terms,
        save_figure,
        top_terms,
    )


@app.cell
def __(pathlib):
    LAKE_DIR = pathlib.Path("lake")
    DRUGS = ["metformin", "atorvastatin", "lisinopril", "amlodipine"]
    return DRUGS, LAKE_DIR


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 1 — Write NLP Terms to the Lake

        Export all drug term DataFrames to a single JSONL file so DuckDB
        can JOIN them against the prescribing view.
        """
    )
    return


@app.cell
def __(DRUGS, LAKE_DIR, top_terms):
    import polars as pl

    all_terms_frames = []
    for drug in DRUGS:
        try:
            df = top_terms(drug, LAKE_DIR, n=20)
            all_terms_frames.append(df)
        except FileNotFoundError:
            print(f"  Skipping {drug} — lake file not found")

    if all_terms_frames:
        combined_terms = pl.concat(all_terms_frames)
        nlp_path = LAKE_DIR / "nlp_terms.jsonl"
        with nlp_path.open("w", encoding="utf-8") as fh:
            for row in combined_terms.iter_rows(named=True):
                import json

                fh.write(json.dumps(row) + "\n")
        print(f"NLP terms written: {len(combined_terms):,} rows → {nlp_path}")
    else:
        print("No NLP terms to write — check that lake/*/nhs_pages.json files exist")
        combined_terms = pl.DataFrame(
            schema={
                "drug": pl.String,
                "term": pl.String,
                "frequency": pl.Int64,
                "page_type": pl.String,
            }
        )
    return all_terms_frames, combined_terms, df, drug, fh, json, nlp_path, pl, row


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 2 — DuckDB JOIN: Structured ⋈ Unstructured

        Join prescribing aggregates with top side-effect terms per drug.
        This is the key analytical result — linking clinical text signals
        with prescribing volume and cost.
        """
    )
    return


@app.cell
def __(duckdb):
    con = duckdb.connect("pipeline.duckdb")

    # Ensure the prescriptions view still exists
    con.execute(
        """
        CREATE OR REPLACE VIEW prescriptions AS
        SELECT *
        FROM read_json(
            'lake/*/prescribing.jsonl',
            format = 'newline_delimited',
            auto_detect = true
        )
        """
    )

    # Create a view over the NLP JSONL
    con.execute(
        """
        CREATE OR REPLACE VIEW nlp_terms AS
        SELECT *
        FROM read_json(
            'lake/nlp_terms.jsonl',
            format = 'newline_delimited',
            auto_detect = true
        )
        """
    )
    print("Views created: prescriptions, nlp_terms")
    return (con,)


@app.cell
def __(con):
    # JOIN: prescribing aggregates with top 5 side-effect terms per drug
    join_result = con.execute(
        """
        -- Clinical insight: prescribing metrics joined with NLP side-effect terms
        WITH prescribing_agg AS (
            SELECT
                drug,
                SUM(items)                                              AS total_items,
                ROUND(SUM(actual_cost), 2)                             AS total_cost_gbp,
                ROUND(SUM(actual_cost) / NULLIF(SUM(items), 0), 4)    AS avg_nic_per_item
            FROM prescriptions
            GROUP BY drug
        ),
        side_effect_terms AS (
            SELECT
                drug,
                STRING_AGG(term, ', ' ORDER BY frequency DESC)         AS top_5_terms
            FROM (
                SELECT drug, term, frequency,
                       ROW_NUMBER() OVER (PARTITION BY drug ORDER BY frequency DESC) AS rn
                FROM nlp_terms
                WHERE page_type = 'side_effects'
            )
            WHERE rn <= 5
            GROUP BY drug
        )
        SELECT
            p.drug,
            p.total_items,
            p.total_cost_gbp,
            p.avg_nic_per_item,
            COALESCE(s.top_5_terms, 'N/A')                            AS top_5_side_effect_terms
        FROM prescribing_agg p
        LEFT JOIN side_effect_terms s USING (drug)
        ORDER BY p.total_items DESC
        """
    ).df()

    print("Clinical insight — prescribing metrics + NLP side-effect terms:")
    print(join_result.to_string(index=False))
    return (join_result,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 3 — Export to Parquet

        Save the joined result as a Parquet file for downstream use.
        Parquet is columnar, compressed, and fast to read with DuckDB or Polars.
        """
    )
    return


@app.cell
def __(con, pathlib):
    pathlib.Path("lake").mkdir(exist_ok=True)
    con.execute(
        """
        COPY (
            SELECT *
            FROM (
                WITH prescribing_agg AS (
                    SELECT drug,
                           SUM(items)                                           AS total_items,
                           ROUND(SUM(actual_cost), 2)                          AS total_cost_gbp,
                           ROUND(SUM(actual_cost) / NULLIF(SUM(items),0), 4)  AS avg_nic_per_item
                    FROM prescriptions GROUP BY drug
                ),
                side_effect_terms AS (
                    SELECT drug,
                           STRING_AGG(term, ', ' ORDER BY frequency DESC) AS top_5_terms
                    FROM (
                        SELECT drug, term, frequency,
                               ROW_NUMBER() OVER (PARTITION BY drug ORDER BY frequency DESC) AS rn
                        FROM nlp_terms WHERE page_type = 'side_effects'
                    ) WHERE rn <= 5
                    GROUP BY drug
                )
                SELECT p.*, COALESCE(s.top_5_terms,'N/A') AS top_5_side_effect_terms
                FROM prescribing_agg p LEFT JOIN side_effect_terms s USING (drug)
            )
        ) TO 'lake/clinical_insight.parquet' (FORMAT PARQUET)
        """
    )
    parquet_size = pathlib.Path("lake/clinical_insight.parquet").stat().st_size
    print(f"Parquet exported: lake/clinical_insight.parquet ({parquet_size:,} bytes)")
    return (parquet_size,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 4 — Prepare Data for Plotting

        Convert the DuckDB result to Polars for use in the visualise functions.
        """
    )
    return


@app.cell
def __(con, pl):
    plot_df = pl.from_pandas(
        con.execute(
            """
            SELECT drug,
                   SUM(items)                                          AS total_items,
                   ROUND(SUM(actual_cost)/NULLIF(SUM(items),0), 4)   AS avg_nic_per_item
            FROM prescriptions
            GROUP BY drug
            ORDER BY total_items DESC
            """
        ).df()
    )
    print("Data ready for plotting:")
    print(plot_df)
    return (plot_df,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Step 5 — 2×2 Clinical Insight Chart

        Four panels combining structured prescribing data with unstructured NLP signals.
        """
    )
    return


@app.cell
def __(
    combined_terms,
    pathlib,
    plot_cost_per_item,
    plot_df,
    plot_items_by_drug,
    plot_top_terms,
    plt,
    save_figure,
):
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(
        "NHS Prescribing & Clinical Text Analysis — UK Open Data",
        fontsize=14,
        fontweight="bold",
        y=1.01,
    )

    # Panel 1: total items by drug
    plot_items_by_drug(plot_df, axes[0, 0])

    # Panel 2: average cost per item by drug
    plot_cost_per_item(plot_df, axes[0, 1])

    # Panel 3: top 8 side-effect terms for metformin
    plot_top_terms(combined_terms, "metformin", axes[1, 0], n=8)

    # Panel 4: top 8 side-effect terms for atorvastatin
    plot_top_terms(combined_terms, "atorvastatin", axes[1, 1], n=8)

    plt.tight_layout()

    output_path = pathlib.Path("outputs") / "clinical_insight.png"
    save_figure(fig, output_path)

    fig  # noqa: B018 — Marimo renders the bare expression as notebook output
    return axes, fig, output_path


@app.cell
def __(mo, output_path):
    mo.md(
        f"""
        ## Summary

        You have completed the full pipeline:

        1. **Fetch** — parallel HTTP requests to two UK open data sources (Velocity)
        2. **Lake** — JSONL + JSON in the same directory tree (Variety)
        3. **SQL** — DuckDB queries directly on raw files (Volume)
        4. **Transform** — Polars lazy evaluation + veracity report (Volume + Veracity)
        5. **NLP** — regex tokenisation on clinical prose (Variety)
        6. **Join** — DuckDB JOIN structured + unstructured (all 4 V's)
        7. **Visualise** — matplotlib 2×2 clinical insight chart

        Chart saved to: `{output_path}`

        **Reflection question:** The NLP top-terms are computed per drug independently.
        How would you modify the pipeline to find terms that appear in one drug's pages
        but _not_ in any other drug's pages? What clinical insight might that reveal?

        ---

        ## Next Steps

        Now that you understand the pipeline, try the Prefect orchestration layer:

        ```bash
        # Terminal 1:
        uv run prefect server start

        # Terminal 2:
        uv run python flows/pipeline_flow.py
        ```

        Watch the full pipeline run with retries, structured logs, and a visual DAG
        at http://localhost:4200.
        """
    )
    return


if __name__ == "__main__":
    app.run()
