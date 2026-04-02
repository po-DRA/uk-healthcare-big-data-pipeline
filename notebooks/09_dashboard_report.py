import marimo

__generated_with = "0.6.0"
app = marimo.App(title="09 · Dashboard & PDF Report")


@app.cell
def __():
    import marimo as mo

    return (mo,)


@app.cell
def __(mo):
    mo.md(
        """
        # Notebook 09 — Interactive Dashboard & PDF Report

        **Learning objective:** Build a reactive dashboard that reads directly from the
        Gold DuckDB tables, lets a clinician filter by drug and date range, and exports
        a datetime-stamped PDF (or PNG) report with a single button click.

        **Why this matters in practice**
        - Clinical governance boards need PDF evidence of which data cut was presented
        - A datetime-stamped filename is audit-trail proof of when the analysis ran
        - The Marimo download button captures the *current filter state* — so the PDF
          always reflects exactly what is on screen, unlike a scheduled static report

        **Estimated time:** 10 minutes

        > **Prerequisite:** Run notebooks 01–02 to populate `lake/`, then notebook 07
        > to build the Gold DuckDB tables.  Or run `flows/pipeline_flow.py` end-to-end.
        """
    )
    return


@app.cell
def __():
    import pathlib

    import duckdb
    import matplotlib.pyplot as plt
    import polars as pl

    from pipeline.visualise import (
        figure_to_bytes,
        plot_cost_per_item,
        plot_items_by_drug,
        plot_monthly_trend,
        plot_top_terms,
        report_filename,
    )

    return (
        duckdb,
        figure_to_bytes,
        pathlib,
        pl,
        plt,
        plot_cost_per_item,
        plot_items_by_drug,
        plot_monthly_trend,
        plot_top_terms,
        report_filename,
    )


@app.cell
def __(duckdb, mo, pathlib):
    """Check Gold tables exist; stop with a helpful message if not."""
    DB_PATH = pathlib.Path("pipeline.duckdb")

    _gold_ready = False
    if DB_PATH.exists():
        try:
            _con = duckdb.connect(str(DB_PATH), read_only=True)
            _count = _con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'gold'"
            ).fetchone()[0]
            _con.close()
            _gold_ready = _count >= 3
        except Exception:
            pass

    mo.stop(
        not _gold_ready,
        mo.callout(
            mo.md(
                """
                **Gold tables not found.**

                Run this in a Python cell or terminal first:

                ```python
                from pathlib import Path
                from pipeline.medallion import build_silver, build_gold
                build_silver(Path("lake"), Path("pipeline.duckdb"))
                build_gold(Path("pipeline.duckdb"))
                ```

                Or run the full Prefect pipeline:
                ```bash
                uv run python flows/pipeline_flow.py
                ```
                """
            ),
            kind="warn",
        ),
    )

    return (DB_PATH,)


@app.cell
def __(DB_PATH, duckdb):
    """Load available drugs and months from Gold — used to populate controls."""
    _con = duckdb.connect(str(DB_PATH), read_only=True)

    _drugs = [
        r[0]
        for r in _con.execute(
            "SELECT DISTINCT drug FROM gold.drug_summary ORDER BY drug"
        ).fetchall()
    ]

    _months = [
        r[0]
        for r in _con.execute(
            "SELECT DISTINCT year_month FROM gold.drug_monthly_spend "
            "ORDER BY year_month"
        ).fetchall()
    ]

    _con.close()
    available_drugs = _drugs
    available_months = _months
    return available_drugs, available_months


@app.cell
def __(available_drugs, mo):
    """Drug selector — multi-select, defaults to all available drugs."""
    drug_selector = mo.ui.multiselect(
        options=available_drugs,
        value=available_drugs,
        label="Drugs to include",
    )
    return (drug_selector,)


@app.cell
def __(available_months, mo):
    """Month range slider — controls how many recent months appear in the trend chart."""
    _max_months = len(available_months)
    _default = min(24, _max_months)

    months_slider = mo.ui.slider(
        start=3,
        stop=_max_months,
        step=3,
        value=_default,
        label="Months to show in trend chart",
        show_value=True,
    )
    return (months_slider,)


@app.cell
def __(drug_selector, mo, months_slider):
    """Display all controls together."""
    mo.vstack(
        [
            mo.md("## Dashboard Controls"),
            mo.hstack(
                [drug_selector, months_slider],
                justify="start",
                gap=2,
            ),
        ]
    )
    return


@app.cell
def __(DB_PATH, drug_selector, duckdb, months_slider, pl):
    """Query Gold tables with the current filter state.
    Re-runs automatically when drug_selector or months_slider changes.
    """
    _selected = drug_selector.value
    _n_months = months_slider.value

    if not _selected:
        df_summary = pl.DataFrame(
            schema={
                "drug": pl.String,
                "total_items": pl.Int64,
                "total_cost_gbp": pl.Float64,
                "avg_nic_per_item": pl.Float64,
            }
        )
        df_trend = pl.DataFrame(
            schema={
                "drug": pl.String,
                "year_month": pl.String,
                "total_cost_gbp": pl.Float64,
            }
        )
    else:
        _drug_list = ", ".join(f"'{d}'" for d in _selected)
        _con = duckdb.connect(str(DB_PATH), read_only=True)

        df_summary = pl.from_pandas(
            _con.execute(
                f"""
                SELECT drug, total_items, total_cost_gbp, avg_nic_per_item
                FROM gold.drug_summary
                WHERE drug IN ({_drug_list})
                ORDER BY total_items DESC
                """
            ).df()
        )

        df_trend = pl.from_pandas(
            _con.execute(
                f"""
                SELECT drug, year_month, total_cost_gbp
                FROM gold.drug_monthly_spend
                WHERE drug IN ({_drug_list})
                  AND year_month >= (
                      SELECT year_month FROM (
                          SELECT DISTINCT year_month
                          FROM gold.drug_monthly_spend
                          ORDER BY year_month DESC
                          LIMIT {_n_months}
                      ) ORDER BY year_month LIMIT 1
                  )
                ORDER BY drug, year_month
                """
            ).df()
        )

        _con.close()

    return df_summary, df_trend


@app.cell
def __(
    df_summary,
    df_trend,
    drug_selector,
    plt,
    plot_cost_per_item,
    plot_items_by_drug,
    plot_monthly_trend,
):
    """Render the 2×2 dashboard figure.
    Re-renders automatically when the filtered data changes.
    """
    _selected = drug_selector.value

    fig_dashboard, _axes = plt.subplots(2, 2, figsize=(14, 9))
    fig_dashboard.suptitle(
        "NHS Prescribing Dashboard — UK Open Data",
        fontsize=14,
        fontweight="bold",
        y=1.01,
    )

    if len(df_summary) > 0:
        plot_items_by_drug(df_summary, _axes[0, 0])
        plot_cost_per_item(df_summary, _axes[0, 1])
    else:
        _axes[0, 0].text(0.5, 0.5, "No drugs selected", ha="center", va="center")
        _axes[0, 1].text(0.5, 0.5, "No drugs selected", ha="center", va="center")

    if len(df_trend) > 0:
        plot_monthly_trend(df_trend, _axes[1, 0], drugs=_selected)
    else:
        _axes[1, 0].text(0.5, 0.5, "No trend data", ha="center", va="center")

    # Panel 4: summary table rendered as text
    _axes[1, 1].axis("off")
    if len(df_summary) > 0:
        _rows = [
            [
                r["drug"].capitalize(),
                f"{r['total_items']:,.0f}",
                f"£{r['total_cost_gbp']:,.0f}",
                f"£{r['avg_nic_per_item']:.2f}",
            ]
            for r in df_summary.iter_rows(named=True)
        ]
        _table = _axes[1, 1].table(
            cellText=_rows,
            colLabels=["Drug", "Items", "Total Cost", "Avg NIC/Item"],
            cellLoc="center",
            loc="center",
        )
        _table.auto_set_font_size(False)
        _table.set_fontsize(9)
        _table.scale(1, 1.4)
        _axes[1, 1].set_title("Summary Table", fontsize=12, fontweight="bold", pad=12)

    plt.tight_layout()
    fig_dashboard  # noqa: B018 — Marimo renders the bare expression as notebook output
    return (fig_dashboard,)


@app.cell
def __(fig_dashboard, figure_to_bytes, mo, report_filename):
    """
    Download buttons — PDF and PNG with datetime-stamped filenames.

    The timestamp is captured when this cell last ran (i.e. when the
    filter state last changed), so the filename accurately records
    when this particular view of the data was generated.

    This is the audit-trail feature: the filename alone tells a reviewer
    exactly which data cut was exported.
    """
    _pdf_bytes = figure_to_bytes(fig_dashboard, fmt="pdf")
    _png_bytes = figure_to_bytes(fig_dashboard, fmt="png")

    _pdf_name = report_filename("nhs_prescribing_report", "pdf")
    _png_name = report_filename("nhs_prescribing_report", "png")

    mo.vstack(
        [
            mo.md(
                f"""
                ## Export Report

                Click a button to download the current dashboard view.
                The filename includes the exact date and time this view was generated
                — required for NHS governance and audit trails.

                | Format | Filename | Use case |
                |--------|----------|----------|
                | PDF | `{_pdf_name}` | Board papers, governance submissions |
                | PNG | `{_png_name}` | Slide decks, email attachments |
                """
            ),
            mo.hstack(
                [
                    mo.download(
                        data=_pdf_bytes,
                        filename=_pdf_name,
                        mimetype="application/pdf",
                    ),
                    mo.download(
                        data=_png_bytes,
                        filename=_png_name,
                        mimetype="image/png",
                    ),
                ],
                justify="start",
                gap=1,
            ),
        ]
    )
    return


@app.cell
def __(DB_PATH, drug_selector, duckdb, mo, pl):
    """Practice leaderboard — top 10 for the first selected drug."""
    _selected = drug_selector.value
    if not _selected:
        mo.stop(True, mo.md("Select at least one drug to see the leaderboard."))

    _drug = _selected[0]
    _con = duckdb.connect(str(DB_PATH), read_only=True)
    df_leaderboard = pl.from_pandas(
        _con.execute(
            """
            SELECT practice_id, total_items, total_cost_gbp, rank
            FROM gold.practice_leaderboard
            WHERE drug = ?
              AND rank <= 10
            ORDER BY rank
            """,
            [_drug],
        ).df()
    )
    _con.close()

    mo.vstack(
        [
            mo.md(f"## Top 10 Practices — {_drug.capitalize()}"),
            mo.md(
                "_Practices with the highest total items prescribed. "
                "High-volume prescribers often serve larger or older patient populations._"
            ),
            mo.ui.table(
                df_leaderboard.rename(
                    {
                        "practice_id": "Practice ID",
                        "total_items": "Total Items",
                        "total_cost_gbp": "Total Cost (£)",
                        "rank": "Rank",
                    }
                ).to_pandas()
            ),
        ]
    )
    return (df_leaderboard,)


@app.cell
def __(mo):
    mo.md(
        """
        ## Summary

        You have built a reactive clinical dashboard that:

        1. **Reads directly from Gold DuckDB tables** — no data is re-processed;
           the dashboard is instant because Gold is pre-aggregated
        2. **Reacts to filter changes** — Marimo re-runs only the cells that
           depend on the changed control (drug selector or month slider)
        3. **Exports a datetime-stamped PDF and PNG** — the filename is audit-trail
           evidence of when the analysis was generated
        4. **Degrades gracefully** — `mo.stop()` shows a clear message if the
           Gold tables haven't been built yet

        ### Why this matters in healthcare

        A PDF with a timestamp in the filename is not just a convenience — in NHS
        governance it is evidence. A board paper submitted with `nhs_prescribing_report_20260402_143022.pdf`
        tells any future reviewer:
        - *which* data was presented (Gold layer snapshot)
        - *when* it was generated (2 April 2026 at 14:30:22)
        - *that* it was produced by a reproducible, tested pipeline

        Compare this to a screenshot of a spreadsheet: no timestamp, no provenance,
        no way to reproduce it.

        **Reflection question:** The download timestamp is captured when the filter
        state last changed, not when the user clicks the button. When might this
        distinction matter for a clinical audit? How would you capture a
        "click-time" timestamp instead?

        ---

        **→ You have completed the full course.**
        Run the Prefect pipeline end-to-end to see all steps orchestrated:

        ```bash
        uv run prefect server start   # Terminal 1
        uv run python flows/pipeline_flow.py  # Terminal 2
        ```

        Then open **http://localhost:4200** to see the DAG.
        """
    )
    return


if __name__ == "__main__":
    app.run()
