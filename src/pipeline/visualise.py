"""
pipeline/visualise.py — matplotlib chart functions.

Called by notebook 06 to produce the final 2×2 clinical insight figure.
All functions accept a pre-created matplotlib Axes object so they can be
composed into any subplot layout the notebook requires.
"""

from __future__ import annotations

import datetime
import io
import logging
import pathlib

import matplotlib
import matplotlib.pyplot as plt
import polars as pl

matplotlib.use("Agg")  # headless backend — works in Codespaces and CI

_log = logging.getLogger(__name__)

# NHS brand colours used consistently across all charts
_NHS_COLOURS = ["#005EB8", "#41B6E6", "#006747", "#AE2573"]

# Default export resolution.  150 dpi is screen quality; use 300 for print.
_DEFAULT_DPI = 150


def plot_items_by_drug(df: pl.DataFrame, ax: plt.Axes) -> None:
    """Bar chart of total items prescribed per drug.

    Demonstrates **Volume** — the sheer scale of NHS prescribing activity
    across England makes these bars visually striking.

    Parameters
    ----------
    df:
        DataFrame with columns ``drug`` (String) and ``total_items`` (numeric).
    ax:
        Matplotlib Axes to draw on.
    """
    drugs = df["drug"].to_list()
    items = df["total_items"].to_list()

    bars = ax.bar(drugs, items, color=_NHS_COLOURS[: len(drugs)])
    ax.set_title("Total Items Prescribed by Drug", fontsize=12, fontweight="bold")
    ax.set_xlabel("Drug")
    ax.set_ylabel("Total Items")
    ax.tick_params(axis="x", rotation=15)

    for bar, value in zip(bars, items, strict=False):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() * 1.01,
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8,
        )


def plot_cost_per_item(df: pl.DataFrame, ax: plt.Axes) -> None:
    """Bar chart of average NIC (net ingredient cost) per item by drug.

    Demonstrates **Veracity** — cost-per-item varies meaningfully between
    drugs and can reveal pricing anomalies or data entry errors.

    Parameters
    ----------
    df:
        DataFrame with columns ``drug`` (String) and ``avg_nic_per_item`` (numeric).
    ax:
        Matplotlib Axes to draw on.
    """
    drugs = df["drug"].to_list()
    costs = df["avg_nic_per_item"].to_list()

    bars = ax.bar(drugs, costs, color=_NHS_COLOURS[: len(drugs)])
    ax.set_title("Average Cost per Item (£) by Drug", fontsize=12, fontweight="bold")
    ax.set_xlabel("Drug")
    ax.set_ylabel("Avg NIC per Item (£)")
    ax.tick_params(axis="x", rotation=15)

    for bar, value in zip(bars, costs, strict=False):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() * 1.01,
            f"£{value:.2f}",
            ha="center",
            va="bottom",
            fontsize=8,
        )


def plot_top_terms(df: pl.DataFrame, drug: str, ax: plt.Axes, n: int = 8) -> None:
    """Horizontal bar chart of top NLP terms for a single drug.

    Demonstrates **Variety** — language patterns from unstructured clinical
    text are visualised alongside structured prescribing metrics.

    Parameters
    ----------
    df:
        DataFrame with columns ``drug``, ``term``, ``frequency``.
    drug:
        Drug name to filter and label the chart.
    ax:
        Matplotlib Axes to draw on.
    n:
        Number of top terms to display.
    """
    drug_df = (
        df.filter(pl.col("drug") == drug).sort("frequency", descending=True).head(n)
    )

    if len(drug_df) == 0:
        ax.set_title(f"Top terms: {drug} (no data)", fontsize=12)
        return

    terms = drug_df["term"].to_list()
    freqs = drug_df["frequency"].to_list()

    ax.barh(terms[::-1], freqs[::-1], color="#005EB8")
    ax.set_title(
        f"Top {n} Clinical Terms — {drug.capitalize()}",
        fontsize=12,
        fontweight="bold",
    )
    ax.set_xlabel("Term Frequency")
    ax.set_ylabel("Term")


def plot_monthly_trend(
    df: pl.DataFrame,
    ax: plt.Axes,
    drugs: list[str] | None = None,
) -> None:
    """Line chart of monthly NHS prescribing spend per drug.

    Demonstrates **Velocity** — shows how prescribing spend evolves month
    by month, making seasonal patterns and step-changes visible.

    Parameters
    ----------
    df:
        DataFrame with columns ``drug`` (String), ``year_month`` (String),
        ``total_cost_gbp`` (numeric).  Typically from ``gold.drug_monthly_spend``.
    ax:
        Matplotlib Axes to draw on.
    drugs:
        Subset of drugs to plot.  ``None`` plots all drugs in the DataFrame.
    """
    plot_drugs = sorted(drugs) if drugs else sorted(df["drug"].unique().to_list())

    for i, drug in enumerate(plot_drugs):
        drug_df = df.filter(pl.col("drug") == drug).sort("year_month")
        if len(drug_df) == 0:
            continue
        colour = _NHS_COLOURS[i % len(_NHS_COLOURS)]
        ax.plot(
            drug_df["year_month"].to_list(),
            drug_df["total_cost_gbp"].to_list(),
            marker="o",
            markersize=3,
            linewidth=1.5,
            label=drug.capitalize(),
            color=colour,
        )

    ax.set_title("Monthly Prescribing Spend (£)", fontsize=12, fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Total Cost (£)")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(fontsize=8)


def figure_to_bytes(fig: plt.Figure, fmt: str = "pdf") -> bytes:
    """Export a matplotlib figure to an in-memory bytes buffer.

    Used by the dashboard notebook to generate PDF and PNG downloads
    without writing a temporary file to disk.

    Parameters
    ----------
    fig:
        The figure to serialise.
    fmt:
        Output format — ``"pdf"`` or ``"png"``.  Passed directly to
        ``fig.savefig(format=fmt, ...)``.

    Returns
    -------
    bytes
        Raw bytes of the rendered figure.
    """
    buf = io.BytesIO()
    fig.savefig(buf, format=fmt, bbox_inches="tight", dpi=_DEFAULT_DPI)
    buf.seek(0)
    return buf.read()


def report_filename(
    prefix: str = "nhs_prescribing_report",
    ext: str = "pdf",
) -> str:
    """Generate a datetime-stamped report filename.

    The timestamp is captured at call time, so the filename reflects
    when the report was generated — important for NHS audit trails.

    Parameters
    ----------
    prefix:
        Human-readable prefix, e.g. ``"nhs_prescribing_report"``.
    ext:
        File extension without leading dot, e.g. ``"pdf"`` or ``"png"``.

    Returns
    -------
    str
        Filename like ``"nhs_prescribing_report_20260402_143022.pdf"``.

    Example
    -------
    >>> fname = report_filename("metformin_dashboard", "png")
    >>> fname.startswith("metformin_dashboard_")
    True
    """
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.{ext}"


def save_figure(fig: plt.Figure, output_path: pathlib.Path) -> None:
    """Save a matplotlib figure to disk, creating parent directories as needed.

    Parameters
    ----------
    fig:
        The figure to save.
    output_path:
        Destination path (e.g. ``pathlib.Path("outputs/clinical_insight.png")``).
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=_DEFAULT_DPI, bbox_inches="tight")
    _log.info("Figure saved → %s", output_path)
