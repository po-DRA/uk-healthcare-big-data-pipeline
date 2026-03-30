"""
pipeline/visualise.py — matplotlib chart functions.

Called by notebook 06 to produce the final 2×2 clinical insight figure.
All functions accept a pre-created matplotlib Axes object so they can be
composed into any subplot layout the notebook requires.
"""

from __future__ import annotations

import pathlib

import matplotlib
import matplotlib.pyplot as plt
import polars as pl

matplotlib.use("Agg")  # headless backend — works in Codespaces and CI


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

    bars = ax.bar(drugs, items, color=["#005EB8", "#41B6E6", "#006747", "#AE2573"])
    ax.set_title("Total Items Prescribed by Drug", fontsize=12, fontweight="bold")
    ax.set_xlabel("Drug")
    ax.set_ylabel("Total Items")
    ax.tick_params(axis="x", rotation=15)

    for bar, value in zip(bars, items):
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

    bars = ax.bar(drugs, costs, color=["#005EB8", "#41B6E6", "#006747", "#AE2573"])
    ax.set_title("Average Cost per Item (£) by Drug", fontsize=12, fontweight="bold")
    ax.set_xlabel("Drug")
    ax.set_ylabel("Avg NIC per Item (£)")
    ax.tick_params(axis="x", rotation=15)

    for bar, value in zip(bars, costs):
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
        df.filter(pl.col("drug") == drug)
        .sort("frequency", descending=True)
        .head(n)
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
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"  ✓ Figure saved → {output_path}")
