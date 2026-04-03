"""
tests/test_visualise.py — unit tests for pipeline/visualise.py.

Uses the Agg (headless) matplotlib backend so no display is required.
All tests create and close their own figures to avoid resource leaks.
"""

from __future__ import annotations

import pathlib

import matplotlib
import matplotlib.pyplot as plt
import polars as pl
import pytest

matplotlib.use("Agg")

from pipeline.visualise import (  # noqa: E402
    _DEFAULT_DPI,
    figure_to_bytes,
    plot_cost_per_item,
    plot_items_by_drug,
    plot_monthly_trend,
    plot_top_terms,
    report_filename,
    save_figure,
)

# ---------------------------------------------------------------------------
# Shared DataFrames
# ---------------------------------------------------------------------------

ITEMS_DF = pl.DataFrame(
    {"drug": ["metformin", "atorvastatin"], "total_items": [150_000, 90_000]}
)

COST_DF = pl.DataFrame(
    {"drug": ["metformin", "atorvastatin"], "avg_nic_per_item": [0.45, 1.20]}
)

TERMS_DF = pl.DataFrame(
    {
        "drug": ["metformin"] * 5 + ["atorvastatin"] * 3,
        "term": ["nausea", "vomiting", "diarrhoea", "lactic", "acidosis",
                 "muscle", "pain", "liver"],
        "frequency": [120, 80, 60, 40, 30, 90, 70, 50],
    }
)

TREND_DF = pl.DataFrame(
    {
        "drug": ["metformin", "metformin", "atorvastatin", "atorvastatin"],
        "year_month": ["2024-01", "2024-02", "2024-01", "2024-02"],
        "total_cost_gbp": [1000.0, 1100.0, 500.0, 550.0],
    }
)


# ---------------------------------------------------------------------------
# plot_items_by_drug
# ---------------------------------------------------------------------------


def test_plot_items_by_drug_sets_title():
    fig, ax = plt.subplots()
    plot_items_by_drug(ITEMS_DF, ax)
    assert "Items" in ax.get_title()
    plt.close(fig)


def test_plot_items_by_drug_creates_bars():
    fig, ax = plt.subplots()
    plot_items_by_drug(ITEMS_DF, ax)
    assert len(ax.patches) == 2
    plt.close(fig)


def test_plot_items_by_drug_xlabel():
    fig, ax = plt.subplots()
    plot_items_by_drug(ITEMS_DF, ax)
    assert ax.get_xlabel() == "Drug"
    plt.close(fig)


# ---------------------------------------------------------------------------
# plot_cost_per_item
# ---------------------------------------------------------------------------


def test_plot_cost_per_item_sets_title():
    fig, ax = plt.subplots()
    plot_cost_per_item(COST_DF, ax)
    assert "Cost" in ax.get_title()
    plt.close(fig)


def test_plot_cost_per_item_creates_bars():
    fig, ax = plt.subplots()
    plot_cost_per_item(COST_DF, ax)
    assert len(ax.patches) == 2
    plt.close(fig)


def test_plot_cost_per_item_ylabel():
    fig, ax = plt.subplots()
    plot_cost_per_item(COST_DF, ax)
    assert "NIC" in ax.get_ylabel() or "Cost" in ax.get_ylabel()
    plt.close(fig)


# ---------------------------------------------------------------------------
# plot_top_terms
# ---------------------------------------------------------------------------


def test_plot_top_terms_draws_bars():
    fig, ax = plt.subplots()
    plot_top_terms(TERMS_DF, "metformin", ax, n=5)
    assert len(ax.patches) > 0
    plt.close(fig)


def test_plot_top_terms_title_contains_drug():
    fig, ax = plt.subplots()
    plot_top_terms(TERMS_DF, "metformin", ax)
    assert "metformin" in ax.get_title().lower() or "Metformin" in ax.get_title()
    plt.close(fig)


def test_plot_top_terms_empty_data_no_bars():
    """When no rows match the drug, the chart should still render without error."""
    fig, ax = plt.subplots()
    plot_top_terms(TERMS_DF, "lisinopril", ax)
    assert len(ax.patches) == 0
    assert "no data" in ax.get_title().lower()
    plt.close(fig)


def test_plot_top_terms_respects_n():
    fig, ax = plt.subplots()
    plot_top_terms(TERMS_DF, "metformin", ax, n=3)
    assert len(ax.patches) <= 3
    plt.close(fig)


# ---------------------------------------------------------------------------
# plot_monthly_trend
# ---------------------------------------------------------------------------


def test_plot_monthly_trend_sets_title():
    fig, ax = plt.subplots()
    plot_monthly_trend(TREND_DF, ax)
    assert "Spend" in ax.get_title() or "Prescribing" in ax.get_title()
    plt.close(fig)


def test_plot_monthly_trend_draws_lines_for_each_drug():
    fig, ax = plt.subplots()
    plot_monthly_trend(TREND_DF, ax)
    assert len(ax.lines) == 2  # one per drug
    plt.close(fig)


def test_plot_monthly_trend_drug_filter():
    fig, ax = plt.subplots()
    plot_monthly_trend(TREND_DF, ax, drugs=["metformin"])
    assert len(ax.lines) == 1
    plt.close(fig)


def test_plot_monthly_trend_empty_drug_skipped():
    """Filtering to a drug not in the DataFrame should produce no lines."""
    fig, ax = plt.subplots()
    plot_monthly_trend(TREND_DF, ax, drugs=["lisinopril"])
    assert len(ax.lines) == 0
    plt.close(fig)


# ---------------------------------------------------------------------------
# figure_to_bytes
# ---------------------------------------------------------------------------


def test_figure_to_bytes_returns_bytes():
    fig, ax = plt.subplots()
    ax.plot([1, 2], [3, 4])
    result = figure_to_bytes(fig, fmt="png")
    plt.close(fig)
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_figure_to_bytes_png_signature():
    fig, ax = plt.subplots()
    result = figure_to_bytes(fig, fmt="png")
    plt.close(fig)
    # PNG files start with the 8-byte PNG signature
    assert result[:8] == b"\x89PNG\r\n\x1a\n"


def test_figure_to_bytes_pdf_signature():
    fig, ax = plt.subplots()
    result = figure_to_bytes(fig, fmt="pdf")
    plt.close(fig)
    assert result[:4] == b"%PDF"


# ---------------------------------------------------------------------------
# report_filename
# ---------------------------------------------------------------------------


def test_report_filename_default_prefix():
    fname = report_filename()
    assert fname.startswith("nhs_prescribing_report_")


def test_report_filename_custom_prefix():
    fname = report_filename("my_report", "png")
    assert fname.startswith("my_report_")
    assert fname.endswith(".png")


def test_report_filename_timestamp_format():
    fname = report_filename()
    # Strip prefix and extension: "nhs_prescribing_report_20260402_143022.pdf"
    parts = fname.rsplit(".", 1)
    assert parts[1] == "pdf"
    ts = parts[0].split("_", 3)[-1]  # "20260402_143022"
    assert len(ts) == 15  # YYYYMMDD_HHMMSS
    assert ts[8] == "_"


# ---------------------------------------------------------------------------
# save_figure
# ---------------------------------------------------------------------------


def test_save_figure_creates_file(tmp_path):
    fig, ax = plt.subplots()
    ax.plot([1, 2], [3, 4])
    out = tmp_path / "subdir" / "chart.png"
    save_figure(fig, out)
    plt.close(fig)
    assert out.exists()
    assert out.stat().st_size > 0


def test_save_figure_creates_parent_dirs(tmp_path):
    fig, ax = plt.subplots()
    out = tmp_path / "a" / "b" / "c" / "chart.png"
    save_figure(fig, out)
    plt.close(fig)
    assert out.exists()


def test_default_dpi_is_int():
    assert isinstance(_DEFAULT_DPI, int)
    assert _DEFAULT_DPI > 0
