"""
pipeline/fetch.py — HTTP data acquisition layer.

Fetches data from two UK open data sources:
  1. NHSBSA English Prescribing Dataset (EPD) — structured monthly prescribing records
  2. NHS.uk medicines pages — unstructured clinical HTML prose

NHSBSA EPD
----------
The EPD is the primary NHS prescribing dataset published by the NHS Business
Services Authority.  Each monthly file contains ~18 million rows covering every
prescription dispensed in England.

The file is published as a 6-7 GB CSV.  Rather than downloading the whole file,
``fetch_nhsbsa`` streams it in 512 KB chunks and stops as soon as it has
collected enough rows for each drug (early-exit streaming).  Fetching 4 drugs
at 500 rows each typically reads ~50 MB and takes under 10 seconds.

API approach
~~~~~~~~~~~~
NHSBSA publishes files via a CKAN-compatible API:

- ``package_show`` → lists all monthly resource IDs and download URLs
- Direct CSV URL  → streamed row-by-row; ``BNF_CHEMICAL_SUBSTANCE`` is used
  as the filter key

The CKAN ``datastore_search`` filter endpoint returns HTTP 500 consistently
from cloud IPs, so we bypass it entirely and filter client-side while streaming.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import urllib.request

import httpx
from bs4 import BeautifulSoup
from pydantic import ValidationError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from pipeline.contracts import NHSBSAPayload, NHSPagesPayload

_log = logging.getLogger(__name__)

_DRUG_NAME_RE = re.compile(r"^[a-z][a-z\-]*[a-z]$")

_RETRY = retry(
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError, OSError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=2, max=10, jitter=1),
    reraise=True,
)

_HTTP_TIMEOUT: float = float(os.environ.get("HTTP_TIMEOUT", "20"))
_STREAM_TIMEOUT: float = float(os.environ.get("STREAM_TIMEOUT", "120"))

# BNF chemical substance codes (field BNF_CHEMICAL_SUBSTANCE in EPD)
DRUG_CODES: dict[str, str] = {
    "metformin":    "0601022B0",
    "atorvastatin": "0212000B0",
    "lisinopril":   "0205051L0",
    "salbutamol":   "0301011R0",
}

_NHSBSA_PACKAGE_URL = (
    "https://opendata.nhsbsa.net/api/3/action/package_show"
    "?id=english-prescribing-data-epd"
)
_NHS_BASE = "https://www.nhs.uk/medicines/{drug}/{slug}/"
_NHS_SLUGS = [
    "side-effects-of-{drug}",
    "who-can-and-cannot-take-{drug}",
    "taking-{drug}-with-other-medicines-and-herbal-supplements",
]

#: Rows to collect per drug before stopping the stream.
#: 500 rows per drug gives meaningful ICB-level variation while keeping
#: fetch time under 15 seconds.  Override via environment variable:
#:   NHSBSA_ROWS_PER_DRUG=1000 uv run python scripts/01_fetch.py
ROWS_PER_DRUG: int = int(os.environ.get("NHSBSA_ROWS_PER_DRUG", "500"))


# ---------------------------------------------------------------------------
# NHSBSA EPD — structured prescribing data
# ---------------------------------------------------------------------------


def _get_latest_epd_url() -> tuple[str, str]:
    """Return (csv_url, resource_name) for the most recent EPD monthly file."""
    with urllib.request.urlopen(_NHSBSA_PACKAGE_URL, timeout=20) as r:
        d = json.load(r)
    resources = [
        res for res in d["result"]["resources"]
        if res["name"].startswith("EPD_2")
    ]
    latest = sorted(resources, key=lambda x: x["name"])[-1]
    return latest["url"], latest["name"]


@_RETRY
def fetch_nhsbsa(drug_bnf_code: str, drug_name: str) -> dict:
    """Fetch monthly NHS prescribing records for one drug from NHSBSA EPD.

    Streams the latest monthly EPD CSV file and collects rows matching
    ``drug_bnf_code``.  Stops early once ``ROWS_PER_DRUG`` rows are collected —
    no need to read the full 6-7 GB file.

    Demonstrates **Volume** (18 million rows per monthly file) and
    **Velocity** (monthly refresh cycle from NHS BSA).

    Parameters
    ----------
    drug_bnf_code:
        BNF chemical substance code, e.g. ``"0601022B0"`` for metformin.
    drug_name:
        Human-readable drug name used as a label in the returned payload.

    Returns
    -------
    dict with keys:
        - ``drug``       : str
        - ``bnf_code``   : str
        - ``source``     : str — always ``"nhsbsa_epd"``
        - ``resource``   : str — e.g. ``"EPD_202506"``
        - ``total_rows`` : int — rows collected
        - ``records``    : list[dict] — date, actual_cost, items, quantity,
                           row_id, setting, ccg, icb_name, drug
    """
    csv_url, resource_name = _get_latest_epd_url()
    _log.info(
        "Streaming NHSBSA %s for %s (%s), target %d rows",
        resource_name, drug_name, drug_bnf_code, ROWS_PER_DRUG,
    )

    records: list[dict] = []
    buf = ""
    header: list[str] | None = None
    bytes_read = 0

    req = urllib.request.Request(
        csv_url,
        headers={
            "User-Agent": (
                "uk-healthcare-pipeline/1.0 "
                "(+https://github.com/po-DRA/uk-healthcare-big-data-pipeline)"
            )
        },
    )

    with urllib.request.urlopen(req, timeout=_STREAM_TIMEOUT) as r:
        while len(records) < ROWS_PER_DRUG:
            chunk = r.read(512 * 1024)
            if not chunk:
                break
            bytes_read += len(chunk)
            buf += chunk.decode("utf-8", errors="replace")
            lines = buf.split("\n")
            buf = lines[-1]

            for line in lines[:-1]:
                if not line.strip():
                    continue
                if header is None:
                    header = next(csv.reader([line.strip()]))
                    continue

                # Fast pre-filter before full CSV parse
                if drug_bnf_code not in line:
                    continue

                try:
                    row = next(csv.DictReader([line], fieldnames=header))
                except Exception:
                    continue

                if row.get("BNF_CHEMICAL_SUBSTANCE") != drug_bnf_code:
                    continue
                if row.get("UNIDENTIFIED", "").lower() == "true":
                    continue

                records.append({
                    "date":        row.get("YEAR_MONTH", ""),
                    "actual_cost": _to_float(row.get("ACTUAL_COST")),
                    "items":       _to_int(row.get("ITEMS")),
                    "quantity":    _to_float(row.get("QUANTITY")),
                    "row_id":      row.get("PRACTICE_CODE", ""),
                    "setting":     "4",
                    "ccg":         row.get("ICB_CODE", ""),
                    "icb_name":    row.get("ICB_NAME", ""),
                    "drug":        drug_name,
                })

                if len(records) >= ROWS_PER_DRUG:
                    break

    _log.info(
        "%s: %d rows collected from %s (%.1f MB streamed)",
        drug_name, len(records), resource_name, bytes_read / 1e6,
    )

    payload = {
        "drug":       drug_name,
        "bnf_code":   drug_bnf_code,
        "source":     "nhsbsa_epd",
        "resource":   resource_name,
        "total_rows": len(records),
        "records":    records,
    }
    try:
        return NHSBSAPayload.model_validate(payload).model_dump()
    except ValidationError as exc:
        raise ValueError(
            f"fetch_nhsbsa contract violation for {drug_name!r}: {exc}"
        ) from exc


def _to_float(val) -> float | None:
    try:
        return float(val) if val not in (None, "", "NULL") else None
    except (ValueError, TypeError):
        return None


def _to_int(val) -> int | None:
    try:
        return int(float(val)) if val not in (None, "", "NULL") else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# NHS.uk — unstructured clinical prose
# ---------------------------------------------------------------------------


@_RETRY
def fetch_nhs_pages(drug_name: str) -> dict:
    """Fetch and parse the three NHS.uk clinical sub-pages for one drug.

    Demonstrates **Variety** — completely different data structure from the
    NHSBSA API: raw HTML parsed into structured sections with headings,
    paragraphs, and bullet points.

    Parameters
    ----------
    drug_name:
        Lower-case drug name matching the NHS.uk URL slug,
        e.g. ``"metformin"``.

    Returns
    -------
    dict with keys:
        - ``drug``  : str
        - ``type``  : str — always ``"nhs_pages"``
        - ``pages`` : list[dict], each containing:
            url, page_type, heading, text (str), bullets (list[str])
    """
    if not _DRUG_NAME_RE.match(drug_name):
        raise ValueError(
            f"Invalid drug_name {drug_name!r}: must contain only lowercase letters "
            "and hyphens (e.g. 'metformin', 'co-amoxiclav')"
        )

    pages: list[dict] = []
    _log.info("Fetching NHS.uk pages: %s", drug_name)

    for slug_template in _NHS_SLUGS:
        slug = slug_template.format(drug=drug_name)
        url = _NHS_BASE.format(drug=drug_name, slug=slug)

        if "side-effects" in slug:
            page_type = "side_effects"
        elif "who-can" in slug:
            page_type = "contraindications"
        else:
            page_type = "interactions"

        try:
            response = httpx.get(url, timeout=_HTTP_TIMEOUT, follow_redirects=True)
            if response.status_code == 404:
                _log.warning("404 for %s — skipping", url)
                continue
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            _log.warning("HTTP %d for %s — skipping", exc.response.status_code, url)
            continue
        except httpx.RequestError as exc:
            _log.warning("Request error for %s: %s — skipping", url, exc)
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        main = soup.find("main") or soup

        headings = main.find_all(["h2", "h3"])  # type: ignore[union-attr]
        if not headings:
            text = " ".join(p.get_text(" ", strip=True) for p in main.find_all("p"))  # type: ignore[union-attr]
            bullets = [li.get_text(" ", strip=True) for li in main.find_all("li")]  # type: ignore[union-attr]
            pages.append({
                "url": url, "page_type": page_type,
                "heading": "", "text": text, "bullets": bullets,
            })
        else:
            for heading in headings:
                heading_text = heading.get_text(" ", strip=True)
                sibling_texts: list[str] = []
                sibling_bullets: list[str] = []
                for sibling in heading.find_next_siblings():
                    if sibling.name in ("h2", "h3"):
                        break
                    if sibling.name == "p":
                        sibling_texts.append(sibling.get_text(" ", strip=True))
                    elif sibling.name in ("ul", "ol"):
                        sibling_bullets.extend(
                            li.get_text(" ", strip=True)
                            for li in sibling.find_all("li")
                        )
                pages.append({
                    "url": url, "page_type": page_type,
                    "heading": heading_text,
                    "text": " ".join(sibling_texts),
                    "bullets": sibling_bullets,
                })

    _log.info("%s: %d sections extracted across NHS pages", drug_name, len(pages))
    payload = {"drug": drug_name, "type": "nhs_pages", "pages": pages}
    try:
        return NHSPagesPayload.model_validate(payload).model_dump()
    except ValidationError as exc:
        raise ValueError(
            f"fetch_nhs_pages contract violation for {drug_name!r}: {exc}"
        ) from exc
