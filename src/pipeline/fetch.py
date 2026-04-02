"""
pipeline/fetch.py — HTTP data acquisition layer.

Fetches data from two UK open data sources:
  1. OpenPrescribing API  — structured NHS prescribing records (Volume + Velocity)
  2. NHS.uk medicines pages — unstructured clinical HTML prose    (Variety)

All HTTP calls use httpx with a 20-second timeout.
No API keys required for either source.
"""

from __future__ import annotations

import logging
import os
import re

import httpx
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

_log = logging.getLogger(__name__)

# Allowed characters in a drug name: lowercase letters and hyphens only.
# Prevents path traversal and injection if drug_name is ever used in a URL or path.
_DRUG_NAME_RE = re.compile(r"^[a-z][a-z\-]*[a-z]$")

# Retry strategy: up to 3 attempts, exponential backoff (2s → 4s → 8s) plus
# ±1s jitter to avoid thundering-herd if multiple drugs fail simultaneously.
# Retries on transient network errors and 5xx HTTP responses.
_RETRY = retry(
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=2, max=10, jitter=1),
    reraise=True,
)

# ---------------------------------------------------------------------------
# BNF drug codes used throughout the pipeline
# ---------------------------------------------------------------------------
DRUG_CODES: dict[str, str] = {
    "metformin": "0601023A0",
    "atorvastatin": "0212000B0",
    "lisinopril": "0205051R0",
    "amlodipine": "0206020A0",
}

# NHS.uk URL slugs for the three clinical sub-pages per drug
_NHS_SLUGS = [
    "side-effects-of-{drug}",
    "who-can-and-cannot-take-{drug}",
    "taking-{drug}-with-other-medicines-and-herbal-supplements",
]

_OPENPRESCRIBING_BASE = (
    "https://openprescribing.net/api/1.0/spending_by_org/"
    "?org_type=practice&code={bnf_code}"
)
_NHS_BASE = "https://www.nhs.uk/medicines/{drug}/{slug}/"

# Seconds before an HTTP request is abandoned.  20s is generous but appropriate
# for NHS.uk which can be slow to respond.  Override via environment variable:
#   HTTP_TIMEOUT=30 uv run python flows/pipeline_flow.py
_HTTP_TIMEOUT: float = float(os.environ.get("HTTP_TIMEOUT", "20"))


# ---------------------------------------------------------------------------
# OpenPrescribing — structured prescribing data
# ---------------------------------------------------------------------------


@_RETRY
def fetch_openprescribing(drug_bnf_code: str, drug_name: str) -> dict:
    """Fetch monthly NHS prescribing records for one drug from OpenPrescribing.

    Demonstrates **Volume** (hundreds of thousands of practice-level records)
    and **Velocity** (data refreshed monthly by NHS BSA).

    Parameters
    ----------
    drug_bnf_code:
        BNF chemical-substance code, e.g. ``"0601023A0"`` for metformin.
    drug_name:
        Human-readable drug name used as a label in the returned payload,
        e.g. ``"metformin"``.

    Returns
    -------
    dict with keys:
        - ``drug``       : str  — drug name
        - ``bnf_code``   : str  — BNF code supplied
        - ``type``       : str  — always ``"openprescribing"``
        - ``total_rows`` : int  — number of records returned
        - ``records``    : list[dict] — each record contains:
            date, actual_cost, items, quantity, row_id, setting, ccg
    """
    url = _OPENPRESCRIBING_BASE.format(bnf_code=drug_bnf_code)
    _log.info("Fetching OpenPrescribing: %s (%s)", drug_name, drug_bnf_code)

    response = httpx.get(url, timeout=_HTTP_TIMEOUT)
    response.raise_for_status()
    raw: list[dict] = response.json()

    records = [
        {
            "date": item.get("date", ""),
            "actual_cost": item.get("actual_cost"),
            "items": item.get("items"),
            "quantity": item.get("quantity"),
            "row_id": f"{item.get('row_id', '')}",
            "setting": item.get("setting", ""),
            "ccg": item.get("pct_id", ""),
            "drug": drug_name,
        }
        for item in raw
    ]

    _log.info("%s: %d records fetched", drug_name, len(records))
    return {
        "drug": drug_name,
        "bnf_code": drug_bnf_code,
        "type": "openprescribing",
        "total_rows": len(records),
        "records": records,
    }


# ---------------------------------------------------------------------------
# NHS.uk — unstructured clinical prose
# ---------------------------------------------------------------------------


@_RETRY
def fetch_nhs_pages(drug_name: str) -> dict:
    """Fetch and parse the three NHS.uk clinical sub-pages for one drug.

    Demonstrates **Variety** — completely different data structure from the
    OpenPrescribing API: raw HTML → structured sections with headings,
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

    Notes
    -----
    404 responses are skipped with a warning — some drug/slug combinations
    do not exist on NHS.uk.
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

        # Derive a clean page_type label from the slug
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

        # Walk headings and collect the content that follows each one
        headings = main.find_all(["h2", "h3"])  # type: ignore[union-attr]
        if not headings:
            # Fallback: grab all paragraphs as a single unnamed section
            text = " ".join(p.get_text(" ", strip=True) for p in main.find_all("p"))  # type: ignore[union-attr]
            bullets = [li.get_text(" ", strip=True) for li in main.find_all("li")]  # type: ignore[union-attr]
            pages.append(
                {
                    "url": url,
                    "page_type": page_type,
                    "heading": "",
                    "text": text,
                    "bullets": bullets,
                }
            )
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

                pages.append(
                    {
                        "url": url,
                        "page_type": page_type,
                        "heading": heading_text,
                        "text": " ".join(sibling_texts),
                        "bullets": sibling_bullets,
                    }
                )

    _log.info("%s: %d sections extracted across NHS pages", drug_name, len(pages))
    return {
        "drug": drug_name,
        "type": "nhs_pages",
        "pages": pages,
    }
