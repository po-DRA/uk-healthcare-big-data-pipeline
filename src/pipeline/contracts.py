"""
pipeline/contracts.py — Pydantic data contracts for pipeline payloads.

Defines the **schema** that every payload must satisfy before it is written
to the Bronze lake.  This is the difference between *measuring* data quality
(veracity_report) and *enforcing* it: a ValidationError here stops bad data
entering the lake entirely, rather than letting it propagate to Silver where
TRY_CAST would silently convert it to NULL.

Two source contracts are defined:

  NHSBSAPayload    — structured prescribing records streamed from NHSBSA EPD CSV
  NHSPagesPayload  — unstructured clinical sections scraped from NHS.uk

Usage (in fetch.py)::

    from pipeline.contracts import NHSBSAPayload
    validated = NHSBSAPayload.model_validate(raw_payload)
    return validated.model_dump()

Raises ``pydantic.ValidationError`` (a subclass of ``ValueError``) if the
payload does not satisfy the contract.

Design notes
------------
* ``model_config = ConfigDict(extra="ignore")`` — unknown fields are silently
  dropped, making the contract tolerant of additive source changes.

* Numeric fields use ``float | None`` and ``int | None``.  Pydantic v2 will
  attempt coercion before raising, mirroring DuckDB ``TRY_CAST`` in Silver.

* ``SilverDQViolation`` is raised when null-rate thresholds are breached.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Bronze-layer payload contracts
# ---------------------------------------------------------------------------


class PrescribingRecord(BaseModel):
    """One row from the NHSBSA EPD, normalised by fetch.py."""

    model_config = ConfigDict(extra="ignore")

    date: str
    actual_cost: float | None = None
    items: int | None = None
    quantity: float | None = None
    row_id: str
    setting: str = ""
    ccg: str = ""
    icb_name: str = ""
    drug: str


class NHSBSAPayload(BaseModel):
    """Contract for payloads returned by ``fetch_nhsbsa()``."""

    model_config = ConfigDict(extra="ignore")

    drug: str
    bnf_code: str
    source: Literal["nhsbsa_epd"]
    resource: str
    total_rows: int = Field(ge=0)
    records: list[PrescribingRecord]


class NHSPage(BaseModel):
    """One extracted section from an NHS.uk medicines page."""

    model_config = ConfigDict(extra="ignore")

    url: str
    page_type: Literal["side_effects", "contraindications", "interactions"]
    heading: str = ""
    text: str = ""
    bullets: list[str] = Field(default_factory=list)


class NHSPagesPayload(BaseModel):
    """Contract for payloads returned by ``fetch_nhs_pages()``."""

    model_config = ConfigDict(extra="ignore")

    drug: str
    type: Literal["nhs_pages"]
    pages: list[NHSPage]


# ---------------------------------------------------------------------------
# Silver-layer DQ thresholds and violation reporting
# ---------------------------------------------------------------------------

#: Default maximum null-rate (%) per field before the pipeline is halted.
#: Chosen conservatively for NHS prescribing data:
#:   actual_cost — critical financial field; >5% nulls is alarming
#:   items        — primary volume metric; >3% undermines reporting
#:   setting      — practice metadata; >1% means practice dimension is broken
DEFAULT_NULL_THRESHOLDS: dict[str, float] = {
    "actual_cost": 5.0,
    "items": 3.0,
    "setting": 1.0,
}


class SilverDQViolation(Exception):
    """Raised when Silver null-rate thresholds are breached.

    Carries the full set of violations so callers can log structured details.

    Attributes
    ----------
    violations:
        Mapping of field name → dict with keys ``null_pct`` and ``threshold``.

    Example
    -------
    >>> raise SilverDQViolation({"actual_cost": {"null_pct": 7.2, "threshold": 5.0}})
    """

    def __init__(self, violations: dict[str, dict[str, float]]) -> None:
        self.violations = violations
        lines = [
            f"  {field}: {v['null_pct']:.2f}% nulls (threshold: {v['threshold']:.1f}%)"
            for field, v in violations.items()
        ]
        super().__init__("Silver DQ thresholds breached:\n" + "\n".join(lines))
