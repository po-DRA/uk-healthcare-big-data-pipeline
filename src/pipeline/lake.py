"""
pipeline/lake.py — Bronze layer: raw data lake read/write helpers.

This module implements the **Bronze layer** of the medallion architecture.
Bronze is write-once: raw API responses are persisted exactly as received,
in two formats that coexist under the same ``lake/`` directory tree.

  Medallion layer: BRONZE  ← you are here
  Next layer:      SILVER  → see pipeline/medallion.py (build_silver)
  Final layer:     GOLD    → see pipeline/medallion.py (build_gold)

**Bronze rule:** never transform data in place.  If a Silver or Gold
computation is wrong, fix the transformation code and re-derive from
these unchanged Bronze files.

Formats written:
  - JSONL (newline-delimited JSON) for structured prescribing records
  - JSON for unstructured NHS page payloads

This is the Variety V made physically visible: two completely different
file formats coexisting under the same lake/ directory tree.

Lake directory layout (Bronze):
    lake/
    ├── metformin/
    │   ├── EPD_202506/
    │   │   └── prescribing.jsonl   ← one partition per monthly EPD file
    │   ├── EPD_202505/
    │   │   └── prescribing.jsonl
    │   └── nhs_pages.json          ← NHS pages stay at drug level (not monthly)
    ├── atorvastatin/
    │   ├── EPD_202506/
    │   │   └── prescribing.jsonl
    │   └── nhs_pages.json
    ...

Month-partitioned Bronze
------------------------
EPD data is partitioned by the NHSBSA resource name (e.g. ``EPD_202506``).
Each monthly run writes a new partition without touching previous months —
Bronze is write-once per partition.  Silver reads across all partitions with
a ``lake/*/*/prescribing.jsonl`` glob.  This mirrors cloud lake patterns:
    s3://bucket/drug=metformin/year_month=2025-06/part-0000.jsonl
"""

from __future__ import annotations

import json
import logging
import pathlib

_log = logging.getLogger(__name__)


def write_lake(payload: dict, base_dir: pathlib.Path) -> pathlib.Path:
    """Write a fetched payload to the data lake.

    Demonstrates **Variety** — two source types are written in
    fundamentally different formats (JSONL vs JSON).

    Parameters
    ----------
    payload:
        Dict returned by ``fetch_nhsbsa()`` or ``fetch_nhs_pages()``.
        Must contain ``"drug"`` and ``"type"`` keys.
    base_dir:
        Root of the data lake, e.g. ``pathlib.Path("lake")``.

    Returns
    -------
    pathlib.Path
        The path of the file that was written.

    Raises
    ------
    ValueError
        If ``payload["type"]`` is not a recognised data type.
    """
    drug = payload["drug"]
    data_type = payload.get("type") or payload.get("source", "")
    drug_dir = base_dir / drug
    drug_dir.mkdir(parents=True, exist_ok=True)

    if data_type in ("nhsbsa_epd", "openprescribing"):
        resource = payload.get("resource", "")
        partition_dir = (drug_dir / resource) if resource else drug_dir
        partition_dir.mkdir(parents=True, exist_ok=True)
        out_path = partition_dir / "prescribing.jsonl"
        tmp_path = out_path.with_suffix(".jsonl.tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as fh:
                for record in payload["records"]:
                    fh.write(json.dumps(record) + "\n")
            tmp_path.replace(out_path)  # atomic on POSIX and NTFS
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        _log.info("Wrote %d records → %s", len(payload["records"]), out_path)

    elif data_type == "nhs_pages":
        out_path = drug_dir / "nhs_pages.json"
        tmp_path = out_path.with_suffix(".json.tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
            tmp_path.replace(out_path)  # atomic on POSIX and NTFS
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        _log.info("Wrote %d pages → %s", len(payload["pages"]), out_path)

    else:
        raise ValueError(f"Unknown payload type: {data_type!r}")

    return out_path


def read_lake(
    drug: str,
    data_type: str,
    base_dir: pathlib.Path,
) -> list[dict]:
    """Read records back from the data lake.

    Parameters
    ----------
    drug:
        Drug name, e.g. ``"metformin"``.
    data_type:
        Either ``"nhsbsa_epd"`` (reads JSONL) or ``"nhs_pages"``
        (reads JSON and returns the ``pages`` list).
    base_dir:
        Root of the data lake.

    Returns
    -------
    list[dict]
        For ``"nhsbsa_epd"``: one dict per prescribing record.
        For ``"nhs_pages"``: one dict per extracted page section.

    Raises
    ------
    FileNotFoundError
        If the expected lake file does not exist.
    ValueError
        If ``data_type`` is not recognised.
    """
    drug_dir = base_dir / drug

    if data_type in ("nhsbsa_epd", "openprescribing"):
        # Read across all month partitions (lake/{drug}/{resource}/prescribing.jsonl)
        # plus the legacy flat file (lake/{drug}/prescribing.jsonl) for compatibility.
        records: list[dict] = []
        for jsonl_path in sorted(drug_dir.rglob("prescribing.jsonl")):
            with jsonl_path.open("r", encoding="utf-8") as fh:
                records.extend(json.loads(line) for line in fh if line.strip())
        if not records:
            raise FileNotFoundError(f"No prescribing.jsonl found under {drug_dir}")
        return records

    elif data_type == "nhs_pages":
        path = drug_dir / "nhs_pages.json"
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload.get("pages", [])

    else:
        raise ValueError(f"Unknown data_type: {data_type!r}")


def lake_summary(base_dir: pathlib.Path) -> list[dict]:
    """Return a summary of all files in the lake with sizes.

    Parameters
    ----------
    base_dir:
        Root of the data lake.

    Returns
    -------
    list[dict]
        Each dict has keys: drug, file, size_bytes, size_kb.
    """
    summary: list[dict] = []
    if not base_dir.exists():
        return summary

    for drug_dir in sorted(base_dir.iterdir()):
        if not drug_dir.is_dir():
            continue
        for item in sorted(drug_dir.rglob("*")):
            if item.is_dir():
                continue
            rel = item.relative_to(drug_dir)
            size = item.stat().st_size
            summary.append(
                {
                    "drug": drug_dir.name,
                    "file": str(rel),
                    "size_bytes": size,
                    "size_kb": round(size / 1024, 1),
                }
            )
    return summary
