"""
pipeline/lake.py — raw data lake read/write helpers.

Persists fetched payloads to the local file system in two formats:
  - JSONL (newline-delimited JSON) for structured prescribing records
  - JSON for unstructured NHS page payloads

This is the Variety V made physically visible: two completely different
file formats coexisting under the same lake/ directory tree.

Lake directory layout:
    lake/
    ├── metformin/
    │   ├── prescribing.jsonl
    │   └── nhs_pages.json
    ├── atorvastatin/
    │   ├── prescribing.jsonl
    │   └── nhs_pages.json
    ...
"""

from __future__ import annotations

import json
import pathlib


def write_lake(payload: dict, base_dir: pathlib.Path) -> pathlib.Path:
    """Write a fetched payload to the data lake.

    Demonstrates **Variety** — two source types are written in
    fundamentally different formats (JSONL vs JSON).

    Parameters
    ----------
    payload:
        Dict returned by ``fetch_openprescribing()`` or ``fetch_nhs_pages()``.
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
    data_type = payload["type"]
    drug_dir = base_dir / drug
    drug_dir.mkdir(parents=True, exist_ok=True)

    if data_type == "openprescribing":
        out_path = drug_dir / "prescribing.jsonl"
        with out_path.open("w", encoding="utf-8") as fh:
            for record in payload["records"]:
                fh.write(json.dumps(record) + "\n")
        print(f"  ✓ Wrote {len(payload['records']):,} records → {out_path}")

    elif data_type == "nhs_pages":
        out_path = drug_dir / "nhs_pages.json"
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        print(f"  ✓ Wrote {len(payload['pages'])} pages → {out_path}")

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
        Either ``"openprescribing"`` (reads JSONL) or ``"nhs_pages"``
        (reads JSON and returns the ``pages`` list).
    base_dir:
        Root of the data lake.

    Returns
    -------
    list[dict]
        For ``"openprescribing"``: one dict per prescribing record.
        For ``"nhs_pages"``: one dict per extracted page section.

    Raises
    ------
    FileNotFoundError
        If the expected lake file does not exist.
    ValueError
        If ``data_type`` is not recognised.
    """
    drug_dir = base_dir / drug

    if data_type == "openprescribing":
        path = drug_dir / "prescribing.jsonl"
        with path.open("r", encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]

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
    summary = []
    if not base_dir.exists():
        return summary

    for drug_dir in sorted(base_dir.iterdir()):
        if not drug_dir.is_dir():
            continue
        for file in sorted(drug_dir.iterdir()):
            size = file.stat().st_size
            summary.append(
                {
                    "drug": drug_dir.name,
                    "file": file.name,
                    "size_bytes": size,
                    "size_kb": round(size / 1024, 1),
                }
            )
    return summary
