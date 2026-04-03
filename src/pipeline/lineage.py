"""
pipeline/lineage.py — Data lineage tracking via the OpenLineage standard.

What is lineage and why does it matter?
----------------------------------------
Without lineage, this question is impossible to answer quickly:

    "Our Gold cost figures changed on Tuesday — what upstream dataset caused it?"

With lineage, every Gold table has a recorded provenance chain::

    gold.drug_summary
        ← build_gold  (run abc123, completed 14:00)
            ← silver.prescribing
                ← build_silver  (run xyz456, completed 13:45)
                    ← lake/metformin/prescribing.jsonl
                        ← fetch_openprescribing  (run ..., completed 13:30)

This is exactly the **Veracity** observability that Level 3 data engineering
requires: knowing not just *what* the data contains, but *where it came from*
and *when each transformation ran*.

OpenLineage standard
--------------------
OpenLineage (https://openlineage.io) defines a vendor-neutral JSON schema for
lineage events.  Any tool that speaks OpenLineage — OpenMetadata, OpenMetadata,
Atlan, Astronomer, dbt — can consume these events.

Each event is a **RunEvent** with three parts:

1. **Job** — the transformation (e.g. ``build_silver``), identified by namespace + name.
2. **Run** — one execution of the job, identified by a UUID.
3. **Datasets** — which datasets the run consumed (inputs) and produced (outputs).

Event lifecycle: ``START`` → ``COMPLETE`` (or ``FAIL`` on error).

Configuration
-------------
Set ``OPENLINEAGE_URL`` to POST events to a running backend::

    # Run OpenMetadata locally (requires Docker Compose — see README):
    # Once running the UI is at http://localhost:8585

    OPENLINEAGE_URL=http://localhost:8585 uv run python flows/pipeline_flow.py

Without ``OPENLINEAGE_URL``, events are logged as structured JSON to the Python
logger — so students can see exactly what would be emitted without any infra.

Future: running OpenMetadata
----------------------------
See the README for a Docker Compose snippet that starts OpenMetadata alongside
the pipeline.  OpenMetadata adds data discovery, column-level lineage, data
quality integration, and a richer UI compared to a minimal lineage-only backend.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

#: Namespace groups all jobs in the OpenMetadata / OpenLineage UI.
NAMESPACE: str = "uk-healthcare-pipeline"

#: Set this env var to POST events to a real OpenLineage backend (e.g. OpenMetadata).
#: If unset, events are logged locally — no backend required.
OPENLINEAGE_URL: str = os.environ.get("OPENLINEAGE_URL", "")

#: OpenLineage specification URL included in every event.
_SPEC_URL = "https://openlineage.io/spec/1-0-5/OpenLineage.json"


# ---------------------------------------------------------------------------
# Dataset helpers
# ---------------------------------------------------------------------------


def dataset(name: str, namespace: str = NAMESPACE) -> dict:
    """Build an OpenLineage dataset reference dict.

    Parameters
    ----------
    name:
        Dataset name — use the logical name as it would appear in a data
        catalogue, e.g. ``"silver.prescribing"`` or
        ``"lake/metformin/prescribing.jsonl"``.
    namespace:
        Logical grouping for the dataset.  Defaults to the pipeline namespace.

    Returns
    -------
    dict
        Minimal OpenLineage dataset object with ``namespace`` and ``name``.

    Example
    -------
    >>> dataset("silver.prescribing")
    {'namespace': 'uk-healthcare-pipeline', 'name': 'silver.prescribing'}
    """
    return {"namespace": namespace, "name": name}


# ---------------------------------------------------------------------------
# Event emission
# ---------------------------------------------------------------------------


def emit_lineage_event(
    job_name: str,
    state: str,
    run_id: str,
    inputs: list[dict] | None = None,
    outputs: list[dict] | None = None,
) -> dict:
    """Build and emit one OpenLineage RunEvent.

    When ``OPENLINEAGE_URL`` is set the event is POSTed to that backend.
    Otherwise the event is logged as formatted JSON — useful for development
    and for students who want to see the event structure without running a
    backend.

    Parameters
    ----------
    job_name:
        Name of the job / transformation (e.g. ``"build_silver"``).
    state:
        One of ``"START"``, ``"COMPLETE"``, or ``"FAIL"``.
    run_id:
        UUID string identifying this execution.
    inputs:
        List of dataset dicts (from :func:`dataset`) consumed by this run.
    outputs:
        List of dataset dicts (from :func:`dataset`) produced by this run.

    Returns
    -------
    dict
        The OpenLineage event as a Python dict (also sent / logged).
    """
    event: dict = {
        "eventType": state,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {"runId": run_id},
        "job": {"namespace": NAMESPACE, "name": job_name},
        "inputs": inputs or [],
        "outputs": outputs or [],
        "producer": NAMESPACE,
        "schemaURL": _SPEC_URL,
    }

    if OPENLINEAGE_URL:
        try:
            import httpx  # already a project dependency

            resp = httpx.post(
                f"{OPENLINEAGE_URL}/api/v1/lineage",
                json=event,
                timeout=5.0,
            )
            resp.raise_for_status()
            _log.debug(
                "Lineage event emitted: job=%s state=%s run=%s → %s",
                job_name,
                state,
                run_id,
                OPENLINEAGE_URL,
            )
        except Exception as exc:
            # Lineage failure must never break the pipeline itself.
            _log.warning(
                "Lineage emission failed (job=%s state=%s): %s", job_name, state, exc
            )
    else:
        # No backend configured — log the event so students can see its structure.
        _log.info(
            "LINEAGE [no backend] %s job=%s run=%s inputs=%s outputs=%s\n%s",
            state,
            job_name,
            run_id,
            [d["name"] for d in (inputs or [])],
            [d["name"] for d in (outputs or [])],
            json.dumps(event, indent=2),
        )

    return event


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


@contextmanager
def lineage_job(
    job_name: str,
    inputs: list[dict] | None = None,
    outputs: list[dict] | None = None,
) -> Generator[str, None, None]:
    """Context manager that emits START → COMPLETE (or FAIL) lineage events.

    Wrapping a pipeline stage with ``lineage_job`` captures the full lifecycle
    of one execution: when it started, what datasets it read, what datasets it
    wrote, and whether it succeeded.

    Parameters
    ----------
    job_name:
        Name of the job / transformation.
    inputs:
        Datasets consumed (built with :func:`dataset`).
    outputs:
        Datasets produced (built with :func:`dataset`).

    Yields
    ------
    str
        The UUID run_id for this execution.

    Example
    -------
    ::

        with lineage_job(
            "build_silver",
            inputs=[dataset("lake/*/prescribing.jsonl")],
            outputs=[dataset("silver.prescribing")],
        ) as run_id:
            rows = build_silver(lake_dir, db_path)
    """
    run_id = str(uuid.uuid4())
    emit_lineage_event(job_name, "START", run_id, inputs, outputs)
    try:
        yield run_id
        emit_lineage_event(job_name, "COMPLETE", run_id, inputs, outputs)
    except Exception:
        emit_lineage_event(job_name, "FAIL", run_id, inputs, outputs)
        raise
