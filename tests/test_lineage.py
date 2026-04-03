"""
tests/test_lineage.py — unit tests for pipeline/lineage.py.

All tests run in log-only mode (OPENLINEAGE_URL unset) so no backend is needed.
"""

from __future__ import annotations

import logging

import pytest

from pipeline.lineage import (
    NAMESPACE,
    dataset,
    emit_lineage_event,
    lineage_job,
)


# ---------------------------------------------------------------------------
# dataset() helper
# ---------------------------------------------------------------------------


def test_dataset_has_name_and_namespace():
    d = dataset("silver.prescribing")
    assert d["name"] == "silver.prescribing"
    assert d["namespace"] == NAMESPACE


def test_dataset_custom_namespace():
    d = dataset("my_table", namespace="other")
    assert d["namespace"] == "other"


def test_dataset_returns_dict():
    d = dataset("gold.drug_summary")
    assert isinstance(d, dict)
    assert set(d.keys()) == {"namespace", "name"}


# ---------------------------------------------------------------------------
# emit_lineage_event() structure
# ---------------------------------------------------------------------------


def test_emit_event_returns_dict():
    event = emit_lineage_event("build_silver", "START", "run-001")
    assert isinstance(event, dict)


def test_emit_event_required_fields():
    event = emit_lineage_event("build_silver", "COMPLETE", "run-002")
    required = {"eventType", "eventTime", "run", "job", "inputs", "outputs", "producer", "schemaURL"}
    assert required.issubset(set(event.keys()))


def test_emit_event_state_propagated():
    for state in ("START", "COMPLETE", "FAIL"):
        event = emit_lineage_event("test_job", state, "run-abc")
        assert event["eventType"] == state


def test_emit_event_job_name_and_namespace():
    event = emit_lineage_event("build_gold", "START", "run-003")
    assert event["job"]["name"] == "build_gold"
    assert event["job"]["namespace"] == NAMESPACE


def test_emit_event_run_id_propagated():
    event = emit_lineage_event("build_silver", "START", "my-run-id")
    assert event["run"]["runId"] == "my-run-id"


def test_emit_event_inputs_and_outputs():
    inputs = [dataset("lake/metformin/prescribing.jsonl")]
    outputs = [dataset("silver.prescribing")]
    event = emit_lineage_event("build_silver", "START", "run-004", inputs=inputs, outputs=outputs)
    assert event["inputs"] == inputs
    assert event["outputs"] == outputs


def test_emit_event_empty_inputs_outputs_default():
    event = emit_lineage_event("build_silver", "START", "run-005")
    assert event["inputs"] == []
    assert event["outputs"] == []


def test_emit_event_schema_url_is_openlineage():
    event = emit_lineage_event("build_silver", "START", "run-006")
    assert "openlineage.io" in event["schemaURL"]


def test_emit_event_logs_when_no_backend(caplog):
    """Without OPENLINEAGE_URL, the event should be logged."""
    with caplog.at_level(logging.INFO, logger="pipeline.lineage"):
        emit_lineage_event("build_silver", "START", "run-007")
    assert any("LINEAGE" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# lineage_job() context manager
# ---------------------------------------------------------------------------


def test_lineage_job_yields_run_id():
    with lineage_job("test_job") as run_id:
        assert isinstance(run_id, str)
        assert len(run_id) == 36  # UUID4 format


def test_lineage_job_emits_start_and_complete(caplog):
    with caplog.at_level(logging.INFO, logger="pipeline.lineage"):
        with lineage_job("test_job"):
            pass
    messages = " ".join(r.message for r in caplog.records)
    assert "START" in messages
    assert "COMPLETE" in messages


def test_lineage_job_emits_fail_on_exception(caplog):
    with caplog.at_level(logging.INFO, logger="pipeline.lineage"):
        with pytest.raises(ValueError):
            with lineage_job("test_job"):
                raise ValueError("simulated failure")
    messages = " ".join(r.message for r in caplog.records)
    assert "FAIL" in messages
    assert "COMPLETE" not in messages


def test_lineage_job_reraises_exception():
    with pytest.raises(RuntimeError, match="pipeline error"):
        with lineage_job("test_job"):
            raise RuntimeError("pipeline error")


def test_lineage_job_unique_run_ids():
    run_ids = []
    for _ in range(3):
        with lineage_job("test_job") as run_id:
            run_ids.append(run_id)
    assert len(set(run_ids)) == 3  # all unique


def test_lineage_job_passes_datasets_to_events(caplog):
    inputs = [dataset("lake/metformin/prescribing.jsonl")]
    outputs = [dataset("silver.prescribing")]
    with caplog.at_level(logging.INFO, logger="pipeline.lineage"):
        with lineage_job("build_silver", inputs=inputs, outputs=outputs):
            pass
    messages = " ".join(r.message for r in caplog.records)
    assert "silver.prescribing" in messages
