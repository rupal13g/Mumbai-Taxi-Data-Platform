import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_pipeline.py"
SPEC = importlib.util.spec_from_file_location("run_pipeline", MODULE_PATH)
pipeline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pipeline)


def test_validate_bronze_path_accepts_valid_batch():
    path = f"{pipeline.BRONZE_PREFIX}batch_20261012_154208.jsonl"
    assert pipeline.validate_bronze_path(path) == "batch_20261012_154208"


@pytest.mark.parametrize(
    "path",
    [
        "gs://wrong-bucket/bronze/taxi_trips/batch_20261012_154208.jsonl",
        f"{pipeline.BRONZE_PREFIX}batch_20261012_154208.csv",
        f"{pipeline.BRONZE_PREFIX}invalid_20261012_154208.jsonl",
    ],
)
def test_validate_bronze_path_rejects_invalid_paths(path):
    with pytest.raises(ValueError):
        pipeline.validate_bronze_path(path)


def test_sql_string_escapes_single_quotes():
    assert pipeline.sql_string("driver's trip") == "'driver''s trip'"


def test_sql_string_handles_backslashes():
    assert pipeline.sql_string(r"C:\taxi\data") == r"'C:\\taxi\\data'"


def test_select_bronze_path_picks_newest_eligible_batch(monkeypatch):
    prefix = pipeline.BRONZE_PREFIX
    paths = (
        f"{prefix}batch_20261010_120000.jsonl\n"
        f"{prefix}batch_20261012_154208.jsonl\n"
        f"{prefix}batch_20261011_120000.jsonl"
    )

    monkeypatch.setattr(
        pipeline, "run_command", lambda *args, **kwargs: paths
    )
    monkeypatch.setattr(
        pipeline,
        "run_bq_query",
        lambda *args, **kwargs: [{"batch_id": "batch_20261011_120000"}],
    )

    selected = pipeline.select_bronze_path("test-project")

    assert selected == f"{prefix}batch_20261012_154208.jsonl"


def test_select_bronze_path_skips_malformed_filenames(monkeypatch):
    prefix = pipeline.BRONZE_PREFIX
    paths = (
        f"{prefix}batch_latest.jsonl\n"
        f"{prefix}not_a_batch.jsonl\n"
        f"{prefix}batch_20261012_154208.jsonl"
    )

    monkeypatch.setattr(
        pipeline, "run_command", lambda *args, **kwargs: paths
    )
    monkeypatch.setattr(pipeline, "run_bq_query", lambda *args, **kwargs: [])

    assert pipeline.select_bronze_path("test-project") == (
        f"{prefix}batch_20261012_154208.jsonl"
    )


def test_select_bronze_path_fails_when_all_batches_processed(monkeypatch):
    prefix = pipeline.BRONZE_PREFIX
    paths = f"{prefix}batch_20261012_154208.jsonl"

    monkeypatch.setattr(
        pipeline, "run_command", lambda *args, **kwargs: paths
    )
    monkeypatch.setattr(
        pipeline,
        "run_bq_query",
        lambda *args, **kwargs: [{"batch_id": "batch_20261012_154208"}],
    )

    with pytest.raises(RuntimeError, match="No unprocessed production Bronze batches"):
        pipeline.select_bronze_path("test-project")


@pytest.mark.parametrize(
    "filename",
    [
        "batch_latest.jsonl",
        "batch_20261312_154208.jsonl",
        "batch_20261012_256208.jsonl",
        "batch_20261012_15420.jsonl",
        "batch_20261012_154208_extra.jsonl",
    ],
)
def test_validate_bronze_path_rejects_malformed_batch_ids(filename):
    path = f"{pipeline.BRONZE_PREFIX}{filename}"

    with pytest.raises(ValueError):
        pipeline.validate_bronze_path(path)


def test_validate_bronze_path_rejects_nested_batch_path():
    path = (
        f"{pipeline.BRONZE_PREFIX}"
        "nested/batch_20261012_154208.jsonl"
    )

    with pytest.raises(ValueError):
        pipeline.validate_bronze_path(path)


def test_execute_pipeline_runs_stages_in_order(monkeypatch):
    stages = []

    def fake_run_command(command, step, timeout=None):
        stages.append(step)
        return ""

    def fake_run_bq_query(project_id, sql, step):
        if "batch_audit" in sql:
            return [{
                "batch_id": "batch_20261012_154208",
                "status": "SUCCESS",
                "bronze_records": "10",
                "valid_records": "10",
                "invalid_records": "0",
                "silver_records": "10",
                "processed_at": "2026-10-12T15:42:08",
            }]

        return [{
            "gold_rows": "1",
            "total_revenue": "100.00",
        }]

    monkeypatch.setattr(
        pipeline, "run_command", fake_run_command
    )
    monkeypatch.setattr(
        pipeline, "run_bq_query", fake_run_bq_query
    )

    pipeline.execute_pipeline(
        "test-project",
        "batch_20261012_154208",
        f"{pipeline.BRONZE_PREFIX}batch_20261012_154208.jsonl",
    )

    assert stages == [
        "Bronze to Silver: validate, deduplicate and quarantine",
        "Register Silver partitions in Hive",
        "Silver to Gold and BigQuery publishing",
        "Reconcile Silver aggregates against BigQuery Gold",
    ]


def test_execute_pipeline_stops_when_stage_fails(monkeypatch):
    calls = []

    def fake_run_command(command, step, timeout=None):
        calls.append(step)
        if step == "Register Silver partitions in Hive":
            raise RuntimeError("Hive partition repair failed")
        return ""

    def fake_run_bq_query(project_id, sql, step):
        pytest.fail("BigQuery validation should not run after a stage failure")

    monkeypatch.setattr(pipeline, "run_command", fake_run_command)
    monkeypatch.setattr(pipeline, "run_bq_query", fake_run_bq_query)

    with pytest.raises(RuntimeError, match="Hive partition repair failed"):
        pipeline.execute_pipeline(
            "test-project",
            "batch_20261012_154208",
            f"{pipeline.BRONZE_PREFIX}batch_20261012_154208.jsonl",
        )

    assert calls == [
        "Bronze to Silver: validate, deduplicate and quarantine",
        "Register Silver partitions in Hive",
    ]


@pytest.mark.parametrize(
    ("audit_rows", "error"),
    [
        ([], "No audit record found"),
        (
            [{
                "batch_id": "batch_20261012_154208",
                "status": "FAILED",
                "bronze_records": "10",
                "valid_records": "10",
                "invalid_records": "0",
                "silver_records": "10",
                "processed_at": "2026-10-12T15:42:08",
            }],
            "has status: FAILED",
        ),
        (
            [{
                "batch_id": "batch_20261012_154208",
                "status": "SUCCESS",
                "bronze_records": "-1",
                "valid_records": "0",
                "invalid_records": "0",
                "silver_records": "0",
                "processed_at": "2026-10-12T15:42:08",
            }],
            "negative record count",
        ),
        (
            [{
                "batch_id": "batch_20261012_154208",
                "status": "SUCCESS",
                "bronze_records": "10",
                "valid_records": "8",
                "invalid_records": "1",
                "silver_records": "8",
                "processed_at": "2026-10-12T15:42:08",
            }],
            r"Bronze must equal valid \+ invalid",
        ),
        (
            [{
                "batch_id": "batch_20261012_154208",
                "status": "SUCCESS",
                "bronze_records": "10",
                "valid_records": "8",
                "invalid_records": "2",
                "silver_records": "9",
                "processed_at": "2026-10-12T15:42:08",
            }],
            "Silver exceeds valid batch records",
        ),
    ],
)
def test_execute_pipeline_rejects_invalid_audit(
    monkeypatch, audit_rows, error
):
    def fake_run_command(command, step, timeout=None):
        return ""

    def fake_run_bq_query(project_id, sql, step):
        if "batch_audit" in sql:
            return audit_rows
        return [{"gold_rows": "1", "total_revenue": "100.00"}]

    monkeypatch.setattr(pipeline, "run_command", fake_run_command)
    monkeypatch.setattr(pipeline, "run_bq_query", fake_run_bq_query)

    with pytest.raises(RuntimeError, match=error):
        pipeline.execute_pipeline(
            "test-project",
            "batch_20261012_154208",
            f"{pipeline.BRONZE_PREFIX}batch_20261012_154208.jsonl",
        )


def test_main_marks_attempt_failed_when_pipeline_fails(monkeypatch):
    calls = []

    monkeypatch.setattr(pipeline, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        pipeline, "select_bronze_path",
        lambda project_id: (
            f"{pipeline.BRONZE_PREFIX}batch_20261012_154208.jsonl"
        ),
    )
    monkeypatch.setattr(
        pipeline, "write_attempt",
        lambda **kwargs: calls.append(("write", kwargs["status"])),
    )

    def fake_execute(*args):
        raise RuntimeError("Simulated pipeline failure")

    monkeypatch.setattr(pipeline, "execute_pipeline", fake_execute)

    def fake_update(project_id, attempt_id, status, failure_reason=None):
        calls.append(("update", status, failure_reason))

    monkeypatch.setattr(pipeline, "update_attempt", fake_update)
    monkeypatch.setattr("sys.argv", ["run_pipeline.py"])

    with pytest.raises(SystemExit) as exc:
        pipeline.main()

    assert exc.value.code == 1
    assert calls[0] == ("write", "RUNNING")
    assert calls[1][0:2] == ("update", "FAILED")
    assert "Simulated pipeline failure" in calls[1][2]


def test_main_reports_failure_when_failed_status_cannot_be_recorded(
    monkeypatch, capsys
):
    monkeypatch.setattr(pipeline, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        pipeline, "select_bronze_path",
        lambda project_id: (
            f"{pipeline.BRONZE_PREFIX}batch_20261012_154208.jsonl"
        ),
    )
    monkeypatch.setattr(pipeline, "write_attempt", lambda **kwargs: None)
    monkeypatch.setattr(
        pipeline, "execute_pipeline",
        lambda *args: (_ for _ in ()).throw(
            RuntimeError("Simulated pipeline failure")
        ),
    )

    def fail_update(*args, **kwargs):
        raise RuntimeError("BigQuery unavailable")

    monkeypatch.setattr(pipeline, "update_attempt", fail_update)
    monkeypatch.setattr("sys.argv", ["run_pipeline.py"])

    with pytest.raises(SystemExit) as exc:
        pipeline.main()

    output = capsys.readouterr().out
    assert exc.value.code == 1
    assert "PIPELINE FAILED" in output
    assert "CRITICAL: Could not record FAILED attempt status" in output
    assert "BigQuery unavailable" in output


def test_main_fails_if_success_finalization_fails(monkeypatch, capsys):
    monkeypatch.setattr(pipeline, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        pipeline,
        "select_bronze_path",
        lambda project_id: (
            f"{pipeline.BRONZE_PREFIX}batch_20261012_154208.jsonl"
        ),
    )
    monkeypatch.setattr(pipeline, "write_attempt", lambda **kwargs: None)
    monkeypatch.setattr(pipeline, "execute_pipeline", lambda *args: None)
    monkeypatch.setattr("sys.argv", ["run_pipeline.py"])

    def fail_success_update(project_id, attempt_id, status, failure_reason=None):
        assert status == "SUCCESS"
        raise RuntimeError("BigQuery finalization failed")

    monkeypatch.setattr(pipeline, "update_attempt", fail_success_update)

    with pytest.raises(RuntimeError, match="BigQuery finalization failed"):
        pipeline.main()

    output = capsys.readouterr().out
    assert "PIPELINE COMPLETED" not in output


def test_select_bronze_path_does_not_retry_running_batch(monkeypatch):
    prefix = pipeline.BRONZE_PREFIX
    paths = f"{prefix}batch_20261012_154208.jsonl"

    monkeypatch.setattr(
        pipeline, "run_command", lambda *args, **kwargs: paths
    )
    monkeypatch.setattr(
        pipeline,
        "run_bq_query",
        lambda *args, **kwargs: [
            {"batch_id": "batch_20261012_154208"}
        ],
    )

    with pytest.raises(
        RuntimeError,
        match="No unprocessed production Bronze batches",
    ):
        pipeline.select_bronze_path("test-project")


def test_stale_check_cli_does_not_start_pipeline(monkeypatch):
    calls = []

    monkeypatch.setattr(
        pipeline, "get_project_id",
        lambda: "test-project",
    )
    monkeypatch.setattr(
        pipeline, "check_stale_attempts",
        lambda project_id: calls.append(("stale_check", project_id)),
    )
    monkeypatch.setattr(
        pipeline, "execute_pipeline",
        lambda *args: pytest.fail("Pipeline must not execute"),
    )
    monkeypatch.setattr(
        pipeline, "write_attempt",
        lambda **kwargs: pytest.fail("Attempt must not be written"),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["run_pipeline.py", "--check-stale-attempts"],
    )

    pipeline.main()

    assert calls == [("stale_check", "test-project")]
