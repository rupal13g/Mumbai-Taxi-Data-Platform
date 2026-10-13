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
