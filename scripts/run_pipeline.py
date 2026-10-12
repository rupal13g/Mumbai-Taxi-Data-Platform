import argparse
import json
import os
import subprocess
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GCLOUD = r"C:\GCloud_CLI\google-cloud-sdk\bin\gcloud.cmd"
BQ = r"C:\GCloud_CLI\google-cloud-sdk\bin\bq.cmd"

BUCKET = "mumbai-taxi-data-platform-rg1303"
BRONZE_PREFIX = f"gs://{BUCKET}/bronze/taxi_trips/"
CLUSTER = "hadoop-cluster"
REGION = "us-east4"
ZONE = "us-east4-b"
MASTER = "hadoop-cluster-m"
COMMAND_TIMEOUT_SECONDS = 180

SILVER_JOB = PROJECT_ROOT / "jobs" / "bronze_to_silver.py"
GOLD_JOB = PROJECT_ROOT / "jobs" / "silver_to_gold.py"
VALIDATION_JOB = PROJECT_ROOT / "jobs" / "validate_silver_gold.py"


def run_command(command, step, timeout=COMMAND_TIMEOUT_SECONDS):
    print(f"\n=== {step} ===", flush=True)
    print("$ " + subprocess.list2cmdline(command), flush=True)

    creationflags = (
        subprocess.CREATE_NEW_PROCESS_GROUP
        if os.name == "nt"
        else 0
    )

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        creationflags=creationflags,
    )

    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        print(
            f"Command exceeded {timeout} seconds; terminating process.",
            flush=True,
        )

        if os.name == "nt":
            try:
                subprocess.run(
                    [
                        "taskkill",
                        "/PID",
                        str(process.pid),
                        "/T",
                        "/F",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                pass
        else:
            process.kill()

        try:
            stdout, stderr = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()

        if stdout:
            print(stdout, end="" if stdout.endswith("\n") else "\n")
        if stderr:
            print(stderr, end="" if stderr.endswith("\n") else "\n")

        raise RuntimeError(
            f"Step timed out after {timeout} seconds: {step}. "
            "Inspect BigQuery job history before retrying."
        ) from exc

    if stdout:
        print(stdout, end="" if stdout.endswith("\n") else "\n")

    if stderr:
        print(stderr, end="" if stderr.endswith("\n") else "\n")

    if process.returncode != 0:
        raise RuntimeError(
            f"Step failed: {step} (exit code {process.returncode})"
        )

    return stdout.strip()


def get_project_id():
    project_id = run_command(
        [GCLOUD, "config", "get-value", "project"],
        "Checking active GCP project",
    ).strip()

    if not project_id or project_id == "(unset)":
        raise RuntimeError(
            "No active GCP project. Set one with "
            "'gcloud config set project YOUR_PROJECT_ID'."
        )

    return project_id


def validate_bronze_path(bronze_path):
    if not bronze_path.startswith(BRONZE_PREFIX):
        raise ValueError(
            f"Bronze input must start with {BRONZE_PREFIX}"
        )

    filename = PurePosixPath(bronze_path).name

    if not filename.endswith(".jsonl"):
        raise ValueError("Bronze input must be a .jsonl file")

    batch_id = filename.removesuffix(".jsonl")

    if not batch_id.startswith("batch_"):
        raise ValueError(
            "Could not extract a valid batch ID from filename"
        )

    return batch_id


def check_stale_attempts(project_id, stale_minutes=120):
    sql = (
        "SELECT batch_id, attempt_id, status, started_at, "
        "TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), started_at, MINUTE) "
        "AS age_minutes "
        "FROM ("
        " SELECT batch_id, attempt_id, status, started_at, "
        " ROW_NUMBER() OVER ("
        " PARTITION BY batch_id ORDER BY started_at DESC"
        " ) AS rn "
        f" FROM `{project_id}.mumbai_taxi.batch_attempts`"
        ") "
        "WHERE rn = 1 AND status = 'RUNNING' "
        f"AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), started_at, MINUTE) "
        f"> {int(stale_minutes)} "
        "ORDER BY started_at"
    )

    rows = run_bq_query(
        project_id,
        sql,
        "Check for potentially stale pipeline attempts",
    )

    if not rows:
        print("No potentially stale RUNNING attempts found.")
        return

    for row in rows:
        print(
            "REVIEW REQUIRED:",
            row["batch_id"],
            "| Attempt:",
            row["attempt_id"],
            "| Age (minutes):",
            row["age_minutes"],
        )

    print("Do not retry these batches until remote job activity is checked.")


def select_bronze_path(project_id):
    """Select the newest timestamped production Bronze batch not yet completed."""
    output = run_command(
        [GCLOUD, "storage", "ls", BRONZE_PREFIX],
        "Discovering Bronze JSONL batches",
    )

    candidates = []
    for line in output.splitlines():
        path = line.strip()

        # Only select timestamped production batches, not test fixtures.
        filename = PurePosixPath(path).name
        if (
            path.startswith(BRONZE_PREFIX)
            and filename.endswith(".jsonl")
            and filename.startswith("batch_")
        ):
            batch_id = filename.removesuffix(".jsonl")
            timestamp = batch_id.removeprefix("batch_")

            try:
                datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
            except ValueError:
                continue

            candidates.append((batch_id, path))

    if not candidates:
        raise RuntimeError(
            f"No timestamped production JSONL batches found under {BRONZE_PREFIX}"
        )

    # Latest attempt determines whether a batch completed successfully.
    processed_sql = (
        "WITH latest_attempt AS ("
        " SELECT batch_id, status, "
        " ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY started_at DESC) AS rn"
        f" FROM `{project_id}.mumbai_taxi.batch_attempts`"
        "), latest_audit AS ("
        " SELECT batch_id, status, "
        " ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY processed_at DESC) AS rn"
        f" FROM `{project_id}.mumbai_taxi.batch_audit`"
        ") "
        "SELECT batch_id FROM latest_attempt "
        "WHERE rn = 1 AND status IN ('SUCCESS', 'RUNNING') "
        "UNION DISTINCT "
        "SELECT a.batch_id FROM latest_audit a "
        "LEFT JOIN latest_attempt t ON a.batch_id = t.batch_id AND t.rn = 1 "
        "WHERE a.rn = 1 "
        "AND UPPER(a.status) IN ('SUCCESS', 'SUCCESS_WITH_REJECTIONS') "
        "AND t.batch_id IS NULL"
    )

    rows = run_bq_query(
        project_id,
        processed_sql,
        "Find previously completed Bronze batches",
    )
    processed_batches = {row["batch_id"] for row in rows}

    # Timestamped IDs sort chronologically in this naming convention.
    candidates.sort(key=lambda item: item[0], reverse=True)

    for batch_id, path in candidates:
        if batch_id not in processed_batches:
            print(f"Automatically selected Bronze batch: {batch_id}")
            return path

    raise RuntimeError(
        "No unprocessed production Bronze batches found. "
        "Upload a new batch or specify --bronze-path to rerun a batch explicitly."
    )


def sql_string(value):
    """Encode a Python string as a BigQuery Standard SQL string literal."""
    value = str(value)
    value = value.replace("\\", "\\\\")
    value = value.replace("'", "''")
    value = value.replace("\r", "\\r")
    value = value.replace("\n", "\\n")
    value = value.replace("\t", "\\t")
    return f"'{value}'"


def run_bq_query(project_id, sql, step):
    output = run_command(
        [
            BQ,
            "query",
            "--project_id=" + project_id,
            "--location=" + REGION,
            "--use_legacy_sql=false",
            "--format=json",
            sql,
        ],
        step,
    )

    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Could not parse BigQuery output for: {step}"
        ) from exc


def write_attempt(
    project_id,
    attempt_id,
    batch_id,
    status,
    started_at,
    finished_at=None,
    failure_reason=None,
):
    started_literal = started_at.astimezone(timezone.utc).isoformat()

    finished_sql = (
        f"TIMESTAMP({sql_string(finished_at.astimezone(timezone.utc).isoformat())})"
        if finished_at is not None
        else "CAST(NULL AS TIMESTAMP)"
    )

    reason_sql = (
        sql_string(failure_reason)
        if failure_reason is not None
        else "CAST(NULL AS STRING)"
    )

    # Keep the SQL statement on one line when passing it to bq.cmd.
    sql = (
        f"INSERT INTO `{project_id}.mumbai_taxi.batch_attempts` "
        "(attempt_id, batch_id, status, started_at, finished_at, failure_reason) "
        f"VALUES ({sql_string(attempt_id)}, {sql_string(batch_id)}, "
        f"{sql_string(status)}, TIMESTAMP({sql_string(started_literal)}), "
        f"{finished_sql}, {reason_sql})"
    )

    run_command(
        [
            BQ,
            "query",
            "--project_id=" + project_id,
            "--location=" + REGION,
            "--use_legacy_sql=false",
            sql,
        ],
        f"Record attempt status: {status}",
    )


def update_attempt(
    project_id,
    attempt_id,
    status,
    failure_reason=None,
):
    reason_sql = (
        sql_string(failure_reason)
        if failure_reason is not None
        else "CAST(NULL AS STRING)"
    )

    # Keep the SQL statement on one line for the Windows CLI.
    sql = (
        f"UPDATE `{project_id}.mumbai_taxi.batch_attempts` "
        f"SET status = {sql_string(status)}, "
        "finished_at = CURRENT_TIMESTAMP(), "
        f"failure_reason = {reason_sql} "
        f"WHERE attempt_id = {sql_string(attempt_id)} "
        "AND status = 'RUNNING'"
    )

    output = run_command(
        [
            BQ,
            "query",
            "--project_id=" + project_id,
            "--location=" + REGION,
            "--use_legacy_sql=false",
            sql,
        ],
        f"Finalize attempt as {status}",
    )

    # bq reports affected rows for DML statements. Check it when reported.
    if "Number of affected rows: 0" in output:
        raise RuntimeError(
            f"Attempt {attempt_id} was not updated. "
            "It may not exist or may no longer be RUNNING."
        )


def execute_pipeline(project_id, batch_id, bronze_path):
    print("\n=== V2 Pipeline Configuration ===")
    print("Project:", project_id)
    print("Batch ID:", batch_id)
    print("Bronze path:", bronze_path)
    print("Cluster:", CLUSTER)
    print("Region:", REGION)

    run_command(
        [
            GCLOUD, "dataproc", "jobs", "submit", "pyspark",
            str(SILVER_JOB),
            f"--cluster={CLUSTER}",
            f"--region={REGION}",
            "--",
            bronze_path,
        ],
        "Bronze to Silver: validate, deduplicate and quarantine",
    )

    run_command(
        [
            GCLOUD, "compute", "ssh", MASTER,
            f"--zone={ZONE}",
            f"--project={project_id}",
            "--command",
            'hive -e "MSCK REPAIR TABLE mumbai_taxi.taxi_trips_silver"',
        ],
        "Register Silver partitions in Hive",
    )

    run_command(
        [
            GCLOUD, "dataproc", "jobs", "submit", "pyspark",
            str(GOLD_JOB),
            f"--cluster={CLUSTER}",
            f"--region={REGION}",
        ],
        "Silver to Gold and BigQuery publishing",
    )

    run_command(
        [
            GCLOUD, "dataproc", "jobs", "submit", "pyspark",
            str(VALIDATION_JOB),
            f"--cluster={CLUSTER}",
            f"--region={REGION}",
            "--",
            "--project-id",
            project_id,
        ],
        "Reconcile Silver aggregates against BigQuery Gold",
    )

    audit_sql = (
        "SELECT batch_id, status, bronze_records, valid_records, "
        "invalid_records, silver_records, processed_at "
        f"FROM `{project_id}.mumbai_taxi.batch_audit` "
        f"WHERE batch_id = {sql_string(batch_id)} "
        "ORDER BY processed_at DESC LIMIT 1"
    )

    audit_rows = run_bq_query(
        project_id,
        audit_sql,
        "Verify batch audit in BigQuery",
    )

    if not audit_rows:
        raise RuntimeError(f"No audit record found for batch {batch_id}")

    audit = audit_rows[0]

    if audit["batch_id"] != batch_id:
        raise RuntimeError("Audit batch ID does not match requested batch")

    accepted_statuses = {"SUCCESS", "SUCCESS_WITH_REJECTIONS"}
    if audit["status"].upper() not in accepted_statuses:
        raise RuntimeError(f"Batch {batch_id} has status: {audit['status']}")

    bronze_count = int(audit["bronze_records"])
    valid_count = int(audit["valid_records"])
    invalid_count = int(audit["invalid_records"])
    silver_count = int(audit["silver_records"])

    if min(
        bronze_count, valid_count, invalid_count, silver_count
    ) < 0:
        raise RuntimeError("Audit contains a negative record count")

    if bronze_count != valid_count + invalid_count:
        raise RuntimeError(
            "Audit count mismatch: Bronze must equal valid + invalid"
        )

    if silver_count > valid_count:
        raise RuntimeError(
            "Audit count mismatch: Silver exceeds valid batch records"
        )

    print(
        f"Audit validated: Bronze={bronze_count}, "
        f"Valid={valid_count}, Invalid={invalid_count}, "
        f"Silver={silver_count}"
    )

    gold_sql = (
        "SELECT COUNT(*) AS gold_rows, "
        "ROUND(COALESCE(SUM(total_revenue), 0), 2) AS total_revenue "
        f"FROM `{project_id}.mumbai_taxi.taxi_metrics`"
    )

    gold_rows = run_bq_query(
        project_id,
        gold_sql,
        "Verify Gold metrics in BigQuery",
    )

    if not gold_rows:
        raise RuntimeError("Gold verification returned no result")

    row_count = int(gold_rows[0]["gold_rows"])
    revenue = Decimal(str(gold_rows[0]["total_revenue"]))

    if row_count <= 0:
        raise RuntimeError("Gold table contains no metric rows")

    if not revenue.is_finite() or revenue < 0:
        raise RuntimeError(f"Invalid Gold revenue total: {revenue}")

    print(
        f"Gold validated: {row_count} rows, "
        f"revenue={revenue:.2f}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Run the V2 Mumbai Taxi downstream pipeline"
    )
    parser.add_argument(
        "--bronze-path",
        required=False,
        default=None,
        help="Optional full gs:// path to a specific Bronze JSONL batch",
    )
    args = parser.parse_args()

    for job in (SILVER_JOB, GOLD_JOB, VALIDATION_JOB):
        if not job.is_file():
            raise FileNotFoundError(f"Spark job not found: {job}")

    project_id = get_project_id()

    if args.bronze_path:
        bronze_path = args.bronze_path
        batch_id = validate_bronze_path(bronze_path)
    else:
        bronze_path = select_bronze_path(project_id)
        batch_id = validate_bronze_path(bronze_path)

    attempt_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    print("\n=== Pipeline Attempt ===")
    print("Attempt ID:", attempt_id)
    print("Batch ID:", batch_id)

    # Record the attempt before submitting processing jobs.
    write_attempt(
        project_id=project_id,
        attempt_id=attempt_id,
        batch_id=batch_id,
        status="RUNNING",
        started_at=started_at,
    )

    try:
        execute_pipeline(project_id, batch_id, bronze_path)
    except Exception as exc:
        failure_reason = f"{type(exc).__name__}: {exc}"[:4000]
        print(f"\nPIPELINE FAILED: {failure_reason}")

        try:
            update_attempt(
                project_id,
                attempt_id,
                "FAILED",
                failure_reason,
            )
        except Exception as audit_exc:
            print(
                "CRITICAL: Could not record FAILED attempt status: "
                f"{type(audit_exc).__name__}: {audit_exc}"
            )

        raise SystemExit(1) from exc

    # If finalization fails, the row remains RUNNING rather than
    # incorrectly reporting success.
    update_attempt(project_id, attempt_id, "SUCCESS")

    print("\n=== PIPELINE COMPLETED ===")
    print("Batch ID:", batch_id)
    print("Attempt ID:", attempt_id)
    print("Audit, Gold, and reconciliation checks passed.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        print(f"\nPIPELINE FAILED: {exc}")
        raise SystemExit(1)
