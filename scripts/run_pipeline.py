import argparse
import subprocess
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

SILVER_JOB = PROJECT_ROOT / "jobs" / "bronze_to_silver.py"
GOLD_JOB = PROJECT_ROOT / "jobs" / "silver_to_gold.py"


def run_command(command, step):
    print(f"\n=== {step} ===", flush=True)
    print("$ " + subprocess.list2cmdline(command), flush=True)

    result = subprocess.run(
        command,
        text=True,
        capture_output=True,
    )

    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")

    if result.returncode != 0:
        if result.stderr:
            print(result.stderr)
        raise RuntimeError(
            f"Step failed: {step} (exit code {result.returncode})"
        )

    if result.stderr:
        print(result.stderr)

    return result.stdout.strip()


def get_project_id():
    project_id = run_command(
        [GCLOUD, "config", "get-value", "project"],
        "Checking active GCP project",
    )

    if not project_id or project_id == "(unset)":
        raise RuntimeError(
            "No active GCP project. Set one with "
            "'GCLOUD config set project YOUR_PROJECT_ID'."
        )

    return project_id


def validate_bronze_path(bronze_path):
    if not bronze_path.startswith(BRONZE_PREFIX):
        raise ValueError(
            f"Bronze path must start with {BRONZE_PREFIX}"
        )

    filename = PurePosixPath(bronze_path).name

    if not filename.endswith(".jsonl"):
        raise ValueError("Bronze input must be a .jsonl file")

    batch_id = filename.removesuffix(".jsonl")

    if not batch_id.startswith("batch_"):
        raise ValueError("Could not extract a valid batch ID from filename")

    return batch_id


def main():
    parser = argparse.ArgumentParser(
        description="Run the V2 Mumbai Taxi downstream pipeline"
    )
    parser.add_argument(
        "--bronze-path",
        required=True,
        help="Full gs:// path to an existing Bronze JSONL batch",
    )
    args = parser.parse_args()

    batch_id = validate_bronze_path(args.bronze_path)

    for job in (SILVER_JOB, GOLD_JOB):
        if not job.is_file():
            raise FileNotFoundError(f"Spark job not found: {job}")

    project_id = get_project_id()

    print("\n=== V2 Pipeline Configuration ===")
    print("Project:", project_id)
    print("Batch ID:", batch_id)
    print("Bronze path:", args.bronze_path)
    print("Cluster:", CLUSTER)
    print("Region:", REGION)

    run_command(
        [
            GCLOUD, "dataproc", "jobs", "submit", "pyspark",
            str(SILVER_JOB),
            f"--cluster={CLUSTER}",
            f"--region={REGION}",
            "--",
            args.bronze_path,
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

    audit_sql = (
        "SELECT batch_id, status, bronze_records, valid_records, "
        "invalid_records, silver_records, processed_at "
        f"FROM `{project_id}.mumbai_taxi.batch_audit` "
        f"WHERE batch_id = '{batch_id}' "
        "ORDER BY processed_at DESC LIMIT 1"
    )

    audit_result = run_command(
        [
            BQ, "query",
            "--use_legacy_sql=false",
            "--format=csv",
            audit_sql,
        ],
        "Verify batch audit in BigQuery",
    )

    if batch_id not in audit_result:
        raise RuntimeError(
            f"No audit record found for batch {batch_id}"
        )

    gold_sql = (
        "SELECT COUNT(*) AS gold_rows, "
        "ROUND(COALESCE(SUM(total_revenue), 0), 2) AS total_revenue "
        f"FROM `{project_id}.mumbai_taxi.taxi_metrics`"
    )

    run_command(
        [
            BQ, "query",
            "--use_legacy_sql=false",
            "--format=csv",
            gold_sql,
        ],
        "Verify Gold metrics in BigQuery",
    )

    print("\n=== PIPELINE COMPLETED ===")
    print("Batch ID:", batch_id)
    print("Audit record found and Gold metrics query completed.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        print(f"\nPIPELINE FAILED: {exc}")
        raise SystemExit(1)
    except FileNotFoundError as exc:
        print(f"\nCOMMAND OR FILE NOT FOUND: {exc}")
        raise SystemExit(1)