# Mumbai Taxi Data Engineering Platform

An end-to-end Data Engineering project on Google Cloud Platform (GCP) that processes simulated Mumbai taxi-trip events through a Bronze–Silver–Gold data pipeline using Python, PySpark, Apache Hive, Dataproc, and BigQuery.

The project demonstrates data ingestion, data-quality validation, deduplication, distributed processing, SQL analytics, pipeline orchestration, batch auditing, automated batch selection, and data reconciliation.

## Architecture

```text
FastAPI Event Generator
        |
        v
Google Cloud Storage — Bronze (JSONL)
        |
        v
Dataproc / PySpark — Bronze to Silver
        |
        +--------------------+
        |                    |
        v                    v
GCS Silver (Parquet)    GCS Quarantine
        |
        v
Apache Hive SQL
        |
        v
Dataproc / PySpark — Silver to Gold
        |
        +--------------------+
        |                    |
        v                    v
GCS Gold (Parquet)     BigQuery Gold Metrics
                             |
                             v
                       Batch Audit Table
```

**Data layers:** Bronze preserves ingested JSONL batches, Silver stores validated and deduplicated records, and Gold contains analytical aggregates.

## Version 1 — Core Data Pipeline

V1 establishes the foundational data-processing workflow.

1. Generate simulated taxi-trip events using FastAPI.
2. Ingest JSONL batches into Google Cloud Storage.
3. Validate, type-convert, and deduplicate records using PySpark.
4. Route invalid records to quarantine.
5. Query Silver Parquet data through a partitioned Hive external table.
6. Aggregate trip counts, revenue, average fare, distance, and duration by event date and pickup zone.
7. Publish analytical metrics to BigQuery and record batch audit results.

## Version 2 — Python CLI Orchestration

V2 introduces `scripts/run_pipeline.py` to coordinate the processing stages.

The orchestrator validates Bronze input, submits Dataproc jobs, refreshes Hive partition metadata, and verifies BigQuery audit results and Gold table sanity conditions.

## Version 2.1 — Reliability and Data Quality

V2.1 strengthens pipeline verification and batch-level observability.

### Key improvements

- **Independent Silver-to-Gold reconciliation:** Compare aggregates calculated from Silver with the published Gold metrics.
- **Batch-attempt tracking:** Record pipeline attempts, statuses, start and finish times, and failure details in BigQuery.
- **Audit validation:** Verify processing counts and ensure Bronze records reconcile with valid and invalid records.
- **Data-quality safeguards:** Validate required fields and numeric constraints, deduplicate events, and quarantine invalid records.
- **Failure visibility:** Detect failed pipeline stages and retain diagnostic information for investigation.

### V2.1 baseline validation results

The following results were previously verified against actual GCP resources. They are historical baseline results and do not represent a new V2.2 validation run.

| Validation | Result |
|---|---:|
| Verified batch | `batch_20261009_073425` |
| Bronze records | 5 |
| Valid records | 5 |
| Invalid records | 0 |
| Silver records | 5 |
| Pipeline attempt | `SUCCESS` |
| Gold aggregate groups reconciled | 14 |
| Reconciliation mismatches | 0 |

The tested batch completed successfully, and the independent Silver-to-Gold reconciliation passed with zero mismatches.

## Version 2.2 — Automated Pipeline Execution

V2.2 improves execution safety and reduces manual batch-selection steps.

### Key improvements

- **Automatic Bronze batch selection:** When `--bronze-path` is omitted, the orchestrator selects the latest eligible timestamped Bronze JSONL batch.
- **Execution safety:** Batch selection skips batches identified as already successfully processed or currently running, based on recorded attempt and audit status.
- **Stale-attempt diagnostics:** Detects pipeline attempts older than 120 minutes for investigation.
- **Reliability continuity:** Retains V2.1 batch-attempt tracking, audit validation, and independent Silver-to-Gold reconciliation.
- **Cost-aware execution:** Stops before downstream processing when no eligible, unprocessed production Bronze batch is available, avoiding unnecessary Dataproc job submissions.
- **Duplicate-processing protection:** Skips batches identified as successfully processed or currently running based on recorded attempt and audit status.

### Execution

Inspect the available command-line arguments:

```powershell
python scripts/run_pipeline.py --help
```

To use automatic batch selection, run the pipeline without supplying `--bronze-path`. To process a specific batch, provide the supported Bronze path argument.

**Stale-attempt handling:** Detection is diagnostic only. The orchestrator does not automatically recover or retry stale attempts.

**Release status:** The V2.2 implementation has been merged into `main`. Pipeline validation and the V2.2.0 release is successful and V2.2.0 is the latest version of this pipeline.

## Technology Stack

- **Python and FastAPI:** Simulated taxi-trip event generation and orchestration.
- **Google Cloud Storage:** Bronze, Silver, Gold, and quarantine storage.
- **Google Cloud Dataproc:** Distributed Spark job execution.
- **Apache Spark / PySpark:** Validation, deduplication, transformation, and aggregation.
- **Apache Hive:** SQL queries and partition metadata management.
- **BigQuery:** Analytical metrics, batch audits, and attempt tracking.
- **Git and GitHub:** Version control and release management.

## Repository Structure

```text
mumbai-taxi-data-platform/
├── api/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── gcs.py
│   │   └── main.py
│   └── requirements.txt
├── jobs/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── validate_silver_gold.py
├── scripts/
│   ├── run_pipeline.py
│   └── fixtures/
│       └── test_null_classification.jsonl
├── docs/
├── .gitignore
└── README.md
```

## GCP Resources

The development environment uses:

- **Cloud Storage bucket:** `mumbai-taxi-data-platform-rg1303`
- **Dataproc cluster:** `hadoop-cluster`
- **BigQuery dataset:** `mumbai_taxi`
- **BigQuery metrics table:** `mumbai_taxi.taxi_metrics`
- **BigQuery audit table:** `mumbai_taxi.batch_audit`
- **BigQuery attempt table:** `mumbai_taxi.batch_attempts`

The GCP project ID is environment-specific and should not be hardcoded into project documentation.

## Prerequisites

- Python and the required API dependencies.
- Google Cloud CLI installed and authenticated.
- An active GCP project with the necessary GCS, Dataproc, and BigQuery permissions.
- A running Dataproc cluster with the required Spark and Hive environment.
- Required BigQuery datasets and tables.
- An ingested Bronze JSONL batch when running with an explicit input path; automatic selection requires an eligible batch.

Inspect the available CLI arguments with:

```powershell
python scripts/run_pipeline.py --help
```

## Current Limitations

- Orchestration runs through a local Python CLI rather than a managed scheduler.
- Stale pipeline attempts require investigation; they are not automatically recovered.
- Quarantine retry behavior and duplicate quarantine prevention need further hardening.
- Multiple audit records may exist for a batch across repeated attempts.
- Automated unit and integration test coverage, monitoring, and alerting remain areas for improvement.

## Cost Awareness

Dataproc and other GCP resources may incur charges while active. Stop or delete resources when they are no longer needed, after checking data and configuration dependencies. Monitor billing and resource usage throughout development.

## Roadmap

### V2.3 — Testing and Engineering Quality

- Expand unit and integration tests.
- Add continuous integration checks.
- Improve logging and recovery documentation.
- Strengthen pipeline regression validation.

### V2.4 — Recovery and Operational Reliability

- Define how failed and stale attempts can be safely retried.
- Make reruns and partial failures easier to diagnose.
- Strengthen audit consistency and quarantine handling.
- Add structured logs and clearer error reporting.

### V2.5 — Scheduling and Monitoring

- Evaluate a managed scheduler or workflow orchestrator.
- Add run summaries, failure notifications, and data freshness checks.
- Track execution duration and GCP costs.
- Add operational dashboards where they provide real value.

### V3.0 — Production-Style Operations

- Add monitoring and failure alerts.
- Define data freshness and quality expectations.
- Tune performance using measured results.
- Document analytical use cases and an end-to-end demonstration.
