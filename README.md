# Mumbai Taxi Data Engineering Platform

An end-to-end Data Engineering project on Google Cloud Platform (GCP) that processes simulated Mumbai taxi-trip events through a Bronze–Silver–Gold data pipeline using PySpark, Apache Hive, Dataproc, and BigQuery.

The project demonstrates data ingestion, data-quality validation, deduplication, distributed processing, SQL analytics, pipeline orchestration, and batch-level verification.

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

**Source of truth:** Bronze stores the original ingested JSONL batches. Silver contains validated, deduplicated records, while Gold contains analytical aggregates.

## Version 1 — Manual Data Pipeline

V1 establishes the core data-processing workflow. Pipeline stages are executed manually, providing a baseline for subsequent orchestration and reliability improvements.

### Pipeline stages

1. **Bronze ingestion:** Generate simulated taxi-trip events using FastAPI and upload each batch as a JSONL object to GCS.
2. **Bronze to Silver:** Parse raw records, apply data-quality rules, convert fields to appropriate types, deduplicate by event ID, and route invalid records to quarantine.
3. **Hive analytics:** Query Silver Parquet data through an external, partitioned Hive table.
4. **Silver to Gold:** Aggregate trip counts, revenue, average fare, distance, and duration by event date and pickup zone.
5. **BigQuery publishing:** Publish analytical metrics to `mumbai_taxi.taxi_metrics` and record processing results in `mumbai_taxi.batch_audit`.

## Version 2 — Python CLI Orchestration

V2 introduces a Python orchestrator that coordinates the existing processing stages and verifies their results.

### Orchestration workflow

The `scripts/run_pipeline.py` script:

1. Accepts and validates a Bronze JSONL batch path.
2. Submits the Bronze-to-Silver PySpark job to Dataproc.
3. Refreshes Hive partition metadata.
4. Submits the Silver-to-Gold PySpark job.
5. Queries BigQuery for the latest audit record associated with the batch.
6. Validates the audit status and processing-count consistency.
7. Queries the Gold table and validates that it contains metric rows and a finite, nonnegative total revenue.
8. Reports pipeline completion or raises an error when a verification step fails.

### Data quality and reliability

* Validate required fields and numeric constraints.
* Separate invalid records into a quarantine dataset.
* Deduplicate Silver records by event ID.
* Avoid inserting previously processed valid events into Silver when the same batch is rerun.
* Record processing counts and status in BigQuery.
* Validate audit counts against the relationship `Bronze = Valid + Invalid`.
* Verify that Silver batch records do not exceed valid batch records.
* Verify basic Gold table and revenue sanity conditions.

### Verification results

The tested ten-event batch, `batch_20261009_124840.jsonl`, completed successfully.

| Check                        | Observed result |
| ---------------------------- | --------------- |
| Bronze records               | 10              |
| Valid records                | 10              |
| Invalid records              | 0               |
| Silver records for the batch | 10              |
| New Silver records on rerun  | 0               |
| Latest audit status          | `SUCCESS`       |
| Gold aggregate rows          | 12              |
| Gold total revenue           | ₹7,443.05       |

These results demonstrate successful execution and Silver idempotency for the tested batch. Gold verification currently checks basic table and revenue conditions; independent Silver-to-Gold reconciliation remains a planned improvement.

## Technology Stack

* **Python and FastAPI:** Generate simulated taxi-trip events.
* **Google Cloud Storage:** Store Bronze JSONL, Silver Parquet, quarantine data, and Gold Parquet.
* **Google Cloud Dataproc:** Execute distributed PySpark transformations.
* **Apache Spark / PySpark:** Process, validate, deduplicate, and aggregate records.
* **Apache Hive:** Query Silver Parquet data and manage partition metadata.
* **BigQuery:** Serve analytical metrics and retain batch audit records.
* **Git and GitHub:** Track source code, branches, and versioned releases.

## Repository Structure

```text
mumbai-taxi-data-platform/
├── api/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   └── gcs.py
│   └── requirements.txt
├── jobs/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── scripts/
│   └── run_pipeline.py
├── docs/
├── .gitignore
└── README.md
```

## GCP Resources

The current development environment uses:

* **Cloud Storage bucket:** `mumbai-taxi-data-platform-rg123`
* **Dataproc cluster:** `hadoop-cluster`
* **BigQuery dataset:** `mumbai_taxi`
* **BigQuery metrics table:** `mumbai_taxi.taxi_metrics`
* **BigQuery audit table:** `mumbai_taxi.batch_audit`

The project ID is environment-specific and should be obtained from the active GCP configuration rather than hardcoded into project documentation or scripts.

## Prerequisites

Before running the pipeline, ensure that you have:

* Python and the dependencies required by the API.
* Google Cloud CLI installed and authenticated.
* An active GCP project selected in the CLI.
* Permissions to submit Dataproc jobs and access the required GCS and BigQuery resources.
* A running Dataproc cluster with the required Spark and Hive environment.
* The necessary BigQuery dataset and tables.
* A previously ingested Bronze JSONL batch.

Use `python scripts/run_pipeline.py --help` to inspect the supported CLI arguments before execution.

## Current Limitations

* The orchestrator runs as a local Python CLI rather than through a managed scheduling service.
* A successful pipeline run does not yet guarantee independent reconciliation between Silver records and Gold aggregates.
* Quarantine retry behavior and duplicate quarantine prevention need further hardening.
* Batch audit records are append-only, so repeated attempts can produce multiple audit records for the same batch.
* Automated unit tests, integration tests, monitoring, and alerting remain future improvements.

## Cost Awareness

This project uses a small Dataproc cluster for experimentation. GCP resources may incur charges while active. Stop or delete resources when they are no longer required, after checking whether any project data or configuration depends on them.

Review GCP billing and resource usage regularly during development.

## Roadmap

### V2.1 — Reliability and Data Quality

* Independent Silver-to-Gold reconciliation.
* Stronger audit-attempt tracking.
* Retry-safe quarantine processing.
* Failure-recovery and data-quality tests.

### V2.2 — Automated Execution

* Reduce manual invocation steps.
* Improve configuration management.
* Evaluate managed orchestration options based on operational needs and cost.

### V2.3 — Testing and Engineering Quality

* Unit and integration tests.
* Continuous integration for automated checks.
* Better logging and recovery documentation.

### V3.0 — Production-Style Operations

* Monitoring and failure alerts.
* Data freshness and quality expectations.
* Performance tuning based on measurements.
* Documented analytical use cases and an end-to-end demo.
