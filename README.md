# Mumbai Taxi Data Engineering Platform

An end-to-end data engineering project built on Google Cloud Platform (GCP), processing simulated Mumbai taxi trip events through a Bronze–Silver–Gold data pipeline.

## V1 — Manual Data Pipeline

Version 1 establishes the core data-processing workflow. Each stage is executed manually, providing a working baseline before orchestration is introduced in V2.

### Architecture

FastAPI Event Generator → Google Cloud Storage (Bronze) → Dataproc / PySpark (Silver) → Hive SQL → Dataproc / PySpark (Gold) → BigQuery

Invalid records are routed to a separate quarantine area for data-quality inspection.

### Technology Stack

* **Python and FastAPI:** Generate simulated taxi-trip events.
* **Google Cloud Storage:** Store raw Bronze JSONL data, cleaned Silver Parquet data, quarantined records, and aggregated Gold Parquet data.
* **Google Cloud Dataproc:** Execute distributed PySpark transformations.
* **Apache Hive:** Query Silver Parquet data and register partitions.
* **BigQuery:** Serve aggregated taxi metrics and maintain batch-processing audit records.

### Data Pipeline

1. **Bronze ingestion:** Generate taxi-trip events through the FastAPI service and store each batch as a JSONL object in GCS.
2. **Bronze to Silver:** Parse and type raw records, validate data-quality rules, deduplicate by event ID, and separate invalid records into quarantine.
3. **Hive analytics:** Register Silver partitions and query the structured Parquet dataset using Hive SQL.
4. **Silver to Gold:** Aggregate trip counts, total revenue, average fare, distance, and duration by event date and pickup zone.
5. **BigQuery publishing:** Publish Gold metrics to `mumbai_taxi.taxi_metrics` and track batch-processing results in `mumbai_taxi.batch_audit`.

### Data Quality and Reliability

* Validate required fields and numeric constraints.
* Quarantine invalid records instead of mixing them with clean data.
* Deduplicate Silver records by event ID.
* Make Silver ingestion idempotent so rerunning a processed batch does not insert the same valid events again.
* Record processing counts and status in BigQuery.

### GCP Resources

* Cloud Storage bucket: `mumbai-taxi-data-platform-rg123`
* Dataproc cluster: `hadoop-cluster`
* BigQuery dataset: `mumbai_taxi`

### Current Scope

V1 uses manually executed commands to run the pipeline stages. The project is being extended in V2 with a Python orchestrator to coordinate processing, reduce manual intervention, and improve end-to-end verification.

### Cost Awareness

The project uses a small Dataproc cluster for experimentation. Cloud resources can incur charges while running, so the cluster should be stopped or deleted when no longer needed, subject to the project's current usage requirements.

## Repository Structure

```text
api/
  app/
    main.py
    gcs.py
  requirements.txt
jobs/
  bronze_to_silver.py
  silver_to_gold.py
docs/
README.md
```

## Future Improvements

* Orchestrate the pipeline using a Python CLI.
* Improve retry handling and batch-level verification.
* Add automated tests for transformations and data-quality rules.
* Improve monitoring, documentation, and operational recovery procedures.
* Evaluate managed scheduling options when justified by project needs.
