import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    current_timestamp,
    lit
)


BUCKET = "mumbai-taxi-data-platform-rg1303"

SILVER_PATH = f"gs://{BUCKET}/silver/taxi_trips"
QUARANTINE_PATH = f"gs://{BUCKET}/quarantine/taxi_trips"

BQ_AUDIT_TABLE = "mumbai_taxi.batch_audit"


if len(sys.argv) != 2:
    print("Usage: spark-submit bronze_to_silver.py <bronze_jsonl_path>")
    sys.exit(1)


BRONZE_PATH = sys.argv[1]

# Extract batch_id from:
# gs://.../batch_20261008_071134.jsonl
batch_id = os.path.basename(BRONZE_PATH).replace(".jsonl", "")
BATCH_QUARANTINE_PATH = f"{QUARANTINE_PATH}/{batch_id}"

print("=== Batch Processing ===")
print("Batch ID:", batch_id)
print("Bronze input:", BRONZE_PATH)


spark = (
    SparkSession.builder
    .appName(f"MumbaiTaxi-BronzeToSilver-{batch_id}")
    .getOrCreate()
)

# BigQuery connector temporary storage.
spark.conf.set(
    "temporaryGcsBucket",
    BUCKET
)


# ---------------------------------------------------------
# 1. Read this specific Bronze batch
# ---------------------------------------------------------

df = spark.read.json(BRONZE_PATH)

print("=== Bronze schema ===")
df.printSchema()

bronze_count = df.count()

print("Bronze record count:", bronze_count)


# ---------------------------------------------------------
# 2. Enforce data types
# ---------------------------------------------------------

df = (
    df
    .withColumn("event_timestamp", to_timestamp("event_timestamp"))
    .withColumn("distance_km", col("distance_km").cast("double"))
    .withColumn("duration_min", col("duration_min").cast("int"))
    .withColumn("fare_inr", col("fare_inr").cast("double"))
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("ingestion_timestamp", current_timestamp())
)


# ---------------------------------------------------------
# 3. Validate records
# ---------------------------------------------------------

valid_df = df.filter(
    col("event_id").isNotNull()
    & col("event_timestamp").isNotNull()
    & col("pickup_zone").isNotNull()
    & col("dropoff_zone").isNotNull()
    & (col("distance_km") > 0)
    & (col("duration_min") > 0)
    & (col("fare_inr") > 0)
    & col("passenger_count").between(1, 4)
)


# ---------------------------------------------------------
# 4. Identify invalid records
# ---------------------------------------------------------

invalid_df = df.filter(
    col("event_id").isNull()
    | col("event_timestamp").isNull()
    | col("pickup_zone").isNull()
    | col("dropoff_zone").isNull()
    | col("distance_km").isNull()
    | (col("distance_km") <= 0)
    | col("duration_min").isNull()
    | (col("duration_min") <= 0)
    | col("fare_inr").isNull()
    | (col("fare_inr") <= 0)
    | col("passenger_count").isNull()
    | (~col("passenger_count").between(1, 4))
)


# ---------------------------------------------------------
# 5. Deduplicate within this batch
# ---------------------------------------------------------

batch_silver_df = valid_df.dropDuplicates(["event_id"])


# ---------------------------------------------------------
# 6. Add event date
# ---------------------------------------------------------

batch_silver_df = batch_silver_df.withColumn(
    "event_date",
    to_date("event_timestamp")
)

invalid_df = invalid_df.withColumn(
    "event_date",
    to_date("event_timestamp")
)


# ---------------------------------------------------------
# 7. Remove events already present in Silver
# ---------------------------------------------------------

existing_silver_df = spark.read.parquet(SILVER_PATH)

existing_event_ids = existing_silver_df.select(
    "event_id"
).dropDuplicates()

new_silver_df = batch_silver_df.join(
    existing_event_ids,
    on="event_id",
    how="left_anti"
)


# ---------------------------------------------------------
# 8. Calculate batch metrics
# ---------------------------------------------------------

valid_count = valid_df.count()
invalid_count = invalid_df.count()
batch_silver_count = batch_silver_df.count()
new_silver_count = new_silver_df.count()

# Every Bronze record must be classified exactly once.
if bronze_count != valid_count + invalid_count:
    raise RuntimeError(
        "Data quality classification failed: "
        f"Bronze={bronze_count}, "
        f"Valid={valid_count}, "
        f"Invalid={invalid_count}"
    )

print("=== Batch Metrics ===")
print("Bronze records:", bronze_count)
print("Valid records:", valid_count)
print("Invalid records:", invalid_count)
print("Batch Silver records:", batch_silver_count)
print("New Silver records:", new_silver_count)


# ---------------------------------------------------------
# 9. Write NEW records to Silver
# ---------------------------------------------------------

if new_silver_count > 0:

    (
        new_silver_df
        .write
        .mode("append")
        .partitionBy("event_date")
        .parquet(SILVER_PATH)
    )

    print("New records written to Silver:", new_silver_count)

else:
    print("No new records to write to Silver.")


# ---------------------------------------------------------
# 10. Write invalid records to batch-specific Quarantine
# ---------------------------------------------------------

if invalid_count > 0:
    (
        invalid_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(BATCH_QUARANTINE_PATH)
    )

    print(
        "Invalid records written to:",
        BATCH_QUARANTINE_PATH
    )

else:
    print("No invalid records found.")


# ---------------------------------------------------------
# 11. Write batch audit record to BigQuery
# ---------------------------------------------------------

status = "SUCCESS" if invalid_count == 0 else "SUCCESS_WITH_REJECTIONS"

audit_df = spark.createDataFrame(
    [
        (
            batch_id,
            bronze_count,
            valid_count,
            invalid_count,
            batch_silver_count,
            status,
        )
    ],
    [
        "batch_id",
        "bronze_records",
        "valid_records",
        "invalid_records",
        "silver_records",
        "status",
    ]
).withColumn(
    "processed_at",
    current_timestamp()
)


print("=== Writing batch audit to BigQuery ===")
print("Target table:", BQ_AUDIT_TABLE)

(
    audit_df
    .write
    .format("bigquery")
    .option("table", BQ_AUDIT_TABLE)
    .mode("append")
    .save()
)

print("=== Batch audit written successfully ===")
print("Target table:", BQ_AUDIT_TABLE)

spark.stop()