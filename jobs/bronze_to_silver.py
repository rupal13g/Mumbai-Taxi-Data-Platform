from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, current_timestamp


BUCKET = "mumbai-taxi-data-platform-rg1303"

BRONZE_PATH = f"gs://{BUCKET}/bronze/taxi_trips/*.jsonl"
SILVER_PATH = f"gs://{BUCKET}/silver/taxi_trips"
QUARANTINE_PATH = f"gs://{BUCKET}/quarantine/taxi_trips"


spark = (
    SparkSession.builder
    .appName("MumbaiTaxi-BronzeToSilver")
    .getOrCreate()
)


# 1. Read Bronze JSONL
df = spark.read.json(BRONZE_PATH)

print("=== Bronze schema ===")
df.printSchema()

bronze_count = df.count()
print("Bronze record count:", bronze_count)


# 2. Enforce/convert data types
df = (
    df
    .withColumn("event_timestamp", to_timestamp("event_timestamp"))
    .withColumn("distance_km", col("distance_km").cast("double"))
    .withColumn("duration_min", col("duration_min").cast("int"))
    .withColumn("fare_inr", col("fare_inr").cast("double"))
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("ingestion_timestamp", current_timestamp())
)


# 3. Validate records
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


# 4. Identify invalid records
invalid_df = df.filter(
    col("event_id").isNull()
    | col("event_timestamp").isNull()
    | col("pickup_zone").isNull()
    | col("dropoff_zone").isNull()
    | (col("distance_km") <= 0)
    | (col("duration_min") <= 0)
    | (col("fare_inr") <= 0)
    | ~col("passenger_count").between(1, 4)
)


# 5. Remove duplicate events from valid records
silver_df = valid_df.dropDuplicates(["event_id"])


# 6. Add event date for partitioning
silver_df = silver_df.withColumn(
    "event_date",
    to_date("event_timestamp")
)

invalid_df = invalid_df.withColumn(
    "event_date",
    to_date("event_timestamp")
)


# 7. Print data-quality statistics
valid_count = valid_df.count()
invalid_count = invalid_df.count()
silver_count = silver_df.count()

print("Valid records:", valid_count)
print("Invalid records:", invalid_count)
print("Silver records after deduplication:", silver_count)


# 8. Write valid records to Silver
(
    silver_df
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(SILVER_PATH)
)


# 9. Write invalid records to Quarantine
if invalid_count > 0:
    (
        invalid_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(QUARANTINE_PATH)
    )

    print("Invalid records written to:", QUARANTINE_PATH)
else:
    print("No invalid records found.")


print("Silver data written to:", SILVER_PATH)

spark.stop()