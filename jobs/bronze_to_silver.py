from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, current_timestamp


BUCKET = "mumbai-taxi-data-platform-rg1303"

BRONZE_PATH = f"gs://{BUCKET}/bronze/taxi_trips/*.jsonl"
SILVER_PATH = f"gs://{BUCKET}/silver/taxi_trips"


spark = (
    SparkSession.builder
    .appName("MumbaiTaxi-BronzeToSilver")
    .getOrCreate()
)


# 1. Read Bronze JSONL
df = spark.read.json(BRONZE_PATH)

print("=== Bronze schema ===")
df.printSchema()

print("Bronze record count:", df.count())


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


# 4. Remove duplicate events
silver_df = valid_df.dropDuplicates(["event_id"])


# 5. Add partition column
silver_df = silver_df.withColumn(
    "event_date",
    to_date("event_timestamp")
)


print("Valid records:", valid_df.count())
print("Silver records:", silver_df.count())


# 6. Write Silver as partitioned Parquet
(
    silver_df
    .write
    .mode("append")
    .partitionBy("event_date")
    .parquet(SILVER_PATH)
)


print("Silver data written to:", SILVER_PATH)

spark.stop()