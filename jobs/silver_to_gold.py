from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round
)


BUCKET = "mumbai-taxi-data-platform-rg1303"

SILVER_PATH = f"gs://{BUCKET}/silver/taxi_trips"
GOLD_PATH = f"gs://{BUCKET}/gold/taxi_metrics"


spark = (
    SparkSession.builder
    .appName("MumbaiTaxi-SilverToGold")
    .getOrCreate()
)


# 1. Read Silver
silver_df = spark.read.parquet(SILVER_PATH)

print("=== Silver schema ===")
silver_df.printSchema()

print("Silver record count:", silver_df.count())


# 2. Aggregate trips by date and pickup zone
gold_df = (
    silver_df
    .groupBy(
        "event_date",
        "pickup_zone"
    )
    .agg(
        count("*").alias("trip_count"),
        round(sum("fare_inr"), 2).alias("total_revenue"),
        round(avg("fare_inr"), 2).alias("avg_fare"),
        round(avg("distance_km"), 2).alias("avg_distance_km"),
        round(avg("duration_min"), 2).alias("avg_duration_min")
    )
)


# 3. Show results
print("=== Gold metrics ===")
gold_df.orderBy(
    "event_date",
    "trip_count",
    ascending=False
).show(truncate=False)


print("Gold record count:", gold_df.count())


# 4. Write Gold as partitioned Parquet
(
    gold_df
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(GOLD_PATH)
)


print("Gold data written to:", GOLD_PATH)


spark.stop()