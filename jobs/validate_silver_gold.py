import argparse
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count,
    round,
    sum as spark_sum,
)


BUCKET = "mumbai-taxi-data-platform-rg1303"
SILVER_PATH = f"gs://{BUCKET}/silver/taxi_trips"

DATASET = "mumbai_taxi"
GOLD_TABLE = "taxi_metrics"

# Gold metrics are rounded to two decimal places.
# Allow a small tolerance when comparing floating-point values.
NUMERIC_TOLERANCE = 0.01

KEY_COLUMNS = ["event_date", "pickup_zone"]

METRIC_COLUMNS = [
    "trip_count",
    "total_revenue",
    "avg_fare",
    "avg_distance_km",
    "avg_duration_min",
]


def main():
    parser = argparse.ArgumentParser(
        description="Independently reconcile Silver aggregates with BigQuery Gold"
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="Active GCP project ID",
    )
    args = parser.parse_args()

    gold_table = f"{args.project_id}.{DATASET}.{GOLD_TABLE}"

    spark = (
        SparkSession.builder
        .appName("MumbaiTaxi-SilverGoldReconciliation")
        .getOrCreate()
    )

    try:
        # 1. Read Silver and independently calculate expected Gold metrics.
        print("=== Read Silver and calculate expected metrics ===")

        silver_df = spark.read.parquet(SILVER_PATH)

        expected_df = (
            silver_df
            .groupBy(*KEY_COLUMNS)
            .agg(
                count("*").alias("trip_count"),
                round(spark_sum("fare_inr"), 2).alias("total_revenue"),
                round(avg("fare_inr"), 2).alias("avg_fare"),
                round(avg("distance_km"), 2).alias("avg_distance_km"),
                round(avg("duration_min"), 2).alias("avg_duration_min"),
            )
        )

        expected_count = expected_df.count()
        print(f"Expected aggregate groups from Silver: {expected_count}")

        if expected_count == 0:
            raise RuntimeError(
                "Silver contains no aggregate groups; reconciliation cannot pass"
            )

        # 2. Read the actual published Gold table.
        print(f"=== Read BigQuery Gold: {gold_table} ===")

        actual_df = (
            spark.read
            .format("bigquery")
            .option("table", gold_table)
            .load()
        )

        # 3. Full outer join ensures missing and unexpected groups are detected.
        expected = expected_df.alias("expected")
        actual = actual_df.alias("actual")

        joined_df = expected.join(
            actual,
            on=KEY_COLUMNS,
            how="full_outer",
        )

        expected_count_col = col("expected.trip_count")
        actual_count_col = col("actual.trip_count")

        missing_or_extra = (
            expected_count_col.isNull()
            | actual_count_col.isNull()
        )

        metric_mismatch = (
            expected_count_col.isNotNull()
            & actual_count_col.isNotNull()
            & (expected_count_col != actual_count_col)
        )

        for metric in METRIC_COLUMNS[1:]:
            expected_value = col(f"expected.{metric}")
            actual_value = col(f"actual.{metric}")

            metric_mismatch = metric_mismatch | (
                expected_count_col.isNotNull()
                & actual_count_col.isNotNull()
                & (
                    expected_value.isNull()
                    | actual_value.isNull()
                    | (
                        spark_abs(expected_value - actual_value)
                        > NUMERIC_TOLERANCE
                    )
                )
            )

        mismatches_df = joined_df.filter(
            missing_or_extra | metric_mismatch
        )

        mismatch_count = mismatches_df.count()

        print(f"Actual Gold aggregate groups: {actual_df.count()}")
        print(f"Reconciliation mismatches: {mismatch_count}")

        if mismatch_count > 0:
            print("=== Reconciliation mismatch samples ===")

            (
                mismatches_df
                .select(
                    *KEY_COLUMNS,
                    col("expected.trip_count").alias("expected_trip_count"),
                    col("actual.trip_count").alias("actual_trip_count"),
                    col("expected.total_revenue").alias("expected_revenue"),
                    col("actual.total_revenue").alias("actual_revenue"),
                    col("expected.avg_fare").alias("expected_avg_fare"),
                    col("actual.avg_fare").alias("actual_avg_fare"),
                    col("expected.avg_distance_km").alias(
                        "expected_avg_distance_km"
                    ),
                    col("actual.avg_distance_km").alias(
                        "actual_avg_distance_km"
                    ),
                    col("expected.avg_duration_min").alias(
                        "expected_avg_duration_min"
                    ),
                    col("actual.avg_duration_min").alias(
                        "actual_avg_duration_min"
                    ),
                )
                .show(20, truncate=False)
            )

            raise RuntimeError(
                f"Silver-to-Gold reconciliation failed: "
                f"{mismatch_count} mismatched groups"
            )

        print(
            "SILVER-TO-GOLD RECONCILIATION PASSED: "
            f"{expected_count} groups matched."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
