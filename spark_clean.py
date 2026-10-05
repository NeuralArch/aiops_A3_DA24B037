"""
spark_clean.py
DA3408 A3 - Spark pipeline

Pipeline:
  1. Ingest CSV/Parquet trip files + a location lookup table
  2. Cleanse: drop nulls, de-dup, parse timestamps
  3. Transform: join trips -> locations, compute avg speed per hour via UDF
  4. Export: write result as Parquet

Run (after the master/workers are up):
  /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 1G --executor-memory 1G \
      spark_clean.py --input /opt/spark-data/trips --locations /opt/spark-data/locations.csv \
      --output /opt/spark-data/output_spark
"""

import argparse
import glob
import os
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, LongType

# ---- Config: adjust to match your actual column names ----------------
TRIP_PICKUP_TS = "tpep_pickup_datetime"
TRIP_DROPOFF_TS = "tpep_dropoff_datetime"
TRIP_DISTANCE_COL = "trip_distance"
PU_LOCATION_ID = "PULocationID"
DO_LOCATION_ID = "DOLocationID"
LOC_ID_COL = "LocationID"
# ------------------------------------------------------------------------


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Directory of trip CSV/Parquet files")
    p.add_argument("--locations", required=True, help="Location lookup CSV/Parquet")
    p.add_argument("--output", required=True, help="Output Parquet path")
    p.add_argument("--format", default="csv", choices=["csv", "parquet"])
    return p.parse_args()


def avg_speed_mph(distance_miles, pickup_ts, dropoff_ts):
    """Custom Python UDF: average speed = distance / elapsed hours."""
    if distance_miles is None or pickup_ts is None or dropoff_ts is None:
        return None
    elapsed_seconds = (dropoff_ts - pickup_ts).total_seconds()
    if elapsed_seconds <= 0:
        return None
    hours = elapsed_seconds / 3600.0
    return float(distance_miles) / hours


def read_trips_parquet(spark, input_dir):
    """
    NYC TLC monthly parquet files don't share a consistent physical schema —
    e.g. VendorID is stored as INT32 in some months and INT64/bigint in others.
    Spark's directory-level reader (and even mergeSchema) can't reconcile this
    at the column-encoding level, so we read each file separately, normalize
    known mismatched columns, and union the results.
    """
    files = sorted(glob.glob(os.path.join(input_dir, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No .parquet files found under {input_dir}")

    frames = []
    for f in files:
        df = spark.read.parquet(f)
        # Normalize columns known to vary in physical type across monthly files
        if "VendorID" in df.columns:
            df = df.withColumn("VendorID", F.col("VendorID").cast(LongType()))
        if "RatecodeID" in df.columns:
            df = df.withColumn("RatecodeID", F.col("RatecodeID").cast(LongType()))
        if "PULocationID" in df.columns:
            df = df.withColumn("PULocationID", F.col("PULocationID").cast(LongType()))
        if "DOLocationID" in df.columns:
            df = df.withColumn("DOLocationID", F.col("DOLocationID").cast(LongType()))
        if "payment_type" in df.columns:
            df = df.withColumn("payment_type", F.col("payment_type").cast(LongType()))
        frames.append(df)

    result = frames[0]
    for df in frames[1:]:
        result = result.unionByName(df, allowMissingColumns=True)
    return result


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("SparkVsRay-Cleaning")
        .getOrCreate()
    )

    t0 = time.time()

    # ---- 1. Ingestion ----
    if args.format == "csv":
        trips = spark.read.option("header", True).csv(args.input)
        locations = spark.read.option("header", True).csv(args.locations)
    else:
        trips = read_trips_parquet(spark, args.input)
        locations = spark.read.parquet(args.locations)

    ingest_time = time.time() - t0
    print(f"[TIMING] Ingestion: {ingest_time:.2f}s")

    # ---- 2. Cleansing ----
    t1 = time.time()

    trips = trips.withColumn(TRIP_PICKUP_TS, F.to_timestamp(TRIP_PICKUP_TS))
    trips = trips.withColumn(TRIP_DROPOFF_TS, F.to_timestamp(TRIP_DROPOFF_TS))
    trips = trips.withColumn(TRIP_DISTANCE_COL, F.col(TRIP_DISTANCE_COL).cast(DoubleType()))

    trips_clean = (
        trips.dropna(
            subset=[TRIP_PICKUP_TS, TRIP_DROPOFF_TS, TRIP_DISTANCE_COL, PU_LOCATION_ID, DO_LOCATION_ID]
        )
        .dropDuplicates()
        .filter(F.col(TRIP_DISTANCE_COL) > 0)
    )

    clean_time = time.time() - t1
    print(f"[TIMING] Cleansing: {clean_time:.2f}s")

    # ---- 3. Transformation ----
    t2 = time.time()

    # Heavy join: trips -> pickup location names
    joined = trips_clean.join(
        locations.withColumnRenamed(LOC_ID_COL, PU_LOCATION_ID),
        on=PU_LOCATION_ID,
        how="inner",
    )

    # UDF timing isolated separately below; register UDF
    avg_speed_udf = F.udf(avg_speed_mph, DoubleType())

    t_udf0 = time.time()
    result = joined.withColumn(
        "avg_speed_mph",
        avg_speed_udf(F.col(TRIP_DISTANCE_COL), F.col(TRIP_PICKUP_TS), F.col(TRIP_DROPOFF_TS)),
    )
    # Force evaluation of the UDF column specifically to measure its cost
    result.select(F.sum("avg_speed_mph")).collect()
    udf_time = time.time() - t_udf0
    print(f"[TIMING] UDF (avg speed) evaluation: {udf_time:.2f}s")

    transform_time = time.time() - t2
    print(f"[TIMING] Transformation (total, incl. UDF): {transform_time:.2f}s")

    # ---- 4. Export ----
    t3 = time.time()
    result.write.mode("overwrite").parquet(args.output)
    export_time = time.time() - t3
    print(f"[TIMING] Export: {export_time:.2f}s")

    total_time = time.time() - t0
    print(f"[TIMING] TOTAL wall-clock: {total_time:.2f}s")

    print("\n[SUMMARY]")
    print(f"ingestion_s={ingest_time:.2f}")
    print(f"cleansing_s={clean_time:.2f}")
    print(f"transformation_s={transform_time:.2f}")
    print(f"udf_s={udf_time:.2f}")
    print(f"export_s={export_time:.2f}")
    print(f"total_s={total_time:.2f}")

    spark.stop()


if __name__ == "__main__":
    main()