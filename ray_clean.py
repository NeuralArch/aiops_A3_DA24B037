"""
ray_clean.py
DA3408 A3 - Ray pipeline (mirrors spark_clean.py stage-for-stage)
"""

import argparse
import time

import pandas as pd
import ray

TRIP_PICKUP_TS = "tpep_pickup_datetime"
TRIP_DROPOFF_TS = "tpep_dropoff_datetime"
TRIP_DISTANCE_COL = "trip_distance"
PU_LOCATION_ID = "PULocationID"
DO_LOCATION_ID = "DOLocationID"
LOC_ID_COL = "LocationID"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--locations", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--format", default="csv", choices=["csv", "parquet"])
    p.add_argument("--address", default="auto")
    return p.parse_args()


def clean_batch(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("VendorID", "RatecodeID", "PULocationID", "DOLocationID", "payment_type"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df[TRIP_PICKUP_TS] = pd.to_datetime(df[TRIP_PICKUP_TS], errors="coerce")
    df[TRIP_DROPOFF_TS] = pd.to_datetime(df[TRIP_DROPOFF_TS], errors="coerce")
    df[TRIP_DISTANCE_COL] = pd.to_numeric(df[TRIP_DISTANCE_COL], errors="coerce")

    required = [TRIP_PICKUP_TS, TRIP_DROPOFF_TS, TRIP_DISTANCE_COL, PU_LOCATION_ID, DO_LOCATION_ID]
    df = df.dropna(subset=required)
    df = df.drop_duplicates()
    df = df[df[TRIP_DISTANCE_COL] > 0]
    return df


def add_avg_speed(df: pd.DataFrame) -> pd.DataFrame:
    elapsed_seconds = (df[TRIP_DROPOFF_TS] - df[TRIP_PICKUP_TS]).dt.total_seconds()
    hours = elapsed_seconds.where(elapsed_seconds > 0)
    df["avg_speed_mph"] = df[TRIP_DISTANCE_COL] / (hours / 3600.0)
    return df


def main():
    args = parse_args()
    ray.init(address=args.address)

    t0 = time.time()

    read_fn = ray.data.read_csv if args.format == "csv" else ray.data.read_parquet
    trips = read_fn(args.input)
    locations = read_fn(args.locations)

    _ = trips.count()
    ingest_time = time.time() - t0
    print(f"[TIMING] Ingestion: {ingest_time:.2f}s")

    t1 = time.time()
    trips_clean = trips.map_batches(clean_batch, batch_format="pandas")
    _ = trips_clean.count()
    clean_time = time.time() - t1
    print(f"[TIMING] Cleansing: {clean_time:.2f}s")

    t2 = time.time()
    locations_renamed = locations.map_batches(
        lambda df: df.rename(columns={LOC_ID_COL: PU_LOCATION_ID}), batch_format="pandas"
    )
    joined = trips_clean.join(
        locations_renamed,
        join_type="inner",
        num_partitions=8,
        on=(PU_LOCATION_ID,),
    )

    t_udf0 = time.time()
    result = joined.map_batches(add_avg_speed, batch_format="pandas")
    _ = result.sum("avg_speed_mph")
    udf_time = time.time() - t_udf0
    print(f"[TIMING] UDF (avg speed) evaluation: {udf_time:.2f}s")

    transform_time = time.time() - t2
    print(f"[TIMING] Transformation (total, incl. UDF): {transform_time:.2f}s")

    t3 = time.time()
    result.write_parquet(args.output)
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

    ray.shutdown()


if __name__ == "__main__":
    main()
