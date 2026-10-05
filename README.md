# DA3408 A3: Spark vs. Ray — The Data Engineering Duel

An identical data preprocessing pipeline implemented in Apache Spark (PySpark) and Ray (Ray Data), benchmarked against NYC Yellow Taxi trip data on a 2-node cluster for each framework.

## Contents

| File | Description |
|---|---|
| `spark_clean.py` | PySpark pipeline: ingest → cleanse → join + UDF → export |
| `ray_clean.py` | Ray Data pipeline mirroring the same four stages |
| `AI_DISCLOSURE.md` | Disclosure of AI assistance used, per course Code of Conduct |
| `DA3408_A3_Benchmark_Report.pdf` | Full benchmark report with results, screenshots, and analysis |

## Pipeline

Both scripts implement the same four stages against the same schema:

1. **Ingestion** — read monthly Parquet trip files + a taxi zone lookup table
2. **Cleansing** — drop nulls, de-duplicate, parse timestamps, filter invalid distances
3. **Transformation** — inner join trips → pickup locations; custom Python UDF computing average speed (`trip_distance / elapsed_hours`)
4. **Export** — write the result to Parquet

### Schema fix

NYC TLC monthly files store `VendorID` (and related ID columns) as `INT32` in some months and `INT64` in others. Both pipelines read each file individually and explicitly cast these columns to a consistent type before joining/unioning, to avoid Spark's directory-level reader failing on the mismatch.

## Running

### Spark

Requires a running Spark cluster (see `docker-spark-cluster` setup separately):

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1G --executor-memory 1G \
  spark_clean.py \
  --input /path/to/trips \
  --locations /path/to/locations.parquet \
  --output /path/to/output_spark \
  --format parquet
```

### Ray

Requires a running Ray cluster (`ray start --head`, then workers join via `ray start --address=...`):

```bash
python ray_clean.py \
  --address auto \
  --input /path/to/trips \
  --locations /path/to/locations.parquet \
  --output /path/to/output_ray \
  --format parquet
```

Both scripts print `[TIMING]` lines for each stage and a final `[SUMMARY]` block with machine-readable `key=value` pairs.

## Results Summary

| Metric | Spark | Ray |
|---|---|---|
| Dataset size | 315 MB | 107 MB |
| Cluster | 2 workers (Docker) | 2 nodes (local), final run single-node |
| Total wall-clock | 1630.96 s | 279.38 s |
| UDF evaluation time | 581.62 s | 116.85 s |

Full stage-by-stage breakdown, cluster screenshots, and discussion of a hardware memory constraint encountered during testing are in `DA3408_A3_Benchmark_Report.pdf`.

## Note on Dataset Sizes

The two benchmark runs use different input sizes. The development VM (5.76 GB RAM) could not run Ray's 2-worker cluster on the assignment's target ~2 GB dataset without repeated `OutOfMemoryError` failures, even after reducing to 315 MB. The largest size at which each framework reliably completed was used for its respective benchmark. This constraint and its implications are discussed in the report.

## AI Use

See `AI_DISCLOSURE.md` for a full breakdown of where AI assistance was used (logic translation, debugging, report drafting) and what was not AI-generated (all benchmark numbers, cluster setup, screenshots).
