# AI Disclosure — DA3408 A3: Spark vs. Ray

**Student:** DA24B037
**Assignment:** A3 — Spark vs. Ray: The Data Engineering Duel
**AI Tool Used:** Claude (Anthropic)

This document discloses how AI assistance was used in this assignment, per the course's Code of Conduct on Fair Use of AI.

## What AI Was Used For

### 1. Logic Translation (explicitly permitted)
The initial `spark_clean.py` and `ray_clean.py` pipeline structures — mirroring the same four stages (ingestion, cleansing, transformation/UDF, export) across both frameworks — were drafted with AI assistance, translating the same cleaning/transformation logic between PySpark and Ray Data APIs.

### 2. Debugging Assistance
AI was used to diagnose and fix two code-level issues encountered during development, not to generate results:

- **Schema mismatch across monthly files:** NYC TLC Parquet files store `VendorID` (and related ID columns) as `INT32` in some months and `INT64`/bigint in others, which crashed Spark's directory-level reader. AI helped diagnose the root cause and suggested the fix: read each file individually, explicitly cast the affected columns to a consistent type, then union — applied identically to both the Spark and Ray pipelines to preserve pipeline parity.
- **Ray API incompatibility:** `Dataset.num_blocks()` is unavailable on a lazy (non-materialized) Ray Dataset in the installed Ray version. AI suggested replacing it with a fixed `num_partitions` value for the join.

### 3. Infrastructure Troubleshooting
AI assisted with general environment setup and debugging throughout: Java/PySpark/Ray installation, Docker build issues (a deprecated base image in the cluster repo's Dockerfile), disk space management, and diagnosing repeated Ray `OutOfMemoryError` failures caused by the development VM's limited RAM (5.76 GB). All cluster configuration and network setup (starting the Spark Docker Compose cluster, starting and joining Ray head/worker nodes) was performed manually by the student, per the assignment's Code of Conduct.

### 4. Report Drafting
The benchmark report (LaTeX/PDF) was drafted with AI assistance, structuring the content around the rubric categories. All performance numbers, screenshots, and timing data embedded in the report were taken directly from actual terminal output and the Spark UI / Ray Dashboard — none were AI-generated or estimated.

## What Was NOT AI-Generated

- **No benchmark numbers were synthesized.** Every execution time, memory figure, and row count in the report was captured from real `spark-submit` and `python ray_clean.py` runs on the student's machine, cross-verified against the Spark UI (port 4040/9090) and Ray Dashboard (port 8265).
- **Cluster orchestration and networking** (Docker Compose, `ray start --head`, worker joins) were performed manually by the student.
- **Screenshots** are genuine captures from the student's own running cluster, not generated or edited.

## Performance Tuning Note

As required by the Code of Conduct, the specific AI-suggested optimization affecting pipeline results is documented here: the per-file schema-casting fix (Section 3, main report) was necessary for *both* pipelines to run at all on the real dataset — without it, Spark's bulk directory read failed outright on mixed-schema monthly files. This was a correctness fix, not a performance optimization, and was applied identically to both frameworks to preserve a fair comparison.
