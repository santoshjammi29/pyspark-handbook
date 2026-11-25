"""Scenario & Scale: Production ETL crashing with Java heap space OOMs.

Separates Driver OOM vs Executor OOM root causes and provides Spark 4.0 /
Spark Connect-safe mitigations (bounded driver reads & staging lineage cuts).
"""


from pyspark.sql import DataFrame, Row, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Calling `.collect()` / `.toPandas()` on large DataFrames,
# broadcasting multi-GB tables, or chaining 100+ `withColumn()` loops without
# breaking the Catalyst/lineage tree.
# Otherwise: Driver JVM exhausts `spark.driver.memory` (`OutOfMemoryError: Java
# heap space` or `GC overhead limit exceeded`); on workers, huge `collect_list`
# or undersized partitions trigger Executor OOM (`Exit code 137` container kill).
def bad_driver_collect_and_deep_dag(df: DataFrame) -> list[Row]:
    for i in range(150):
        # Deeply nested projection tree causes Catalyst planning/driver OOM
        df = df.withColumn(f"c_{i}", F.col("id") + F.lit(i))
    return df.collect()  # Pulls entire distributed dataset to driver heap!


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Bound driver pulls with `limit(n)` / `take(n)` or
# `toLocalIterator()`, combine projections in a single `select(...)`, and break
# deep iterative DAGs using a staging write/read (portable across Spark Connect
# without JVM checkpoint directory configuration).
def safe_driver_sample(df: DataFrame, max_rows: int = 10) -> list[Row]:
    """Retrieve bounded rows safely without pulling full data to driver."""
    return list(df.limit(max_rows).toLocalIterator())


def truncate_lineage_via_staging(
    spark: SparkSession,
    df: DataFrame,
    staging_uri: str,
) -> DataFrame:
    """Break deep Catalyst logical/physical lineage cleanly via staging I/O."""
    df.write.mode("overwrite").parquet(staging_uri)
    return spark.read.parquet(staging_uri)


def build_oom_diagnostic_matrix(spark: SparkSession) -> DataFrame:
    """Return structured Driver vs Executor OOM triage reference."""
    rows = [
        (
            "Driver OOM",
            "collect() / toPandas() on big DF",
            "Write to sink or bound with limit()/toLocalIterator()",
        ),
        (
            "Driver OOM",
            "Oversized broadcast table (> driver RAM)",
            "Lower autoBroadcastJoinThreshold or disable broadcast hint",
        ),
        (
            "Driver OOM",
            "100+ chained withColumn() deep DAG",
            "Batch into single select() or stage to Parquet/Delta",
        ),
        (
            "Executor OOM",
            "Severe key skew or collect_list(hot_key)",
            "Enable AQE skewJoin, salt key, or use Window/agg",
        ),
        (
            "Executor OOM",
            "Undersized shuffle partitions (>2 GB/task)",
            "Increase spark.sql.shuffle.partitions (~150 MB/task)",
        ),
    ]
    return spark.createDataFrame(
        rows, "failure_type STRING, root_cause STRING, remediation STRING"
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# Spark UI OOM Forensics:
#   - Driver OOM: Driver log shows `java.lang.OutOfMemoryError` during
#     `SparkPlan.executeCollect`, `BroadcastExchange`, or Catalyst rule batching;
#     UI disconnects completely and job never enters or stalls before stage tasks.
#   - Executor OOM: Executors tab shows `ExecutorLostFailure (Container killed
#     by YARN/K8s for exceeding memory limits, exit code 137)` or `GC overhead`;
#     inspect `Peak Execution Memory`, `Spill (Disk)`, and `Memory Overhead`.
#   - `truncate_lineage_via_staging()` resets `explain("formatted")` to a clean
#     single `FileScan parquet` node.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. `Exit code 137` in Kubernetes/YARN is usually OFF-HEAP / container memory
#    breach (`spark.executor.memoryOverhead`), NOT JVM heap—tune off-heap /
#    Arrow Python worker memory (`spark.executor.pyspark.memory`), not just `-Xmx`.
# 2. `df.cache()` preserves the full lineage DAG (recomputing lost partitions on
#    failure), whereas staging to storage or `checkpoint()` truncates lineage.
# 3. Calling `df.withColumn(...)` inside a Python loop builds an $O(N^2)$ Catalyst
#    Schema/Project plan; pass a single `.select(*exprs)` list instead.


if __name__ == "__main__":
    import tempfile

    spark = (
        SparkSession.builder.appName("082_OOMDiagnosisAndMitigation")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    build_oom_diagnostic_matrix(spark).show(truncate=False)
    df_large = spark.range(1, 100_000, 1, 4)
    print("Safe sample IDs:", [r.id for r in safe_driver_sample(df_large, 3)])
    with tempfile.TemporaryDirectory() as tmp_dir:
        df_cut = truncate_lineage_via_staging(spark, df_large, f"{tmp_dir}/stg")
        df_cut.explain("formatted")
