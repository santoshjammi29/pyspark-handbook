"""Slowly Changing Dimension Type 1 (SCD Type 1) via Delta MERGE INTO.

Scenario & Scale:
    A customer master dimension (25M rows) stores current contact email and
    city. Under privacy and business policy, updates overwrite prior values in
    place via nightly 500k-row CDC batches within a 10-minute SLA.
"""

from __future__ import annotations

import tempfile

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Merging un-deduplicated CDC batches or using `mode("overwrite")`
# by joining old + new tables manually.
# Otherwise: Duplicate `cust_id` rows in the source batch crash Delta `MERGE`
# (`UnsupportedOperationException: Cannot perform Merge as multiple source rows
# matched`), while full-table overwrites rewrite terabytes unnecessarily.
def merge_scd1_unsafe_bad(target_table, df_cdc: DataFrame) -> None:
    target_table.alias("t").merge(df_cdc.alias("s"), "t.cust_id = s.cust_id").whenMatchedUpdateAll().execute()


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Deterministically deduplicate inbound CDC by `last_updated`,
# then execute an idempotent Delta `MERGE` updating only when attributes differ.
def transform_prepare_scd1_source(df_cdc: DataFrame) -> DataFrame:
    """Deduplicate inbound CDC feed to latest event per natural key."""
    df_typed = df_cdc.withColumn(
        "last_updated_ts",
        F.try_to_timestamp(F.col("last_updated"), F.lit("yyyy-MM-dd")),
    )
    win = Window.partitionBy("cust_id").orderBy(F.col("last_updated_ts").desc_nulls_last())
    return (
        df_typed.withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "last_updated_ts")
    )


def apply_scd1_delta_merge(
    spark: SparkSession, target_uri: str, df_cdc_clean: DataFrame
) -> None:
    """Execute idempotent SCD Type 1 in-place upsert against a Delta table."""
    from delta.tables import DeltaTable

    dt_target = DeltaTable.forPath(spark, target_uri)
    change_guard = (
        "target.full_name <> source.full_name OR "
        "target.email <> source.email OR "
        "target.city <> source.city"
    )
    (
        dt_target.alias("target")
        .merge(source=df_cdc_clean.alias("source"), condition="target.cust_id = source.cust_id")
        .whenMatchedUpdate(
            condition=change_guard,
            set={
                "full_name": F.col("source.full_name"),
                "email": F.col("source.email"),
                "city": F.col("source.city"),
                "last_updated": F.col("source.last_updated"),
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Delta `MERGE` runs two internal phases in Spark UI: (1) file/partition pruning
#   join to locate touched Parquet files, and (2) copy-on-write (or Deletion
#   Vector) rewrite of only matched files.
# - Adding `condition=change_guard` prevents rewriting Parquet files for no-op
#   unchanged rows in replayed batches.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always guard `whenMatchedUpdate` with `IS DISTINCT FROM` / change-hash
#    comparison (`target.row_hash <> source.row_hash`) so nullable columns don't
#    break `<>` comparisons and unchanged rows don't cause write amplification.
# 2. Enable Delta Deletion Vectors (`delta.enableDeletionVectors = true`) on
#    large SCD1 tables to turn small updates into fast merge-on-read marks.
# 3. SCD Type 1 still retains historical versions briefly in Delta Time Travel
#    until `VACUUM`; run `VACUUM` if PII erasure is legally required.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("068_scd_type_1")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    df_cdc = spark.createDataFrame(
        [
            ("C-01", "Alice Smith", "alice@old.com", "Austin", "2025-01-01"),
            ("C-01", "Alice Smith", "alice@new.com", "Denver", "2025-01-20"),
            ("C-03", "Charlie Day", "charlie@bar.com", "Philly", "2025-01-20"),
        ],
        "cust_id STRING, full_name STRING, email STRING, city STRING, last_updated STRING",
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        transform_prepare_scd1_source(df_cdc).write.format("parquet").mode("overwrite").save(tmp_dir)
        spark.read.parquet(tmp_dir).orderBy("cust_id").show(truncate=False)
