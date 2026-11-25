"""Slowly Changing Dimension Type 2 (SCD Type 2) via Single-Pass Delta MERGE.

Scenario & Scale:
    An insurance underwriting platform tracks 40M policyholder address histories.
    Claims require exact point-in-time address lookups; daily 200k-row updates
    must atomically expire prior versions and insert active rows in < 5 minutes.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Collecting changed keys to the driver (`df.collect()`) and
# issuing row-by-row `dt_scd2.update(...)` calls followed by a separate append.
# Otherwise: Driver OOM on large batches, N+1 sequential Delta commits (breaking
# atomicity), and partial-failure window where a customer has zero active rows.
def apply_scd2_driver_loop_bad(dt_scd2, df_to_expire: DataFrame) -> None:
    for row in df_to_expire.collect():  # Driver OOM & N sequential Delta transactions
        dt_scd2.update(
            condition=F.col("sk_id") == row.sk_id,
            set={"end_date": F.lit(row.change_date), "is_current": F.lit(False)},
        )


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Build a staged union DataFrame (`merge_key` = natural key
# for expiring existing active rows, `merge_key = NULL` for inserting new active
# versions) and commit atomically in ONE Delta `MERGE INTO` operation.
def transform_stage_scd2_updates(
    df_current_dim: DataFrame,
    df_incoming: DataFrame,
) -> DataFrame:
    """Produce staged SCD2 upsert payload for a single-pass atomic Delta MERGE."""
    df_active = df_current_dim.filter(F.col("is_current") == F.lit(True))

    # Rows whose tracked attributes actually changed -> must expire old + insert new
    df_changed = (
        df_incoming.alias("s")
        .join(
            df_active.alias("t"),
            (F.col("s.cust_id") == F.col("t.cust_id")) & (F.col("s.city") != F.col("t.city")),
            how="inner",
        )
        .select(
            F.col("s.cust_id").alias("cust_id"),
            F.col("s.name").alias("name"),
            F.col("s.city").alias("city"),
            F.col("s.change_date").alias("change_date"),
        )
    )

    # New customers not yet in dimension -> insert directly
    df_brand_new = df_incoming.join(df_active, on="cust_id", how="left_anti")

    # 1. Records with merge_key = cust_id match active row to expire it
    df_expire_rows = df_changed.withColumn("merge_key", F.col("cust_id"))

    # 2. Records with merge_key = NULL bypass match condition -> trigger INSERT
    df_insert_rows = df_changed.unionByName(df_brand_new).withColumn(
        "merge_key", F.lit(None).cast("string")
    )

    return df_expire_rows.unionByName(df_insert_rows).withColumn(
        "sk_id",
        F.xxhash64(F.col("cust_id"), F.col("change_date"), F.col("city")),
    )


def apply_scd2_atomic_merge(
    spark: SparkSession, scd2_uri: str, df_staged: DataFrame
) -> None:
    """Atomically expire previous active rows and insert new SCD2 versions."""
    from delta.tables import DeltaTable

    dt_scd2 = DeltaTable.forPath(spark, scd2_uri)
    (
        dt_scd2.alias("target")
        .merge(
            source=df_staged.alias("source"),
            condition="target.cust_id = source.merge_key AND target.is_current = true",
        )
        .whenMatchedUpdate(
            set={
                "end_date": F.col("source.change_date"),
                "is_current": F.lit(False),
            }
        )
        .whenNotMatchedInsert(
            values={
                "sk_id": F.col("source.sk_id"),
                "cust_id": F.col("source.cust_id"),
                "name": F.col("source.name"),
                "city": F.col("source.city"),
                "start_date": F.col("source.change_date"),
                "end_date": F.lit("9999-12-31"),
                "is_current": F.lit(True),
            }
        )
        .execute()
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` on `transform_stage_scd2_updates` shows one inner join
#   and one `left_anti` join; for huge dimensions, broadcast `df_incoming` when
#   delta batch < 100 MB to avoid shuffling the full 40M-row active dimension.
# - The single `MERGE` produces exactly ONE Delta commit in `DESCRIBE HISTORY`.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. The `merge_key = NULL` union trick is the canonical Databricks/Delta SCD2
#    idiom: `NULL = target.cust_id` never matches, forcing `whenNotMatchedInsert`
#    for the new active version in the exact same ACID transaction as expiration.
# 2. Point-in-time joins (`fact.txn_date >= dim.start_date AND fact.txn_date < dim.end_date`)
#    should use half-open `[start_date, end_date)` intervals so boundary-day
#    transactions never double-match two SCD2 versions.
# 3. Always partition or cluster SCD2 tables by `is_current` (or `ZORDER BY (cust_id)`)
#    so active-record merges skip 95%+ of cold historical Parquet files.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("069_scd_type_2_staged_merge")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    df_cur = spark.createDataFrame(
        [(1, "C-101", "Alice", "Austin", "2025-01-01", "9999-12-31", True)],
        "sk_id LONG, cust_id STRING, name STRING, city STRING, start_date STRING, end_date STRING, is_current BOOLEAN",
    )
    df_in = spark.createDataFrame(
        [("C-101", "Alice", "Denver", "2025-02-01"), ("C-103", "Cara", "Boston", "2025-02-01")],
        "cust_id STRING, name STRING, city STRING, change_date STRING",
    )
    transform_stage_scd2_updates(df_cur, df_in).show(truncate=False)
