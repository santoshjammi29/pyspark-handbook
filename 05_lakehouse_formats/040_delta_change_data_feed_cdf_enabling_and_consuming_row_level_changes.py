"""
Module 05 / 040: Delta Change Data Feed (CDF): Enabling & Consuming Row-Level Changes

Scenario & Scale:
    A financial risk engine must propagate incremental inserts, updates, and
    deletes from a 5B-row Silver customer ledger into downstream Gold feature
    stores and regulatory reporting tables every 15 minutes without re-scanning
    all 5B rows.

Goal:
    Enable `delta.enableChangeDataFeed=true`, emit commits (`UPDATE`, `DELETE`,
    `INSERT`), and consume `readChangeFeed` streams handling `_change_type`
    (`insert`, `update_preimage`, `update_postimage`, `delete`) idempotently.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("040_Delta_Change_Data_Feed")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: Full-table daily anti-join diffs across multi-TB snapshots
# ---------------------------------------------------------------------------
def anti_pattern_full_table_diff(
    spark: SparkSession, prev_uri: str, curr_uri: str
) -> DataFrame:
    # ❌ Bad Practice: Diffing yesterday's vs today's entire 5B-row Delta table via
    # `full_outer` join (`curr.join(prev, "cust_id", "full_outer")`) to detect CDC rows.
    # Otherwise: Every 15-minute cycle shuffles terabytes of unchanged cold rows across
    # executors, explodes shuffle spill, and misses intra-window multi-update sequences.
    prev_df = spark.read.format("delta").load(prev_uri)
    curr_df = spark.read.format("delta").load(curr_uri)
    return curr_df.join(prev_df, on="cust_id", how="left_anti")


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Delta Change Data Feed (CDF) Consumption
# ---------------------------------------------------------------------------
def good_practice_cdf_pipeline(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Enable `delta.enableChangeDataFeed=true` on the source Delta
    # table and read bounded commit deltas via `.option("readChangeFeed", "true")`.
    target_uri = str(delta_dir)

    init_customers = [
        ("C-01", "Alice", "1000.00"),
        ("C-02", "Bob", "2500.00"),
    ]
    init_df = spark.createDataFrame(
        init_customers, "cust_id STRING, name STRING, credit_limit STRING"
    ).withColumn("credit_limit", F.try_cast(F.col("credit_limit"), "DECIMAL(12,2)"))

    (
        init_df.write.format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .save(target_uri)
    )

    dt = DeltaTable.forPath(spark, target_uri)
    dt.update(condition=F.col("cust_id") == F.lit("C-01"), set={"credit_limit": F.lit(1500.00)})
    dt.delete(condition=F.col("cust_id") == F.lit("C-02"))

    new_cust = spark.createDataFrame([("C-03", "Charlie", "5000.00")], "cust_id STRING, name STRING, credit_limit STRING").withColumn(
        "credit_limit", F.try_cast(F.col("credit_limit"), "DECIMAL(12,2)")
    )
    new_cust.write.format("delta").mode("append").save(target_uri)

    cdf_df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .load(target_uri)
    )
    cdf_df.select(
        "cust_id",
        "name",
        "credit_limit",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ).orderBy("_commit_version", "cust_id", "_change_type").show(truncate=False)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - In `explain("formatted")`, `readChangeFeed` generates a `FileScan` reading only the
#   incremental `_change_data/` Parquet files (for updates/deletes) and newly added
#   commit files (for pure inserts)—zero full-table shuffle or untouched-file scans.
# - In the Spark UI SQL tab, verify `number of files read` matches only the commits
#   between `startingVersion` and `endingVersion`, not total table file count.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Preimage vs Postimage Semantics: An `UPDATE` emits two rows (`update_preimage` and
#    `update_postimage`); downstream `MERGE` pipelines must either filter out
#    `update_preimage` or deduplicate by `_commit_version` DESC per primary key.
# 2. Pure Inserts Reuse Data Files: When an operation only inserts (`APPEND`), Delta
#    does NOT duplicate rows into `_change_data/`; CDF reads the commit's `add` files
#    directly with negligible storage overhead.
# 3. `VACUUM` Coupling: CDF history is bound to table file retention—running `VACUUM`
#    removes `_change_data/` files older than retention, breaking late CDF consumers.
# 4. Streaming `skipChangeCommits` vs CDF: Standard `readStream` fails on `UPDATE`/`DELETE`
#    unless `ignoreChanges` / `skipChangeCommits` is set, whereas `readChangeFeed=true`
#    processes row-level mutations cleanly as an append stream.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_cdf_pipeline(spark, Path(tmp_dir) / "delta_cdf")
    finally:
        spark.stop()
