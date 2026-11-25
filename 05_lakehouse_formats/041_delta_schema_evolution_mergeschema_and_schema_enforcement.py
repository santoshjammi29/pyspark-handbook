"""
Module 05 / 041: Delta Schema Evolution (mergeSchema), Enforcement & Column Mapping

Scenario & Scale:
    An e-commerce product catalog ingests 20M SKU feeds/day where upstream
    microservices periodically introduce new attributes (`warranty_months`,
    `energy_rating`) or rename legacy fields. Uncontrolled writes either crash
    nightly pipelines or silently corrupt downstream ML feature schemas.

Goal:
    Leverage Delta Schema Enforcement by default, selectively evolve additive
    nullable columns with `mergeSchema=true`, and enable Delta Column Mapping
    (`name` mode) for zero-rewrite column renames and drops.
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    """Create an ANSI-enabled Spark 4.0+ session with Delta Lake."""
    return (
        SparkSession.builder.appName("041_Delta_Schema_Evolution")
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
# (2) Anti-Pattern: Blind `overwriteSchema=true` or global autoMerge in prod
# ---------------------------------------------------------------------------
def anti_pattern_blind_overwrite_schema(df_drifted: DataFrame, delta_uri: str) -> None:
    # ❌ Bad Practice: Setting `.option("overwriteSchema", "true")` with `mode("overwrite")`
    # or enabling `spark.databricks.delta.schema.autoMerge.enabled=true` cluster-wide.
    # Otherwise: Upstream typos (`prcie` instead of `price`) or breaking type widens
    # silently mutate production Gold table schemas, poisoning downstream BI views and
    # ML inference pipelines without raising an alert.
    (
        df_drifted.write.format("delta")
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .save(delta_uri)
    )


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Enforcement Guard + Scoped `mergeSchema`
# ---------------------------------------------------------------------------
def good_practice_schema_evolution(spark: SparkSession, delta_dir: Path) -> None:
    # ✅ Good Practice: Keep strict Schema Enforcement enabled at write time, catch
    # unexpected drift explicitly, and allow additive evolution (`mergeSchema=true`)
    # only on vetted Bronze/Silver ingestion steps—plus Column Mapping for renames.
    target_uri = str(delta_dir)

    baseline = [("P-100", "Laptop", "899.99")]
    base_df = spark.createDataFrame(baseline, "product_id STRING, name STRING, price STRING").withColumn(
        "price", F.try_cast(F.col("price"), "DECIMAL(10,2)")
    )
    (
        base_df.write.format("delta")
        .option("delta.columnMapping.mode", "name")
        .mode("overwrite")
        .save(target_uri)
    )

    incoming = [("P-200", "Smart TV", "450.00", 24, "A++")]
    extra_df = spark.createDataFrame(
        incoming,
        "product_id STRING, name STRING, price STRING, warranty_months INT, energy_rating STRING",
    ).withColumn("price", F.try_cast(F.col("price"), "DECIMAL(10,2)"))

    # Step A: Default write enforces schema and rejects unapproved columns atomically.
    try:
        extra_df.write.format("delta").mode("append").save(target_uri)
    except Exception as exc:
        print(f"Schema enforcement blocked unexpected drift: {str(exc).splitlines()[0]}")

    # Step B: Explicit, scoped `mergeSchema=true` for approved additive columns.
    (
        extra_df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .save(target_uri)
    )

    evolved_df = spark.read.format("delta").load(target_uri)
    evolved_df.printSchema()
    evolved_df.orderBy("product_id").show(truncate=False)


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - After additive `mergeSchema=true`, `evolved_df.explain("formatted")` shows a
#   `FileScan parquet` across all historical and new files; Delta resolves missing
#   columns in older Parquet files as `NULL` at scan time without rewriting old data.
# - When `delta.columnMapping.mode='name'` is active, physical Parquet columns use
#   stable internal UUIDs (`delta.columnMapping.physicalName`), visible in `FileScan`
#   metadata while logical column renames/drops execute as O(1) `_delta_log` updates.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `mergeSchema` Cannot Narrow or Incompatible-Cast Types: Adding new nullable columns
#    or safe widening (`INT -> BIGINT`) succeeds, whereas `STRING -> INT` fails schema
#    enforcement—cast/clean explicitly before writing under ANSI mode.
# 2. Column Mapping Tradeoff: Enabling `delta.columnMapping.mode='name'` unlocks
#    `ALTER TABLE ... RENAME COLUMN` and `DROP COLUMN` without rewriting terabytes, but
#    upgrades table reader/writer protocol versions (`minReaderVersion >= 2`).
# 3. `overwriteSchema` Danger: `overwriteSchema=true` only works with `mode("overwrite")`
#    (or `replaceWhere`) and drops missing columns from the table schema—never use it
#    on incremental append pipelines.


if __name__ == "__main__":
    spark = build_spark()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            good_practice_schema_evolution(spark, Path(tmp_dir) / "delta_schema")
    finally:
        spark.stop()
