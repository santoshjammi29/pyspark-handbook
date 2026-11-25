"""
Module 04 / 002: ANSI Mode Default & Spark 3.5 -> 4.0 Migration Gotchas

Scenario & Scale:
    A payments lakehouse is upgrading 400+ ETL pipelines from Spark 3.5 to
    Spark 4.0 across 120 TB of daily clickstream and settlement events. In
    Spark 4.0, `spark.sql.ansi.enabled` defaults to `true` (previously `false`),
    converting silent NULL coercions into hard runtime exceptions.

Goal:
    Identify and remediate the four classic Spark 3.5 -> 4.0 ANSI migration
    failures (invalid cast, divide-by-zero, numeric overflow, out-of-bounds
    array indexing) using `try_*` functions and explicit quarantine routing.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark() -> SparkSession:
    """Build an ANSI-enabled Spark 4.0+ session."""
    return (
        SparkSession.builder.appName("002_ANSI_Mode_Spark4_Migration")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: Relying on Spark 3.5 permissive silent NULL semantics
# ---------------------------------------------------------------------------
def anti_pattern_spark35_permissive(df: DataFrame) -> DataFrame:
    # ❌ Bad Practice: Using strict cast, `/`, `+` on INT, and 0-based indexing on
    # uncleaned ingestion data while assuming Spark will silently return NULL.
    # Otherwise: In Spark 4.0 (ANSI=true by default), a single malformed row across
    # 5 billion records aborts the entire job with CAST_INVALID_INPUT (22018),
    # DIVIDE_BY_ZERO (22012), ARITHMETIC_OVERFLOW (22003), or INVALID_ARRAY_INDEX (22003).
    return (
        df.withColumn("qty", F.col("raw_qty").cast(T.IntegerType()))
        .withColumn("unit_price", F.col("total_cents") / F.col("item_count"))
        .withColumn("next_qty", F.col("qty") + F.lit(2_147_483_647))
        .withColumn("first_tag", F.col("tags")[0])
    )


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: `try_*` expressions + DLQ quarantine split
# ---------------------------------------------------------------------------
def good_practice_ansi_safe(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    # ✅ Good Practice: Use `try_cast`, `try_divide`, `try_add` (widened to BIGINT),
    # and 1-based `try_element_at` at the Bronze -> Silver boundary, routing invalid
    # records into an explicit quarantine DataFrame instead of failing or dropping silently.
    staged = (
        df.withColumn("qty", F.try_cast(F.col("raw_qty"), T.IntegerType()))
        .withColumn(
            "unit_price",
            F.try_divide(F.col("total_cents").cast(T.DecimalType(18, 2)), F.col("item_count")),
        )
        .withColumn(
            "next_qty",
            F.try_add(F.col("qty").cast(T.LongType()), F.lit(1).cast(T.LongType())),
        )
        .withColumn("first_tag", F.try_element_at(F.col("tags"), F.lit(1)))
    )

    is_corrupt = (F.col("raw_qty").isNotNull() & F.col("qty").isNull()) | (
        F.col("item_count") == F.lit(0)
    )
    silver_valid = staged.filter(~is_corrupt)
    quarantine_dlq = staged.filter(is_corrupt).withColumn(
        "dq_reason",
        F.when(F.col("qty").isNull(), F.lit("INVALID_INT_CAST"))
        .when(F.col("item_count") == F.lit(0), F.lit("ZERO_DENOMINATOR"))
        .otherwise(F.lit("UNKNOWN")),
    )
    return silver_valid, quarantine_dlq


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - In `explain("formatted")`, ANSI-safe expressions render explicitly in the `Project`
#   operator as `try_cast(raw_qty#... as int)`, `try_divide(...)`, `try_add(...)`, and
#   `try_element_at(tags#..., 1)` rather than `CheckOverflow` / `cast(... as int)`.
# - When an unguarded pipeline fails under Spark 4.0, the Spark UI SQL/Stages tab
#   surfaces exact ANSI `SQLSTATE` codes (e.g., `[CAST_INVALID_INPUT]`, `[DIVIDE_BY_ZERO]`)
#   and the failing Spark task attempt pinpointing the exact partition/input file.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Never Flip `spark.sql.ansi.enabled=false` Permanently: Disabling ANSI globally masks
#    silent arithmetic truncation and invalid casts that corrupt financial Gold tables;
#    fix Bronze -> Silver ingestion boundaries with `try_*` and quarantine tables instead.
# 2. 1-Based `try_element_at` vs 0-Based `col[0]`: `F.try_element_at(arr, 1)` returns the
#    first element (and `NULL` when out of bounds), whereas `col[0]` is 0-based and raises
#    `INVALID_ARRAY_INDEX` under ANSI mode when `size(arr) == 0`.
# 3. Reserved Keywords in Spark 4.0 ANSI SQL: Identifiers such as `user`, `order`, `date`,
#    and `table` are reserved under ANSI SQL and must be backtick-quoted (`` `order` ``).
# 4. Aggregate Overflow in `SUM` / `AVG`: Summing `INT` or narrow `DECIMAL` columns across
#    billions of rows overflows under ANSI mode; widen to `BIGINT` / `DECIMAL(38, s)` or
#    use `F.try_sum(col)` where null-on-overflow is acceptable.


if __name__ == "__main__":
    spark = build_spark()
    try:
        schema = T.StructType(
            [
                T.StructField("raw_qty", T.StringType(), True),
                T.StructField("total_cents", T.LongType(), True),
                T.StructField("item_count", T.LongType(), True),
                T.StructField("tags", T.ArrayType(T.StringType()), True),
            ]
        )
        rows = [
            ("10", 5000, 5, ["promo", "vip"]),
            ("N/A", 1200, 2, []),
            ("4", 900, 0, ["rush"]),
        ]
        raw_df = spark.createDataFrame(rows, schema=schema)
        clean_df, dlq_df = good_practice_ansi_safe(raw_df)
        clean_df.show(truncate=False)
        dlq_df.show(truncate=False)
    finally:
        spark.stop()
