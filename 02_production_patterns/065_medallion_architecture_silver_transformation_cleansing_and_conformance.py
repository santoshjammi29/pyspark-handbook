"""Medallion Architecture: Silver Cleansing, Conformance & Deduplication.

Scenario & Scale:
    Bronze invoices (50M rows/day) contain duplicate updates, unstandardized
    status strings (`appr`, `APPROVED`), comma-formatted amounts (`1,500.00`),
    and negative invalid rows. Silver must emit clean, typed, deduplicated records.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Using strict `.cast("double")` / `F.to_timestamp(...)` on
# dirty Bronze strings and dropping duplicates with non-deterministic `dropDuplicates`.
# Otherwise: Under PySpark 4.0 ANSI mode, a single malformed string aborts the
# entire job (`SparkNumberFormatException`), and `dropDuplicates(["invoice_no"])`
# keeps an arbitrary row version rather than the latest update.
def transform_silver_bad(df_bronze: DataFrame) -> DataFrame:
    return (
        df_bronze.withColumn("amount", F.col("raw_amount").cast("double"))
        .dropDuplicates(["invoice_no"])
    )


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Cleanse via `try_cast` / `try_to_timestamp`, route failed
# quality checks to quarantine, and deduplicate deterministically by `updated_at`.
def transform_cleanse_silver_invoices(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return `(valid_silver_df, quarantine_df)` cleanly under ANSI mode."""
    df_typed = (
        df_bronze.withColumn(
            "invoice_amount",
            F.try_cast(F.regexp_replace(F.col("raw_amount"), r"[^\d.-]", ""), T.DecimalType(14, 2)),
        )
        .withColumn(
            "invoice_status",
            F.when(F.upper(F.trim(F.col("raw_status"))).isin("APPR", "APPROVED"), F.lit("APPROVED"))
            .when(F.upper(F.trim(F.col("raw_status"))).isin("PEND", "PENDING"), F.lit("PENDING"))
            .when(F.upper(F.trim(F.col("raw_status"))).isin("REJ", "REJECTED"), F.lit("REJECTED"))
            .otherwise(F.upper(F.trim(F.col("raw_status")))),
        )
        .withColumn(
            "last_updated_at",
            F.try_to_timestamp(F.col("updated_at"), F.lit("yyyy-MM-dd HH:mm:ss")),
        )
    )

    valid_cond = (
        F.col("invoice_no").isNotNull()
        & F.col("invoice_amount").isNotNull()
        & (F.col("invoice_amount") > F.lit(0))
        & F.col("last_updated_at").isNotNull()
    )

    df_quarantine = df_typed.filter(~valid_cond).withColumn(
        "_quarantine_reason", F.lit("FAILED_TYPE_OR_POSITIVE_AMOUNT_CHECK")
    )

    win_dedup = Window.partitionBy("invoice_no").orderBy(F.col("last_updated_at").desc())
    df_silver = (
        df_typed.filter(valid_cond)
        .withColumn("_rn", F.row_number().over(win_dedup))
        .filter(F.col("_rn") == 1)
        .select(
            "invoice_no",
            "vendor_id",
            "invoice_amount",
            "invoice_status",
            "last_updated_at",
            F.current_timestamp().alias("_silver_processed_at"),
        )
    )
    return df_silver, df_quarantine


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `Window` (`row_number`) -> `Filter (_rn = 1)`
#   compiled into Tungsten `WindowExec` after `Exchange hashpartitioning(invoice_no)`.
# - Note that running both `df_silver` and `df_quarantine` triggers two scans of
#   `df_typed` unless persisted or written via Delta multi-output / `checkpoint`.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `dropDuplicates(["key"])` retains the first row encountered per partition—its
#    output changes across runs; always use `Window.partitionBy(...).orderBy(...)`.
# 2. Use `DecimalType(14, 2)` instead of `DoubleType` for monetary amounts in
#    Silver to avoid IEEE-754 binary floating-point rounding discrepancies.
# 3. Never silently drop failed quality rows; route them to a quarantine table
#    with `_quarantine_reason` and alert if error rate exceeds threshold.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("065_medallion_silver")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    rows = [
        ("INV-1001", "V-01", "1,500.00", "appr", "2025-01-20 10:00:00"),
        ("INV-1001", "V-01", "1,500.00", "APPROVED", "2025-01-20 10:05:00"),
        ("INV-1002", "V-02", "420.50", "PENDING", "2025-01-20 10:01:00"),
        ("INV-1003", "V-03", "-50.00", "REJECTED", "2025-01-20 10:02:00"),
    ]
    df_in = spark.createDataFrame(
        rows, "invoice_no STRING, vendor_id STRING, raw_amount STRING, raw_status STRING, updated_at STRING"
    )
    silver_df, quarantine_df = transform_cleanse_silver_invoices(df_in)
    silver_df.show(truncate=False)
    quarantine_df.show(truncate=False)
