"""
093 — Lakeflow Declarative Pipelines (Delta Live Tables / DLT) & Expectations.

Scenario & Scale:
    An enterprise financial lakehouse processes millions of daily invoices across
    Bronze -> Silver -> Gold. Manual imperative job chains require brittle retry
    bookkeeping and silently drop malformed records without data-quality observability.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: Silent Dropna() & Manual Imperative Checkpoint Wiring
# ==============================================================================
# ❌ Bad Practice:
#   df.dropna(subset=["invoice_no", "amount"]).filter("amount > 0").write.mode("append").save(...)
#
# Otherwise:
#   Silent filtering hides upstream schema/value regressions, gives zero audit metrics
#   on how many rows failed quality checks, and risks corrupting downstream Gold KPIs.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: DLT Expectations & Quarantine Pattern
# ==============================================================================
# ✅ Good Practice: Declare `@dlt.expect*` quality contracts in Lakeflow Pipelines
#                   and model equivalent single-pass quarantine routing in pure PySpark.
LAKEFLOW_DLT_BLUEPRINT = """
import dlt
from pyspark.sql import functions as F

@dlt.table(comment="Raw streaming invoices ingested via Auto Loader")
def invoices_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/enterprise/raw/invoices/")
    )

@dlt.table(comment="Cleansed invoices passing quality contracts")
@dlt.expect("valid_invoice_no", "invoice_no IS NOT NULL")
@dlt.expect_or_drop("valid_positive_amount", "amount > 0")
@dlt.expect_or_fail("valid_status", "status IN ('APPROVED', 'PENDING', 'REJECTED')")
def invoices_silver():
    return dlt.read_stream("invoices_raw").select("invoice_no", "amount", "status")
""".strip()


def evaluate_dlt_expectations_portably(df):
    """Emulate DLT expect / expect_or_drop / expect_or_fail + quarantine in OSS PySpark."""
    from pyspark.sql import functions as F

    valid_statuses = ["APPROVED", "PENDING", "REJECTED"]

    # 1. expect_or_fail check (abort if fatal domain invariant is violated)
    fatal_count = df.filter(~F.col("status").isin(valid_statuses) | F.col("status").isNull()).count()
    if fatal_count > 0:
        raise ValueError(f"expect_or_fail('valid_status') aborted pipeline: {fatal_count} invalid rows.")

    # 2. Tag expectation violations in a single pass for audit + quarantine
    df_tagged = df.withColumn(
        "_dq_drop_reason",
        F.when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("invalid_positive_amount")),
    ).withColumn(
        "_dq_warn_flag",
        F.col("invoice_no").isNull(),
    )

    df_clean = df_tagged.filter(F.col("_dq_drop_reason").isNull()).drop("_dq_drop_reason")
    df_quarantine = df_tagged.filter(F.col("_dq_drop_reason").isNotNull())
    return df_clean, df_quarantine


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df_clean.explain("formatted")`, verify expectation predicates fold into a
#   single `Filter` operator above the source scan (`isnotnull(amount) AND amount > 0`).
# - In Databricks Lakeflow / DLT UI, inspect the Lineage Graph node badges for
#   `Dropped` vs `Warned` expectation counts persisted in the DLT event log.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: `@dlt.expect` logs violations and keeps rows; `@dlt.expect_or_drop`
#   discards failing rows silently unless you add a parallel quarantine table with
#   inverted predicates (`expect_all_or_drop` vs quarantine).
# - Gotcha 2: Streaming Tables (`dlt.read_stream`) process append-only increments once;
#   Materialized Views (`dlt.read`) incrementally recompute aggregations/joins.
# - Gotcha 3: Never pass `--` or unvalidated expressions into expectation strings;
#   keep expectations as pure, deterministic SQL boolean predicates for pushdown.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_093_Lakeflow_DLT").getOrCreate()
    print(LAKEFLOW_DLT_BLUEPRINT)

    batch = [
        ("INV-01", 100.0, "APPROVED"),
        ("INV-02", -50.0, "APPROVED"),
        (None, 250.0, "PENDING"),
    ]
    df_in = spark.createDataFrame(batch, "invoice_no STRING, amount DOUBLE, status STRING")
    clean_df, quarantine_df = evaluate_dlt_expectations_portably(df_in)
    clean_df.show(truncate=False)
    quarantine_df.show(truncate=False)
