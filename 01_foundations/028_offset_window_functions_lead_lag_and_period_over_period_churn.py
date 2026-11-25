"""Scenario & Scale: SaaS MRR expansion, contraction & churn analytics (30M subs/month).

Business Context:
Compute month-over-month MRR variance, safe percentage growth under Spark 4.0 ANSI
mode, and subscription lifecycle tags using `lag()` / `lead()` without self-joins.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "028_Offset_Windows_Lead_Lag_Churn") -> SparkSession:
    """Create or reuse a portable SparkSession with ANSI mode enabled."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1. Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice:
# - Self-joining `df_subs` on `sub_id` and `month == prev_month + 1` to compute MoM
#   change, and dividing `(mrr - prev_mrr) / prev_mrr` without zero guards.
# Otherwise:
# - Self-joins require 2x table scans and a heavy `SortMergeJoin` shuffle, whereas
#   `lag()` runs in a single $O(N)$ partition scan.
# - Dividing by `prev_mrr == 0` raises `DIVIDE_BY_ZERO` under Spark 4.0 ANSI mode.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Single-pass `lag()` / `lead()` + `F.try_divide()` for zero-safe growth.
SUBSCRIPTION_SCHEMA = T.StructType(
    [
        T.StructField("sub_id", T.StringType(), nullable=False),
        T.StructField("billing_month", T.StringType(), nullable=False),
        T.StructField("mrr", T.DoubleType(), nullable=False),
    ]
)


def classify_mrr_movements(df: DataFrame) -> DataFrame:
    """Compute MoM MRR deltas, ANSI-safe growth percentages, and churn classifications."""
    window_sub = Window.partitionBy("sub_id").orderBy("billing_month")

    df_offsets = df.withColumns(
        {
            "prev_mrr": F.lag("mrr", 1, default=0.0).over(window_sub),
            "next_mrr": F.lead("mrr", 1, default=0.0).over(window_sub),
        }
    )

    delta = F.col("mrr") - F.col("prev_mrr")
    pct_change = F.when(F.col("prev_mrr") == 0.0, F.lit(None).cast(T.DoubleType())).otherwise(
        F.round(F.try_divide(delta, F.col("prev_mrr")) * F.lit(100.0), 1)
    )

    return df_offsets.withColumns(
        {
            "mrr_change": F.round(delta, 2),
            "pct_change": pct_change,
            "status_tag": (
                F.when(F.col("prev_mrr") == 0.0, "NEW")
                .when(F.col("mrr") > F.col("prev_mrr"), "EXPANSION")
                .when((F.col("mrr") < F.col("prev_mrr")) & (F.col("mrr") > 0.0), "CONTRACTION")
                .when(F.col("mrr") == 0.0, "CHURNED")
                .otherwise("RETAINED")
            ),
        }
    )


def main() -> None:
    spark = build_spark()

    subscription_history = [
        ("SUB-1", "2023-01", 100.0),
        ("SUB-1", "2023-02", 100.0),
        ("SUB-1", "2023-03", 150.0),
        ("SUB-1", "2023-04", 120.0),
        ("SUB-2", "2023-01", 50.0),
        ("SUB-2", "2023-02", 50.0),
        ("SUB-2", "2023-03", 0.0),
    ]
    df_subs = spark.createDataFrame(subscription_history, schema=SUBSCRIPTION_SCHEMA)
    df_result = classify_mrr_movements(df_subs)

    sub1_mar = df_result.filter(
        (F.col("sub_id") == "SUB-1") & (F.col("billing_month") == "2023-03")
    ).first()
    sub2_mar = df_result.filter(
        (F.col("sub_id") == "SUB-2") & (F.col("billing_month") == "2023-03")
    ).first()
    assert sub1_mar["status_tag"] == "EXPANSION" and sub1_mar["pct_change"] == 50.0
    assert sub2_mar["status_tag"] == "CHURNED" and sub2_mar["mrr_change"] == -50.0


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_result.explain("formatted")` fuses `lag` and `lead` into a single `Window`
#   stage after one `Exchange hashpartitioning(sub_id)` and `Sort`.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `lag(col, offset, default)` ignores NULLs ONLY if `ignoreNulls=True` is set
#    (Spark 3.0+); otherwise, a NULL row in the preceding offset returns NULL.
# 2. `lag` operates on physical row offsets, NOT calendar intervals—if a customer skipped
#    February (`2023-01` -> `2023-03`), `lag(..., 1)` reads January unless you scaffold
#    missing calendar months first.
# 3. Use `F.try_divide(a, b)` whenever denominators can be `0` or `0.0` in Spark 4.0+
#    ANSI mode so transient zero-revenue records return NULL instead of failing jobs.

if __name__ == "__main__":
    main()
