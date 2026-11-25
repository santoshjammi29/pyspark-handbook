"""Scenario & Scale: Financial compliance audit ordering & stratified sampling (500M txns/day).

Business Context:
Produce deterministic top-N compliance queues with explicit null placement and draw
statistically representative stratified samples across imbalanced risk tiers.
"""

import random
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "008_Sorting_Stratified_Sampling") -> SparkSession:
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
# - Calling `df.orderBy("risk_tier", F.desc("amount"))` globally on intermediate
#   steps or sampling imbalanced classes with uniform `df.sample(0.1)`.
# Otherwise:
# - Global `orderBy` triggers a `RangePartitioner` shuffle + sampling job across the
#   entire cluster, which subsequent transformations immediately invalidate.
# - Uniform `sample(0.1)` drops 90% of rare `CRITICAL` fraud cases, starving audits.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Explicit `desc_nulls_last()` only at final sink, `sortWithinPartitions()`,
# and deterministic `sampleBy()` stratified by risk tier.
AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("audit_id", T.StringType(), nullable=False),
        T.StructField("risk_tier", T.StringType(), nullable=False),
        T.StructField("amount", T.DoubleType(), nullable=True),
        T.StructField("org_id", T.StringType(), nullable=False),
    ]
)


def sort_audit_queue(df: DataFrame) -> DataFrame:
    """Sort globally for final reporting with explicit NULL placement at the tail."""
    return df.orderBy(F.col("risk_tier").asc(), F.col("amount").desc_nulls_last())


def stratified_sample(df: DataFrame, fractions: Dict[str, float], seed: int = 42) -> DataFrame:
    """Draw a reproducible stratified sample per risk tier in a single pass."""
    return df.sampleBy("risk_tier", fractions=fractions, seed=seed)


def main() -> None:
    spark = build_spark()

    rng = random.Random(42)
    risk_tiers = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    records = [
        (
            f"AUD-{i:04d}",
            rng.choice(risk_tiers),
            round(rng.uniform(10.0, 50000.0), 2) if i % 10 != 0 else None,
            f"ORG-{rng.randint(1, 5)}",
        )
        for i in range(1, 101)
    ]
    df_audit_logs = spark.createDataFrame(records, schema=AUDIT_SCHEMA)

    df_sorted = sort_audit_queue(df_audit_logs)
    fractions = {"LOW": 0.10, "MEDIUM": 0.30, "HIGH": 0.80, "CRITICAL": 1.00}
    df_stratified = stratified_sample(df_audit_logs, fractions=fractions, seed=42)

    critical_total = df_audit_logs.filter(F.col("risk_tier") == "CRITICAL").count()
    critical_sampled = df_stratified.filter(F.col("risk_tier") == "CRITICAL").count()
    assert critical_sampled == critical_total
    assert df_sorted.count() == 100


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_sorted.explain("formatted")` shows `Exchange rangepartitioning(...)` + `Sort`.
# - For Parquet/Delta clustering inside partitions without a global shuffle, prefer
#   `df.repartition("risk_tier").sortWithinPartitions("amount")` (no `RangePartitioner`).

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `sort()` and `orderBy()` are aliases; both trigger a global `RangePartitioner`
#    shuffle, whereas `sortWithinPartitions()` sorts strictly inside existing partitions.
# 2. `sampleBy()` performs independent Bernoulli coin flips per stratum—actual row counts
#    approximate the fraction; for exact counts per group, use `Window` + `row_number()`.
# 3. In SQL/PySpark ascending sorts default to `NULLS FIRST`, while descending sorts
#    default to `NULLS LAST`; always be explicit with `asc_nulls_last()`/`desc_nulls_last()`.

if __name__ == "__main__":
    main()
