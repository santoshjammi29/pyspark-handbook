"""Scenario & Scale: Fintech loyalty points engine (90M account updates/day, 8m SLA).

Business Context:
Transform, filter, and reduce per-account reward point arrays inline inside JVM
Tungsten memory without multiplying row counts via `explode()` + `groupBy()`.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "014_Higher_Order_Functions") -> SparkSession:
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
#   df.select("account_id", F.explode("point_events").alias("pt"))
#     .filter(F.col("pt") >= 50)
#     .groupBy("account_id").agg(F.sum("pt"))
# Otherwise:
# - `explode()` multiplies intermediate row volume (10x-100x) and forces a full
#   `Exchange` shuffle for `groupBy()`, risking shuffle spill and skew OOMs.
# - Accounts whose elements are all filtered out (`< 50`) are silently dropped.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Native JVM Higher-Order Functions (`transform`, `filter`, `aggregate`,
# `exists`, `forall`) with zero row multiplication or shuffles.
LOYALTY_SCHEMA = T.StructType(
    [
        T.StructField("account_id", T.StringType(), nullable=False),
        T.StructField("point_events", T.ArrayType(T.IntegerType()), nullable=False),
    ]
)


def compute_loyalty_metrics(df: DataFrame) -> DataFrame:
    """Apply ANSI-safe higher-order lambda transformations inline on point arrays."""
    return df.withColumns(
        {
            "bonus_points": F.transform(
                F.col("point_events"),
                lambda x: F.try_cast(x * F.lit(1.10), T.IntegerType()),
            ),
            "significant_events": F.filter(
                F.col("point_events"),
                lambda x: x >= F.lit(50),
            ),
            "total_points": F.aggregate(
                F.col("point_events"),
                F.lit(0),
                lambda acc, x: acc + x,
            ),
            "has_super_bonus": F.exists(
                F.col("point_events"),
                lambda x: x >= F.lit(500),
            ),
            "all_positive": F.forall(
                F.col("point_events"),
                lambda x: x > F.lit(0),
            ),
        }
    )


def main() -> None:
    spark = build_spark()

    loyalty_data = [
        ("ACC-1", [150, 45, 800, 20, 310]),
        ("ACC-2", [50, 25, 10]),
        ("ACC-3", [1200, 450, 90, 850]),
        ("ACC-4", [5, 12, 18]),
    ]
    df_loyalty = spark.createDataFrame(loyalty_data, schema=LOYALTY_SCHEMA)
    df_result = compute_loyalty_metrics(df_loyalty)

    acc1 = df_result.filter(F.col("account_id") == "ACC-1").first()
    acc4 = df_result.filter(F.col("account_id") == "ACC-4").first()
    assert acc1["total_points"] == 1325
    assert acc1["has_super_bonus"] is True
    assert acc4["significant_events"] == [] and acc4["all_positive"] is True


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_result.explain("formatted")` shows a single narrow `Project` with `ArrayTransform`,
#   `ArrayFilter`, `ArrayAggregate`, `ArrayExists`, and `ArrayForAll`—zero `Generate`
#   and zero `Exchange` shuffle nodes.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. PySpark lambda functions (`F.transform`, `F.filter`, `F.aggregate`) compile into
#    Catalyst JVM expressions—arbitrary Python libraries inside the lambda will fail
#    because the lambda is traced symbolically, not executed as a Python UDF.
# 2. If the input array column itself is `NULL`, higher-order functions return `NULL`;
#    wrap nullable arrays with `F.coalesce(F.col("arr"), F.array())` when empty list is wanted.
# 3. `F.aggregate` supports an optional 4th `finish` lambda (`merge -> finish`), e.g. to
#    compute an array average in a single pass without a second traversal.

if __name__ == "__main__":
    main()
