"""Scenario & Scale: Fleet IoT diagnostics alert filter (1B sensor pings/day, 2m SLA).

Business Context:
Identify critical vehicle health alerts across target territories while ensuring
filter predicates push down cleanly into columnar Parquet/Delta row-group statistics.
"""

from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "004_Filtering_Predicate_Pushdown") -> SparkSession:
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
# - Wrapping partition/data columns in functions inside filters:
#   `df.filter(F.upper(F.col("region")) == "NORTH AMERICA")` or
#   `df.filter(F.col("region") == "Europe" and F.col("engine_temp") > 105)`
# Otherwise:
# - Python `and`/`or`/`not` raise `PySparkValueError: Cannot convert column into bool`.
# - Wrapping columns in `F.upper()` or `F.year()` blocks Parquet/Delta min-max
#   `PushedFilters` and partition pruning, forcing a full-table scan across petabytes.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Parenthesized bitwise predicates (`&`, `|`, `~`) + pushdown-friendly bounds.
FLEET_SCHEMA = T.StructType(
    [
        T.StructField("vehicle_id", T.StringType(), nullable=False),
        T.StructField("region", T.StringType(), nullable=False),
        T.StructField("engine_temp", T.DoubleType(), nullable=False),
        T.StructField("tire_pressure", T.DoubleType(), nullable=False),
        T.StructField("battery_pct", T.DoubleType(), nullable=False),
        T.StructField("alert_code", T.StringType(), nullable=False),
    ]
)


def filter_severe_alerts(df: DataFrame, target_regions: Sequence[str]) -> DataFrame:
    """Filter severe fleet alerts with pushdown-compatible column predicates."""
    severe_filter = (
        F.col("region").isin(list(target_regions))
        & (
            (F.col("engine_temp") > 105.0)
            | (F.col("tire_pressure") < 28.0)
            | (F.col("battery_pct") < 60.0)
        )
        & (F.col("alert_code") != "NORMAL")
    )
    return df.filter(severe_filter)


def filter_critical_regex(df: DataFrame) -> DataFrame:
    """Match critical alert prefixes using native JVM regex pushable filter."""
    return df.filter(F.col("alert_code").rlike(r"^CRITICAL_"))


def main() -> None:
    spark = build_spark()

    telemetry = [
        ("V-01", "North America", 102.5, 32.0, 94.0, "WARN_TEMP"),
        ("V-02", "Europe", 88.0, 33.5, 98.0, "NORMAL"),
        ("V-03", "North America", 115.0, 26.0, 65.0, "CRITICAL_PRESSURE"),
        ("V-04", "Asia Pacific", 92.5, 31.0, 45.0, "WARN_BATTERY"),
        ("V-05", "Europe", 110.2, 27.5, 50.0, "CRITICAL_TEMP_PRESSURE"),
        ("V-06", "Latin America", 85.0, 34.0, 99.0, "NORMAL"),
    ]
    df_fleet = spark.createDataFrame(telemetry, schema=FLEET_SCHEMA)

    df_severe = filter_severe_alerts(df_fleet, ["North America", "Europe"])
    df_critical = filter_critical_regex(df_fleet)

    assert [r["vehicle_id"] for r in df_severe.orderBy("vehicle_id").collect()] == ["V-03", "V-05"]
    assert df_critical.count() == 2


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - On Parquet/Delta sources, inspect `df.explain("formatted")` for `PartitionFilters`
#   and `PushedFilters: [IsNotNull(region), In(region, ...)]`.
# - In Spark UI (SQL tab -> FileScan node), compare `number of files pruned` and
#   `rows output` vs `scan time` to verify data skipping is active.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always parenthesize every comparison around `&`, `|`, `~` because Python evaluates
#    bitwise operators at higher precedence than `==`, `<`, `>`.
# 2. Watch out for NULLs with `!= "NORMAL"` or `~col.isin(...)`: SQL three-valued logic
#    evaluates `NULL != 'NORMAL'` to `UNKNOWN` (filtered out); add `.isNull()` if needed.
# 3. To preserve pushdown on date/timestamp columns, rewrite `F.year("ts") == 2025` as
#    a half-open range `("2025-01-01" <= ts) & (ts < "2026-01-01")`.

if __name__ == "__main__":
    main()
