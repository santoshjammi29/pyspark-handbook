"""Project IoT: Industrial Telemetry Anomaly Detection via Moving Z-Score.

Scenario & Scale:
    A smart factory monitors 5,000 welding arms emitting 100M+ vibration
    readings/day. Predictive maintenance computes per-machine moving baselines
    and flags |Z| >= 2.5 sigma anomalies without zero-variance divide crashes.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Using static vibration thresholds or dividing `(x - mean) / stddev`
# directly when a machine's window has identical readings (`stddev == 0`).
# Otherwise: Static thresholds trigger false alarms across load/temp shifts, and
# `stddev == 0` raises `SparkArithmeticException: DIVIDE_BY_ZERO` in ANSI mode.
def transform_zscore_unsafe_bad(df_stats: DataFrame) -> DataFrame:
    return df_stats.withColumn("z", (F.col("vibration_mm") - F.col("moving_avg")) / F.col("moving_stddev"))


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Compute rolling per-machine baselines via `Window.rowsBetween`,
# divide safely with `F.try_divide`, and guard `moving_stddev == 0` -> `0.0`.
def transform_detect_vibration_anomalies(
    df_sensors: DataFrame,
    window_lookback_rows: int = 4,
    z_threshold: float = 2.0,
) -> DataFrame:
    """Compute moving Z-scores per machine safely under PySpark 4.0 ANSI mode."""
    df_typed = (
        df_sensors.withColumn("ts", F.try_to_timestamp(F.col("reading_time"), F.lit("yyyy-MM-dd HH:mm:ss")))
        .withColumn("vibration_val", F.try_cast(F.col("vibration_mm"), T.DoubleType()))
        .filter(F.col("ts").isNotNull() & F.col("vibration_val").isNotNull())
    )

    win_machine = (
        Window.partitionBy("machine_id")
        .orderBy("ts")
        .rowsBetween(-window_lookback_rows, Window.currentRow)
    )

    df_stats = (
        df_typed.withColumn("moving_avg", F.avg("vibration_val").over(win_machine))
        .withColumn("moving_stddev", F.stddev("vibration_val").over(win_machine))
    )

    raw_z = F.try_divide(
        F.abs(F.col("vibration_val") - F.col("moving_avg")),
        F.when(F.col("moving_stddev") > F.lit(0.0), F.col("moving_stddev")),
    )

    return (
        df_stats.withColumn("moving_avg", F.round("moving_avg", 2))
        .withColumn("moving_stddev", F.round("moving_stddev", 2))
        .withColumn("z_score", F.round(F.coalesce(raw_z, F.lit(0.0)), 2))
        .withColumn(
            "maintenance_alert",
            F.when(F.col("z_score") >= F.lit(z_threshold), F.lit("CRITICAL: BEARING_FATIGUE_PREDICTED"))
            .otherwise(F.lit("NORMAL")),
        )
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `Window` with `ROWS BETWEEN 4 PRECEDING AND
#   CURRENT ROW` partitioned by `machine_id`, executing in streaming buffer mode
#   across sorted per-machine partitions with minimal memory footprint.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Include only *preceding* readings (`rowsBetween(-20, -1)`) if a single huge
#    spike skews the current window's own mean and stddev (masking the outlier!).
# 2. `F.stddev()` returns `NULL` for a 1-row window and `0.0` for constant
#    readings; `F.try_divide` + `coalesce(..., 0.0)` handles both cleanly in ANSI mode.
# 3. For high-frequency irregular sensor streams, switch from `rowsBetween` to
#    time-based `rangeBetween(-3600, 0)` over `ts.cast("long")`.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("101_iot_zscore_anomalies")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    readings = [
        ("ARM-01", "2025-01-20 10:00:00", 12.1),
        ("ARM-01", "2025-01-20 10:01:00", 12.3),
        ("ARM-01", "2025-01-20 10:02:00", 12.0),
        ("ARM-01", "2025-01-20 10:03:00", 12.2),
        ("ARM-01", "2025-01-20 10:04:00", 28.5),
        ("ARM-02", "2025-01-20 10:00:00", 8.0),
    ]
    df_in = spark.createDataFrame(readings, "machine_id STRING, reading_time STRING, vibration_mm DOUBLE")
    transform_detect_vibration_anomalies(df_in).select(
        "machine_id", "reading_time", "vibration_mm", "moving_avg", "z_score", "maintenance_alert"
    ).show(truncate=False)
