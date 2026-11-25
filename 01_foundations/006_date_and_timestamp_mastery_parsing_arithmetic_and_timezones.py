"""Scenario & Scale: Global ride-share trip SLA & local peak-hour analytics (80M rides/day).

Business Context:
Parse UTC ISO-8601 pickup/dropoff timestamps, compute trip duration safely under ANSI
mode, and convert UTC events into regional IANA timezones for peak-hour SLA reporting.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "006_Date_Timestamp_Timezone_Mastery") -> SparkSession:
    """Create or reuse a portable SparkSession pinned to UTC session timezone."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1. Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice:
# - Leaving `spark.sql.session.timeZone` at the cluster OS default and adding manual
#   hour offsets (`pickup_ts + F.expr("INTERVAL 5 HOURS")`) or using unsafe `to_timestamp`.
# Otherwise:
# - Manual hour offsets break during Daylight Saving Time (DST) transitions, and
#   unpinned session timezones cause silent 4-8h shifts across multi-region clusters.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Store/parse in UTC, use `try_to_timestamp` & `from_utc_timestamp`.
RIDES_SCHEMA = T.StructType(
    [
        T.StructField("ride_id", T.StringType(), nullable=False),
        T.StructField("pickup_utc", T.StringType(), nullable=True),
        T.StructField("dropoff_utc", T.StringType(), nullable=True),
        T.StructField("tz_city", T.StringType(), nullable=False),
    ]
)


def enrich_ride_timings(df: DataFrame) -> DataFrame:
    """Parse UTC ISO timestamps, compute duration, and project local city time dimensions."""
    pickup_ts = F.try_to_timestamp(F.col("pickup_utc"))
    dropoff_ts = F.try_to_timestamp(F.col("dropoff_utc"))
    duration_sec = F.col("dropoff_ts").cast(T.LongType()) - F.col("pickup_ts").cast(T.LongType())
    pickup_local = F.from_utc_timestamp(F.col("pickup_ts"), F.col("tz_city"))
    pickup_hour = F.hour(pickup_local)

    return (
        df.withColumns({"pickup_ts": pickup_ts, "dropoff_ts": dropoff_ts})
        .withColumns(
            {
                "duration_seconds": duration_sec,
                "duration_minutes": F.round(F.try_divide(duration_sec, F.lit(60.0)), 2),
                "pickup_local": pickup_local,
                "pickup_hour": pickup_hour,
                "day_of_week": F.date_format(pickup_local, "EEEE"),
                "is_peak_hour": pickup_hour.between(7, 9) | pickup_hour.between(17, 19),
            }
        )
    )


def main() -> None:
    spark = build_spark()

    rides_data = [
        ("RIDE-01", "2023-11-15T08:15:00Z", "2023-11-15T08:42:30Z", "America/New_York"),
        ("RIDE-02", "2023-11-15T14:30:00Z", "2023-11-15T15:05:00Z", "Europe/London"),
        ("RIDE-03", "2023-11-15T22:45:00Z", "2023-11-15T23:25:15Z", "Asia/Tokyo"),
        ("RIDE-04", "2023-11-16T01:10:00Z", "2023-11-16T01:35:00Z", "America/Los_Angeles"),
    ]
    df_rides = spark.createDataFrame(rides_data, schema=RIDES_SCHEMA)
    df_local = enrich_ride_timings(df_rides)

    ride1 = df_local.filter(F.col("ride_id") == "RIDE-01").first()
    ride3 = df_local.filter(F.col("ride_id") == "RIDE-03").first()
    assert ride1["duration_minutes"] == 27.5
    assert ride3["pickup_hour"] == 7 and ride3["is_peak_hour"] is True


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_local.explain("formatted")` executes strictly as narrow `Project` expressions
#   with `ConvertToLocalTimestamp` / `FromUTCTimestamp` Catalyst nodes.
# - Verify `spark.sql.session.timeZone` is explicitly `UTC` in the Spark UI Environment tab.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Spark timestamps store UTC epoch microseconds internally; `from_utc_timestamp`
#    shifts the wall-clock representation into a `TIMESTAMP_NTZ`-like local display
#    using IANA rules (DST-aware), whereas `TIMESTAMP_NTZ` avoids timezone shifts.
# 2. Avoid `unix_timestamp()` for sub-second durations; casting `TIMESTAMP` to `BIGINT`
#    or subtracting two timestamps (`TIMESTAMPDIFF(SECOND, a, b)`) is cleaner and ANSI-safe.
# 3. Spark 3.0+ uses the Proleptic Gregorian calendar; legacy Parquet/Avro files written
#    before Spark 3.0 may require `spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED`.

if __name__ == "__main__":
    main()
