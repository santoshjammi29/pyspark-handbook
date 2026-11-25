"""Scenario & Scale: Airline booking normalization pipeline (40M PNRs/day, 10m SLA).

Business Context:
Normalize nested passenger arrays and multi-leg flight itineraries into relational
Silver rows without silently dropping infant-hold bookings that have empty or NULL
passenger lists.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "015_Explode_Outer_PosExplode_Flatten") -> SparkSession:
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
#   df_bookings.select("booking_id", F.explode("passengers").alias("passenger"))
# Otherwise:
# - Standard `F.explode()` silently drops every parent row where `passengers` is `[]`
#   or `NULL`, causing unflagged financial/passenger reconciliation data loss.
# - Calling `explode()` twice in the same `select()` raises `AnalysisException` or
#   creates Cartesian row multiplication if chained carelessly.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Use `explode_outer()` to retain parent rows, `posexplode_outer()`
# for ordinal leg sequencing, and `flatten()` for 2D nested arrays.
BOOKING_SCHEMA = T.StructType(
    [
        T.StructField("booking_id", T.StringType(), nullable=False),
        T.StructField("airline", T.StringType(), nullable=False),
        T.StructField("flight_legs", T.ArrayType(T.StringType()), nullable=True),
        T.StructField("passengers", T.ArrayType(T.StringType()), nullable=True),
    ]
)


def explode_passengers_preserving_bookings(df: DataFrame) -> DataFrame:
    """Normalize passenger arrays without dropping empty or NULL passenger bookings."""
    return df.select(
        "booking_id",
        "airline",
        F.explode_outer("passengers").alias("passenger"),
    )


def extract_ordered_flight_legs(df: DataFrame) -> DataFrame:
    """Assign 0-based leg sequence ordinals while preserving bookings without legs."""
    return df.select(
        "booking_id",
        "airline",
        F.posexplode_outer("flight_legs").alias("leg_seq", "airport"),
    )


def main() -> None:
    spark = build_spark()

    bookings = [
        ("BKG-001", "Delta", ["ATL", "JFK", "LHR"], ["Alice", "Bob"]),
        ("BKG-002", "United", ["ORD", "FRA"], ["Charlie"]),
        ("BKG-003", "Lufthansa", ["MUC"], []),
        ("BKG-004", "British Airways", ["LHR", "SIN"], None),
    ]
    df_bookings = spark.createDataFrame(bookings, schema=BOOKING_SCHEMA)

    df_passengers = explode_passengers_preserving_bookings(df_bookings)
    df_legs = extract_ordered_flight_legs(df_bookings)

    matrix_schema = T.StructType(
        [
            T.StructField("id", T.StringType(), nullable=False),
            T.StructField("matrix", T.ArrayType(T.ArrayType(T.StringType())), nullable=False),
        ]
    )
    df_nested = spark.createDataFrame(
        [("TEST", [["A", "B"], ["C", "D", "E"]])],
        schema=matrix_schema,
    )
    df_flat = df_nested.withColumn("flat_matrix", F.flatten("matrix"))

    assert df_passengers.select("booking_id").distinct().count() == 4
    assert df_passengers.count() == 5
    assert df_legs.filter(F.col("booking_id") == "BKG-001").count() == 3
    assert df_flat.first()["flat_matrix"] == ["A", "B", "C", "D", "E"]


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_passengers.explain("formatted")` emits a `Generate explode(passengers), true`
#   operator (`true` indicates outer semantics).
# - Watch Spark UI Stage Task `Records Written / Shuffle Write`: exploding giant arrays
#   (100K+ elements per row) can trigger skew and Executor OOM on a single task.

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Never chain two `explode` calls on independent array columns in one DataFrame
#    without realizing it produces a cross product ($M \times N$ rows per parent record).
# 2. If you only need to filter, transform, or sum elements inside an array, use
#    Higher-Order Functions (`F.transform`, `F.filter`) instead of `explode`.
# 3. `F.flatten` collapses only one nesting level (`ARRAY<ARRAY<T>>` -> `ARRAY<T>`);
#    apply it repeatedly if deeper array nesting exists.

if __name__ == "__main__":
    main()
