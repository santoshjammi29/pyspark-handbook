"""Scenario & Scale: Streaming platform user genre & feature tagging (120M profiles/day).

Business Context:
Inspect user preference arrays, test membership, safely extract first/last elements
under Spark 4.0 ANSI mode, and compute set unions/intersections without row explosion.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "013_Array_Creation_Extraction_Search") -> SparkSession:
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
#   df.withColumn("second_genre", F.element_at(F.col("genres"), 2))
#   df.withColumn("second_zero_based", F.col("genres")[1])
# Otherwise:
# - In Spark 4.0 ANSI mode (`spark.sql.ansi.enabled=true`), `F.element_at(arr, idx)`
#   and `arr[idx]` raise `INVALID_ARRAY_INDEX` whenever an array has fewer elements
#   than `idx`, crashing the entire job on a single short-array row.


# ---------------------------------------------------------------------------
# 2. ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Use `F.try_element_at` (1-based) or `F.get` (0-based) for safe lookup.
USER_MEDIA_SCHEMA = T.StructType(
    [
        T.StructField("user_id", T.StringType(), nullable=False),
        T.StructField("genres", T.ArrayType(T.StringType()), nullable=False),
        T.StructField("stream_features", T.ArrayType(T.StringType()), nullable=False),
    ]
)


def analyze_user_arrays(df: DataFrame) -> DataFrame:
    """Compute array metrics, ANSI-safe element extraction, and set intersections."""
    recommended = F.array(F.lit("Action"), F.lit("Mystery"), F.lit("Sci-Fi"))

    return df.withColumns(
        {
            "genre_count": F.size(F.col("genres")),
            "likes_action": F.array_contains(F.col("genres"), "Action"),
            "primary_genre": F.try_element_at(F.col("genres"), F.lit(1)),
            "second_genre_safe": F.try_element_at(F.col("genres"), F.lit(2)),
            "last_genre": F.try_element_at(F.col("genres"), F.lit(-1)),
            "sorted_genres": F.array_sort(F.col("genres")),
            "all_genres_union": F.array_distinct(F.array_union(F.col("genres"), recommended)),
            "common_genres": F.array_intersect(F.col("genres"), recommended),
        }
    )


def main() -> None:
    spark = build_spark()

    user_media = [
        ("U-101", ["Sci-Fi", "Action", "Thriller"], ["HD", "Dolby Atmos", "4K"]),
        ("U-102", ["Romance", "Comedy"], ["SD"]),
        ("U-103", ["Documentary"], ["HD", "Subtitles"]),
        ("U-104", ["Action", "Anime", "Fantasy", "Adventure"], ["4K", "HDR10"]),
    ]
    df_users = spark.createDataFrame(user_media, schema=USER_MEDIA_SCHEMA)
    df_result = analyze_user_arrays(df_users)

    u103 = df_result.filter(F.col("user_id") == "U-103").first()
    u101 = df_result.filter(F.col("user_id") == "U-101").first()
    assert u103["genre_count"] == 1
    assert u103["second_genre_safe"] is None
    assert u101["likes_action"] is True and u101["last_genre"] == "Thriller"


# ---------------------------------------------------------------------------
# 3. explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `df_result.explain("formatted")` executes strictly inside a single `Project` on
#   Tungsten `ArrayData` buffers with zero shuffle (`Exchange`) stages.
# - Verify in Spark UI SQL tab that no `Explode` / `Generate` + `HashAggregate` pair
#   is emitted for array set deduplication (`array_union` / `array_distinct`).

# ---------------------------------------------------------------------------
# 4. Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `F.element_at` uses 1-based indexing (`-1` for last), whereas `F.get(arr, 0)` and
#    bracket indexing `arr[0]` use 0-based indexing; under ANSI mode, prefer
#    `F.try_element_at` or `F.get` to return NULL instead of failing on out-of-bounds.
# 2. `F.size(None)` returns `-1` in legacy Spark SQL, or `NULL` depending on config
#    (`spark.sql.legacy.sizeOfNull`); guard nullable arrays with `F.coalesce`.
# 3. `F.array_union` deduplicates elements internally, whereas `F.concat(arr1, arr2)`
#    preserves duplicates and is faster when deduplication is unnecessary.

if __name__ == "__main__":
    main()
