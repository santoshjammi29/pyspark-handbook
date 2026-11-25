"""Slowly Changing Dimensions Type 3 & Type 4 Multi-Tier Implementation.

Scenario & Scale:
    A sales commission engine tracks territory moves for 500k reps. Analysts
    compare current vs immediately prior territory (SCD3), while compliance
    queries full multi-year reassignment history in a separate archive (SCD4).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Mixing unbounded history rows into the hot lookup dimension
# when 99% of joins only need current state, or chaining `prev_1`, `prev_2`...
# Otherwise: Hot operational joins scan bloated historical partitions, while
# ad-hoc `prev_N` columns require constant DDL migrations.
def transform_scd3_unbounded_bad(df: DataFrame) -> DataFrame:
    return df.withColumn("prev_territory_2", F.col("previous_territory"))  # Schema sprawl


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Use SCD Type 3 (`current_*` / `previous_*`) for bounded
# 1-step side-by-side comparisons, and SCD Type 4 (hot current table + append-only
# history table) when rapid current lookups + full audit history are both needed.
def transform_apply_scd3_reassignment(
    df_dim: DataFrame, df_moves: DataFrame
) -> DataFrame:
    """Shift `current_territory` -> `previous_territory` on territory changes."""
    return (
        df_dim.alias("d")
        .join(df_moves.alias("m"), on="rep_id", how="left")
        .select(
            F.col("d.rep_id"),
            F.col("d.rep_name"),
            F.when(
                F.col("m.new_territory").isNotNull()
                & (F.col("m.new_territory") != F.col("d.current_territory")),
                F.col("m.new_territory"),
            )
            .otherwise(F.col("d.current_territory"))
            .alias("current_territory"),
            F.when(
                F.col("m.new_territory").isNotNull()
                & (F.col("m.new_territory") != F.col("d.current_territory")),
                F.col("d.current_territory"),
            )
            .otherwise(F.col("d.previous_territory"))
            .alias("previous_territory"),
            F.try_to_date(
                F.coalesce(F.col("m.effective_date"), F.col("d.last_reassigned")),
                "yyyy-MM-dd",
            ).alias("last_reassigned"),
        )
    )


def transform_build_scd4_tiers(
    df_updated_dim: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Split output into (1) fast SCD4 active table and (2) append-only audit log."""
    df_active = df_updated_dim.select("rep_id", "rep_name", "current_territory")
    df_history_delta = df_updated_dim.select(
        "rep_id",
        F.col("current_territory").alias("territory"),
        F.col("last_reassigned").alias("effective_from"),
        F.current_timestamp().alias("_archived_at"),
    )
    return df_active, df_history_delta


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `transform_apply_scd3_reassignment` triggers a single left join; broadcast
#   `df_moves` when daily reassignment volume < 100 MB (`F.broadcast(df_moves)`).
# - In SCD Type 4, queries against `df_active` avoid `is_current = true` filter
#   overhead and maintain 1 row per business key for Broadcast Hash Joins.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. SCD Type 3 only retains 1 prior snapshot (`previous_territory`); never use
#    it if regulators require a complete chronological audit trail.
# 2. SCD Type 4 is ideal for high-churn dimensions (10+ updates/day/entity)
#    where SCD Type 2 would explode dimension size by 100x and hurt BI joins.
# 3. Keep SCD4 active + history writes coordinated via Delta Change Data Feed
#    (CDF) or atomic multi-table orchestration so history never misses an update.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("070_scd_type_3_and_4")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    df_dim = spark.createDataFrame(
        [("REP-01", "Alice", "West", None, "2025-01-01"), ("REP-02", "Bob", "East", None, "2025-01-01")],
        "rep_id STRING, rep_name STRING, current_territory STRING, previous_territory STRING, last_reassigned STRING",
    )
    df_moves = spark.createDataFrame(
        [("REP-01", "North", "2025-02-01")],
        "rep_id STRING, new_territory STRING, effective_date STRING",
    )
    df_scd3 = transform_apply_scd3_reassignment(df_dim, df_moves)
    df_active, df_hist = transform_build_scd4_tiers(df_scd3)
    df_scd3.show()
    df_active.show()
    df_hist.show()
