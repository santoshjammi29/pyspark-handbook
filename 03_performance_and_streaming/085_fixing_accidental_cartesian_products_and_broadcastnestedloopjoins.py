"""Scenario & Scale: Identity-resolution join stalling indefinitely at runtime.

Detects `BroadcastNestedLoopJoin` / `CartesianProduct` triggered by `OR` or
non-equi predicates and rewrites them into $O(N)$ equi-joins + `unionByName`.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Joining two tables with an `OR` predicate (`a.id = b.id OR
# a.email = b.email`) or missing join condition.
# Otherwise: Catalyst cannot hash-partition or sort-merge on an `OR` condition,
# forcing `BroadcastNestedLoopJoin` or `CartesianProduct` with $O(N \times M)$
# quadratic complexity—hanging 100M x 10M joins for hours or triggering OOM.
def bad_or_cartesian_join(df_left: DataFrame, df_right: DataFrame) -> DataFrame:
    return df_left.join(
        df_right,
        on=(
            (df_left["id"] == df_right["id"])
            | (df_left["email"] == df_right["email"])
        ),
        how="inner",
    )


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Decompose disjunctive `OR` join predicates into separate
# equi-join branches (`BroadcastHashJoin` / `SortMergeJoin`) and combine with
# `unionByName(...).dropDuplicates(...)` for linear $O(N + M)$ scaling.
def optimized_identity_join(
    df_left: DataFrame,
    df_right: DataFrame,
) -> DataFrame:
    """Rewrite OR join into two equi-joins combined via unionByName + dedup."""
    left_a = df_left.alias("l")
    right_a = df_right.alias("r")

    df_match_id = left_a.join(
        right_a, on=F.col("l.id") == F.col("r.id"), how="inner"
    ).select(
        F.col("l.id").alias("id"),
        F.col("l.email").alias("email"),
        F.col("r.email").alias("matched_email"),
    )

    df_match_email = left_a.join(
        right_a, on=F.col("l.email") == F.col("r.email"), how="inner"
    ).select(
        F.col("l.id").alias("id"),
        F.col("l.email").alias("email"),
        F.col("r.email").alias("matched_email"),
    )

    return df_match_id.unionByName(df_match_email).dropDuplicates(
        ["id", "email", "matched_email"]
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# bad_or_cartesian_join(...).explain("formatted") shows:
#   BroadcastNestedLoopJoin BuildRight, Inner, ((id = id) OR (email = email))
#   (or `CartesianProduct` if both sides exceed autoBroadcastJoinThreshold!)
# optimized_identity_join(...).explain("formatted") shows:
#   HashAggregate (distinct)
#   +- Union
#      :- * BroadcastHashJoin [id], [id], Inner
#      +- * BroadcastHashJoin [email], [email], Inner
# Spark UI -> SQL tab:
#   - Any `BroadcastNestedLoopJoin` or `CartesianProduct` node with >10k rows
#     on both inputs is a critical red flag: `numOutputRows` or comparison
#     latency explodes quadratically.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. `spark.sql.crossJoin.enabled` only blocks joins with NO join predicate
#    (explicit/accidental `CROSS JOIN`); `OR` and `<` / `>` predicates bypass
#    that guard and silently degrade into `BroadcastNestedLoopJoin`!
# 2. Note NULL behavior: equality (`=`) never matches NULL=NULL, matching the
#    `OR` join semantics; if `null` keys exist, filter `isNotNull` prior to
#    joining to shrink shuffle/broadcast size.
# 3. For range/interval non-equi joins (e.g. IP-to-CIDR or `ts BETWEEN start
#    AND end`), bucket/bin timestamps or use prefix keys as an equality guard
#    before applying the range filter so Spark can execute a Hash/SortMergeJoin.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("085_CartesianAndBNLJFix")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    df1 = spark.createDataFrame(
        [(1, "alice@a.com"), (2, "bob@b.com")],
        "id INT, email STRING",
    )
    df2 = spark.createDataFrame(
        [(1, "alice@work.com"), (3, "bob@b.com")],
        "id INT, email STRING",
    )
    bad_or_cartesian_join(df1, df2).explain("formatted")
    df_opt = optimized_identity_join(df1, df2)
    df_opt.explain("formatted")
    df_opt.show()
