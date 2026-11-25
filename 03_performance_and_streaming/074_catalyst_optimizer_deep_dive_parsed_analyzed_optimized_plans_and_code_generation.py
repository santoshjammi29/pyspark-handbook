"""Scenario & Scale: Enterprise SQL/DataFrame architecture review at TB scale.

Inspects Catalyst's 4 compilation phases (Parsed, Analyzed, Optimized, Physical)
and Whole-Stage CodeGen (`*`) via `explain("formatted")` before cluster runs.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Wrapping filter/projection logic in opaque Python UDFs or
# blindly chaining transforms without inspecting explain("formatted").
# Otherwise: Python UDFs act as Catalyst black boxes—blocking predicate pushdown,
# column pruning, constant folding, and Whole-Stage Code Generation, resulting
# in full table scans and 10x–50x Python serialization CPU penalties.
def bad_opaque_udf_pipeline(df_orders: DataFrame) -> DataFrame:
    is_high_value = F.udf(
        lambda amt, status: status == "ACTIVE" and (amt or 0.0) > 200.0,
        "boolean",
    )
    return df_orders.filter(is_high_value(F.col("amount"), F.col("status")))


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: Express filters and expressions with native Catalyst Column
# operators so Catalyst folds constants, pushes predicates to Parquet/Delta/ORC
# footers, prunes unused columns, and emits Whole-Stage Java bytecode.
def build_optimizable_orders_query(df_orders: DataFrame) -> DataFrame:
    """Build a Catalyst-optimizable aggregation with pushable predicates."""
    return (
        df_orders.filter(F.col("status") == F.lit("ACTIVE"))
        .filter(F.col("amount") > (F.lit(100.0) * F.lit(2.0)))
        .groupBy("customer_id")
        .agg(F.sum(F.try_cast(F.col("amount"), "double")).alias("total_spend"))
        .filter(F.col("total_spend") > F.lit(500.0))
    )


def inspect_catalyst_plans(df: DataFrame) -> None:
    """Print concise formatted plan and full 4-phase Catalyst plan."""
    df.explain("formatted")
    df.explain(extended=True)


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df.explain("formatted") output anatomy:
#   * HashAggregate (keys=[customer_id], functions=[sum(amount)])
#   +- Exchange hashpartitioning(customer_id, 4)
#      +- * HashAggregate (partial_sum)
#         +- * Project [customer_id, amount]
#            +- * Filter ((status = ACTIVE) AND (amount > 200.0))
#               +- * Range / FileScan [PushedFilters: IsNotNull(status), ...]
# Diagnostic signals:
#   - Constant folding collapsed `100.0 * 2.0` -> `200.0` at planning time.
#   - Asterisk `*` prefix confirms WholeStageCodegen fused operators into a
#     single Java loop (no virtual function dispatch or intermediate objects).
#   - `PushedFilters` / `ReadSchema` confirm storage-layer pushdown & pruning.
#   - Presence of `BatchEvalPython` / `ArrowEvalPython` warns a Python UDF broke
#     WholeStageCodegen.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. `df.explain("formatted")` is far cleaner than `explain(True)` for large DAGs
#    because it separates the operator tree from detailed node parameters.
# 2. The 4 Catalyst phases are: (1) Parsed Logical Plan (AST), (2) Analyzed
#    Logical Plan (Catalog types/columns resolved), (3) Optimized Logical Plan
#    (rules: constant folding, predicate pushdown, pruning), (4) Physical Plan
#    (CBO/AQE join & scan strategies + WholeStageCodegen).
# 3. In Spark 4.0 ANSI mode, invalid arithmetic/casts inside expressions fail
#    loudly unless guarded with `try_cast` / `try_divide` / `try_add`.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("074_CatalystExplainFormatted")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    df_orders = spark.range(1, 10001, 1, 4).select(
        F.col("id").alias("order_id"),
        (F.col("id") % F.lit(10)).alias("customer_id"),
        (F.col("id") * F.lit(1.5)).alias("amount"),
        F.lit("ACTIVE").alias("status"),
    )
    inspect_catalyst_plans(build_optimizable_orders_query(df_orders))
