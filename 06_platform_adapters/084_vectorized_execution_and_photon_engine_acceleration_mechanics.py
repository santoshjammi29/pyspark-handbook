"""
084 — Vectorized Execution & Databricks Photon C++ Engine Acceleration Mechanics.

Scenario & Scale:
    An analytics platform processes 10B+ row SQL joins and aggregations and needs
    to understand when Databricks Photon (native C++ SIMD vectorized engine) delivers
    2x–5x speedups versus when anti-patterns force fallback to classic JVM/Python.
"""

from __future__ import annotations


# ==============================================================================
# 2. Anti-Pattern: Row-by-Row Python UDFs & Unsafe Division Breaking Vectorization
# ==============================================================================
# ❌ Bad Practice:
#   @F.udf("double")
#   def discounted_margin(price, qty):
#       return (price * 0.9) / qty  # ZeroDivisionError + kills Photon vectorization!
#   df.withColumn("margin", discounted_margin("price", "qty"))
#
# Otherwise:
#   Python `@udf` forces Photon C++ columnar batches to convert to JVM rows
#   (`ColumnarToRow`), serialize via Arrow/pickle to Python worker processes
#   (`BatchEvalPython`), and lose SIMD cache locality—degrading throughput 5x–20x.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: 100% Native Columnar Expressions
# ==============================================================================
# ✅ Good Practice: Express business math with built-in Spark SQL expressions and
#                   `try_divide` so Photon (or Tungsten CodeGen) stays vectorized.
def run_vectorized_aggregation(df):
    """Keep filters, arithmetic, and aggregations inside native vectorized operators."""
    from pyspark.sql import functions as F

    return (
        df.filter(F.col("price") > F.lit(500_000.0))
        .withColumn("margin", F.expr("try_divide(price * 0.9, qty)"))
        .groupBy("category")
        .agg(
            F.sum("price").alias("total_price"),
            F.avg("margin").alias("avg_margin"),
        )
    )


def get_photon_comparison_matrix(spark):
    """Return an educational comparison DataFrame of Classic JVM vs Photon."""
    rows = [
        ("Engine Runtime", "JVM Bytecode (Whole-Stage CodeGen)", "Native C++ Machine Code"),
        ("Memory Layout", "Row-oriented Tungsten BinaryRow", "Columnar Batches in L1/L2 CPU Cache"),
        ("CPU Execution", "Scalar JVM loops", "SIMD Vectorized Instructions"),
        ("GC Overhead", "JVM G1GC pauses on heap pressure", "Zero JVM GC on Photon operators"),
        ("Fallback Trigger", "Python UDFs / RDDs", "Drops stage back to JVM + Python"),
    ]
    return spark.createDataFrame(rows, "dimension STRING, classic_spark STRING, databricks_photon STRING")


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - On Photon clusters, inspect `df.explain("formatted")` for `PhotonScan`,
#   `PhotonProject`, `PhotonGroupingAgg`, and `PhotonShuffleExchangeSink`.
# - Red flags in `explain("formatted")` / Spark UI DAG: `ColumnarToRow` followed by
#   `BatchEvalPython` or `ArrowEvalPython` indicates Photon fell back to JVM/Python.
# - In Spark UI -> SQL tab, check the "Photon %" metric bar on each query node.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: Photon does NOT accelerate RDD APIs, Python `@udf`s, or complex
#   legacy Scala UDFs; replace row UDFs with SQL built-ins or `pandas_udf` only if needed.
# - Gotcha 2: Wide schemas with deeply nested maps/structs or high-cardinality string
#   UDFs can reduce Photon's SIMD efficiency—project only needed columns early.
# - Gotcha 3: `try_divide` avoids Spark 4.0 ANSI `DIVIDE_BY_ZERO` aborts while staying
#   100% vectorizable inside native C++/JVM codegen.


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("Adapter_084_Photon_Vectorized").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "true")

    get_photon_comparison_matrix(spark).show(truncate=False)

    df_bench = spark.range(1, 200_000, 1, 4).select(
        F.col("id"),
        (F.col("id") * 5.0).alias("price"),
        (F.col("id") % 5).alias("category"),
        (F.col("id") % 3).alias("qty"),  # includes 0 to exercise ANSI try_divide
    )
    run_vectorized_aggregation(df_bench).show(truncate=False)
