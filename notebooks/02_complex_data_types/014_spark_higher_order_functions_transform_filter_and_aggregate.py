# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 014: Spark Higher-Order Functions: Transform, Filter and Aggregate
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A fintech loyalty platform tracks customer reward points earned across multiple merchant transactions in an array. The team needs to adjust point values, filter out expired promo points, and sum net balances without exploding rows.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement Spark 3+ Higher-Order Functions directly on arrays: `transform` (lambda mapping), `filter` (predicate filtering), `aggregate` (reduce/fold), `exists`, and `forall` without incurring expensive explode-and-groupBy shuffles.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Higher-order functions run inline lambda expressions inside the JVM on array elements without flattening the DataFrame. Exploding an array multiplies the row count and requires a subsequent `groupBy()` shuffle to reconstitute the row, causing 10x-100x slower execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Setup & SparkSession Initialization
# MAGIC If running inside a Databricks notebook, the `spark` session is pre-initialized by the cluster.
# MAGIC If executing outside Databricks (e.g. VS Code, local CLI, CI/CD runner), we initialize a local SparkSession with Delta Lake support.

# COMMAND ----------

import os
import sys

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_014_Spark_Higher_Order_Functions_Transform_Filter_and_Aggregate")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.sparkContext.appName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Generating Loyalty Transactions Data
# MAGIC Create accounts with arrays of point earnings.

# COMMAND ----------

loyalty_data = [
    ("ACC-1", [150, 45, 800, 20, 310]),
    ("ACC-2", [50, 25, 10]),
    ("ACC-3", [1200, 450, 90, 850]),
    ("ACC-4", [5, 12, 18])
]

df_loyalty = spark.createDataFrame(loyalty_data, ["account_id", "point_events"])
df_loyalty.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform & Filter Higher-Order Functions
# MAGIC Apply 10% bonus to points over 100 and filter out micro-transactions (< 30 points).

# COMMAND ----------

# 1. Transform: add 10% bonus to all points: x -> cast(x * 1.10 as int)
df_transformed = df_loyalty.withColumn(
    "bonus_points",
    F.expr("transform(point_events, x -> cast(x * 1.10 AS INT))")
)

# 2. Filter: keep only significant transactions >= 50
df_filtered = df_transformed.withColumn(
    "significant_events",
    F.expr("filter(point_events, x -> x >= 50)")
)

df_filtered.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Aggregate (Reduce), Exists and Forall
# MAGIC Compute running sum via aggregate and evaluate boolean array conditions.

# COMMAND ----------

# Aggregate: sum all points starting from initial accumulator 0: (acc, x) -> acc + x
df_agg = df_filtered.withColumn(
    "total_points",
    F.expr("aggregate(point_events, 0, (acc, x) -> acc + x)")
).withColumn(
    "has_super_bonus",
    F.expr("exists(point_events, x -> x >= 500)")
).withColumn(
    "all_positive",
    F.expr("forall(point_events, x -> x > 0)")
)

df_agg.select("account_id", "point_events", "total_points", "has_super_bonus", "all_positive").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Higher-order functions eliminate the need for `explode()` and `groupBy()` when manipulating array elements.**
# MAGIC - **`transform` maps every element to a new value via lambda expression.**
# MAGIC - **`aggregate(arr, zero, (acc, x) -> ...)` performs in-memory reduction without network shuffle.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why are Higher-Order Functions significantly faster than `explode` followed by `groupBy`?
# MAGIC 2. How does `exists(arr, x -> cond)` differ from `array_contains(arr, val)`?
# MAGIC 3. Can you use custom Python functions inside Spark's `transform` higher-order function?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
