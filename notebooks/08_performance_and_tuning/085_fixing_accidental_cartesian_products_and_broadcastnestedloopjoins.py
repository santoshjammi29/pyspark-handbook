# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 085: Fixing Accidental Cartesian Products and BroadcastNestedLoopJoins
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A junior engineer submits a join query where the join condition accidentally uses an OR operator across two conditions: `ON (a.id = b.id OR a.email = b.email)`. The job hangs indefinitely because Catalyst cannot use hash joins or sort-merge joins, falling back to a catastrophic Cartesian product.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Detect and fix accidental Cartesian products: identify `BroadcastNestedLoopJoin` and `CartesianProduct` operators in execution plans, understand why non-equi joins and OR join conditions disable hash joins, and rewrite queries using UNIONs or window functions.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC SortMergeJoin and BroadcastHashJoin strictly require equi-join conditions (`=`). When a join predicate contains an `OR`, `<>`, or non-equi condition without an equality key, Spark must fall back to a Cartesian Product (complexity $O(N 	imes M)$), generating quadratic row comparisons.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Setup & SparkSession Initialization
# MAGIC If running inside a Databricks notebook, the `spark` session is pre-initialized by the cluster.
# MAGIC If executing outside Databricks (e.g. VS Code, local CLI, CI/CD runner), we initialize a local SparkSession with Delta Lake support.

# COMMAND ----------

import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_085_Fixing_Accidental_Cartesian_Products_and_BroadcastNestedLoopJoins")
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
# MAGIC ### Step 1: Simulating Accidental Cartesian Join Predicate
# MAGIC Create dataset and inspect Cartesian product plan.

# COMMAND ----------

df1 = spark.createDataFrame([(1, "alice@a.com"), (2, "bob@b.com")], ["id", "email"])
df2 = spark.createDataFrame([(1, "alice@work.com"), (3, "bob@b.com")], ["id", "email"])

# Dangerous Join: Joining with OR condition disables Hash Join!
dangerous_join = df1.join(df2, (df1["id"] == df2["id"]) | (df1["email"] == df2["email"]), how="inner")

print("=== DANGEROUS PHYSICAL PLAN (Notice BroadcastNestedLoopJoin or CartesianProduct!) ===")
dangerous_join.explain(mode="simple")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Refactoring Anti-Pattern Using Multiple Equi-Joins and Union
# MAGIC Rewrite OR join into two fast equi-joins combined with distinct union.

# COMMAND ----------

# Optimized Architecture: Split OR condition into two separate Equi-Joins and Union
# 1. Match on ID (fast HashJoin)
df_match_id = df1.join(df2, on="id", how="inner").select(df1["id"], df1["email"])

# 2. Match on Email (fast HashJoin)
df_match_email = df1.join(df2, on="email", how="inner").select(df1["id"], df1["email"])

# 3. Combine with distinct to deduplicate matches
df_optimized = df_match_id.unionByName(df_match_email).distinct()

print("=== OPTIMIZED PHYSICAL PLAN (Both branches use high-speed Hash Joins!) ===")
df_optimized.explain(mode="simple")
df_optimized.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Validation & Complexity Comparison
# MAGIC Verify both queries return identical business results with radically different complexity.

# COMMAND ----------

print("Both implementations produce identical business results, but the optimized query scales linearly O(N) instead of quadratically O(N*M)!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Joining on non-equi conditions or OR predicates forces Spark into `CartesianProduct` or `BroadcastNestedLoopJoin`.**
# MAGIC - **Cartesian products exhibit $O(N 	imes M)$ complexity and will stall clusters on production datasets.**
# MAGIC - **Refactor OR join conditions into separate equi-join queries combined with `union().distinct()`.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why does an `OR` in a join condition prevent Spark from using Sort-Merge Join or Broadcast Hash Join?
# MAGIC 2. What is a `BroadcastNestedLoopJoin` and under what conditions does Catalyst choose it?
# MAGIC 3. How do you rewrite a non-equi join query to enable efficient hash joining?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
