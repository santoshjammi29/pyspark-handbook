# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 079: Caching and Persistence: StorageLevel (MEMORY_AND_DISK) vs Cache Traps
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A machine learning feature engineering notebook calls `df.cache()` on a 50GB dataset. Later in the pipeline, iterative actions cause severe memory eviction, JVM garbage collection thrashing, and executor crashes. The team must learn when caching helps versus when it harms performance.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master Spark DataFrame caching: compare `df.cache()` (default `MEMORY_AND_DISK_DESER`) with explicit `persist(StorageLevel)` options, inspect cache storage in Spark UI, identify common cache traps (caching intermediate one-use DataFrames), and safely unpersist (`df.unpersist()`).
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC `cache()` is an alias for `persist(StorageLevel.MEMORY_AND_DISK)`. Caching is lazy: data is only materialized into memory when an action is executed. Caching small-to-medium DataFrames accessed multiple times saves computation; caching large DataFrames or single-use DataFrames wastes executor memory and evicts shuffle buffers.

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
        .appName("UseCase_079_Caching_and_Persistence_StorageLevel_MEMORY_AND_DISK_vs_Cache_Traps")
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
# MAGIC ### Step 1: Persisting DataFrame with StorageLevel.MEMORY_AND_DISK
# MAGIC Persist DataFrame explicitly and trigger lazy materialization with count.

# COMMAND ----------

from pyspark import StorageLevel

df_features = spark.range(1, 50000, 1, 4).select(
    F.col("id"),
    (F.col("id") * 2.5).alias("feat_a"),
    F.sin("id").alias("feat_b")
)

# Persist with MEMORY_AND_DISK (spills to disk if RAM is exhausted rather than throwing OOM)
df_features.persist(StorageLevel.MEMORY_AND_DISK)
print(f"Is DataFrame cached: {df_features.is_cached}")

# Trigger lazy caching via action
row_count = df_features.count()
print(f"Materialized {row_count} rows into executor memory/disk storage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Iterative Consumption of Cached DataFrame
# MAGIC Execute multiple downstream actions leveraging the cached snapshot without recomputing features.

# COMMAND ----------

import time

# Action 1: Aggregation
t0 = time.time()
res1 = df_features.filter(F.col("feat_a") > 1000).count()
print(f"Action 1 (from cache): {res1} rows in {time.time() - t0:.3f}s")

# Action 2: Statistical summary
t0 = time.time()
res2 = df_features.agg(F.avg("feat_b")).first()[0]
print(f"Action 2 (from cache): avg(feat_b) = {res2:.4f} in {time.time() - t0:.3f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Safely Unpersisting Cached Storage
# MAGIC Release executor memory immediately when cached dataset is no longer needed.

# COMMAND ----------

# Always unpersist when finished to prevent memory leaks and GC pressure
df_features.unpersist(blocking=True)
print(f"DataFrame unpersisted. Is cached: {df_features.is_cached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Only cache DataFrames that are accessed multiple times across distinct actions or iterative ML algorithms.**
# MAGIC - **`df.cache()` is lazy—no data is stored in memory until an action (`count()`, `show()`) is called.**
# MAGIC - **Always call `df.unpersist()` as soon as downstream processing of the cached DataFrame is complete.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `cache()` and `persist()` in PySpark?
# MAGIC 2. What are the different StorageLevels available in Apache Spark (MEMORY_ONLY, MEMORY_AND_DISK, _SER)?
# MAGIC 3. Why is caching a DataFrame that is only used once an anti-pattern?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
