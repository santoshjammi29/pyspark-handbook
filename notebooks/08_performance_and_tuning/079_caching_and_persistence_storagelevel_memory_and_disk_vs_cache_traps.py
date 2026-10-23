# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 079: Caching and Persistence: StorageLevel (MEMORY_AND_DISK) vs Cache Traps
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A machine learning feature engineering notebook calls `df.cache()` on a 50GB dataset. Later in the pipeline, iterative actions cause severe memory eviction, JVM garbage collection thrashing, and executor crashes. The team must learn when caching helps versus when it harms performance.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Master Spark DataFrame caching: compare `df.cache()` (default `MEMORY_AND_DISK_DESER`) with explicit `persist(StorageLevel)` options, inspect cache storage in Spark UI, identify common cache traps (caching intermediate one-use DataFrames), and safely unpersist (`df.unpersist()`).
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC `cache()` is an alias for `persist(StorageLevel.MEMORY_AND_DISK)`. Caching is lazy: data is only materialized into memory when an action is executed. Caching small-to-medium DataFrames accessed multiple times saves computation; caching large DataFrames or single-use DataFrames wastes executor memory and evicts shuffle buffers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Environment Setup & SparkSession Initialization
# MAGIC We configure a resilient `SparkSession` compatible with Databricks runtime, local PySpark development, or CI/CD test runners.

# COMMAND ----------

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Initialize SparkSession with Delta Lake support and optimized local configs
spark = (
    SparkSession.builder
    .appName("UseCase_079_Caching_and_Persistence_StorageLevel_(MEMORY_AND_DISK)_vs_Cache_Traps")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.sparkContext.appName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persisting DataFrame with StorageLevel.MEMORY_AND_DISK
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
# MAGIC ## 6. Iterative Consumption of Cached DataFrame
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
# MAGIC ## 7. Safely Unpersisting Cached Storage
# MAGIC Release executor memory immediately when cached dataset is no longer needed.

# COMMAND ----------

# Always unpersist when finished to prevent memory leaks and GC pressure
df_features.unpersist(blocking=True)
print(f"DataFrame unpersisted. Is cached: {df_features.is_cached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Only cache DataFrames that are accessed multiple times across distinct actions or iterative ML algorithms.**
# MAGIC - **`df.cache()` is lazy—no data is stored in memory until an action (`count()`, `show()`) is called.**
# MAGIC - **Always call `df.unpersist()` as soon as downstream processing of the cached DataFrame is complete.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `cache()` and `persist()` in PySpark?
# MAGIC 2. What are the different StorageLevels available in Apache Spark (MEMORY_ONLY, MEMORY_AND_DISK, _SER)?
# MAGIC 3. Why is caching a DataFrame that is only used once an anti-pattern?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
