# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 080: Repartition vs Coalesce Internals: Full Shuffle vs Partition Minimization
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A data pipeline writes daily reports to cloud storage. An engineer notices that writing a 5MB aggregated table produces 200 tiny files, while attempting to fix it with `repartition(1)` causes an expensive full-cluster shuffle. The team needs to choose between `repartition()` and `coalesce()` correctly.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Deep dive into partition restructuring: compare `repartition(numPartitions)` vs `coalesce(numPartitions)`. Explain why `repartition` triggers a full network shuffle while `coalesce` collapses existing partitions locally, and measure execution plans.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC `repartition()` performs a full shuffle across all cluster nodes using a HashPartitioner, creating evenly balanced partitions. It can both increase and decrease partition counts. `coalesce()` avoids a full shuffle by merging adjacent partitions on the same executor, but can ONLY decrease partition counts and can result in uneven partition sizes.

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
        .appName("UseCase_080_Repartition_vs_Coalesce_Internals_Full_Shuffle_vs_Partition_Minimization")
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
# MAGIC ### Step 1: Generating Dataset with High Partition Count
# MAGIC Create dataset across 8 partitions.

# COMMAND ----------

df_source = spark.range(1, 40001, 1, 8)
print(f"Initial Partition Count: {df_source.rdd.getNumPartitions()}")

# Inspect distribution
df_source.withColumn("pid", F.spark_partition_id()).groupBy("pid").count().orderBy("pid").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Comparing Execution Plans: coalesce(2) vs repartition(2)
# MAGIC Observe absence of shuffle in coalesce vs Exchange operator in repartition.

# COMMAND ----------

# 1. Coalesce down to 2 partitions (No shuffle!)
df_coalesced = df_source.coalesce(2)
print("=== COALESCE PHYSICAL PLAN (No Exchange/Shuffle!) ===")
df_coalesced.explain(mode="simple")
print(f"Coalesced partitions: {df_coalesced.rdd.getNumPartitions()}")

# 2. Repartition down to 2 partitions (Triggers full network shuffle!)
df_repartitioned = df_source.repartition(2)
print("=== REPARTITION PHYSICAL PLAN (Notice 'Exchange RoundRobinPartitioning'!) ===")
df_repartitioned.explain(mode="simple")
print(f"Repartitioned partitions: {df_repartitioned.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Increasing Partition Count (repartition only)
# MAGIC Demonstrate that coalesce cannot increase partition count.

# COMMAND ----------

# Attempting to increase partitions from 2 to 10 with coalesce fails silently (remains 2)
df_failed_increase = df_coalesced.coalesce(10)
print(f"coalesce(10) on 2 partitions results in: {df_failed_increase.rdd.getNumPartitions()} partitions (Cannot increase!)")

# Repartition successfully increases to 10
df_success_increase = df_coalesced.repartition(10)
print(f"repartition(10) on 2 partitions results in: {df_success_increase.rdd.getNumPartitions()} partitions (Successfully expanded!)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Use `coalesce(n)` to decrease partition count before writing to disk—it eliminates network shuffles.**
# MAGIC - **Use `repartition(n)` to increase partition count or evenly redistribute data across executors after heavy skew.**
# MAGIC - **`repartition(col)` partitions data by specific column keys for downstream join or write efficiency.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the internal difference between `repartition()` and `coalesce()` in Apache Spark?
# MAGIC 2. Why can `coalesce()` lead to severe partition size imbalance if partitions are reduced too drastically?
# MAGIC 3. What happens if you call `coalesce(n)` where `n` is greater than the current partition count?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
