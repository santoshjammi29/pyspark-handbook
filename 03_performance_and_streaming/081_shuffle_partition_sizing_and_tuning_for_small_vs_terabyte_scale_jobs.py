# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 081: Shuffle Partition Sizing and Tuning for Small vs Terabyte-Scale Jobs
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A 100MB dataset query takes 45 seconds because Spark defaults to 200 shuffle partitions (`spark.sql.shuffle.partitions = 200`), launching 200 tasks that each process 500KB. Conversely, a 1TB join fails with executor GC timeouts because 200 partitions result in 5GB per task. The team needs a formula to tune shuffle partition sizing.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master shuffle partition sizing: calculate optimal partition counts based on shuffle input data size (targeting 100MB - 200MB per partition), configure `spark.sql.shuffle.partitions` dynamically, and compare execution metrics.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Spark's default `spark.sql.shuffle.partitions = 200` was chosen arbitrarily in Spark 1.0. If shuffle partitions are too high, task scheduling overhead dominates. If too low, partition data exceeds executor RAM, triggering costly disk spills or OOM errors. Rule of thumb: $ShufflePartitions = ShuffleDataSize / 150MB$.

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
        .appName("UseCase_081_Shuffle_Partition_Sizing_and_Tuning_for_Small_vs_Terabyte_Scale_Jobs")
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
# MAGIC ### Step 1: Sizing Formula Cheatsheet
# MAGIC Review mathematical guidelines for sizing shuffle partitions.

# COMMAND ----------

# Tuning Formula:
# Target Partition Size = 100MB to 200MB
# Shuffle Partitions = Total Input Data at Shuffle / 150MB
# Example: 10GB dataset -> 10,000MB / 150MB ≈ 67 partitions
# Example: 500GB dataset -> 500,000MB / 150MB ≈ 3,333 partitions

print("Shuffle partition calculation guidelines loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Benchmarking Small Query with Default (200) vs Tuned (4) Partitions
# MAGIC Observe performance differences on small dataset.

# COMMAND ----------

import time

df_small = spark.range(1, 50000, 1, 4).withColumn("group", F.col("id") % 10)

# Run with default 200 shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")
t0 = time.time()
c1 = df_small.groupBy("group").count().collect()
t_default = time.time() - t0
print(f"200 Partitions Execution Time: {t_default:.3f}s (High task scheduling overhead)")

# Run with tuned 4 shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "4")
t0 = time.time()
c2 = df_small.groupBy("group").count().collect()
t_tuned = time.time() - t0
print(f"4 Partitions Execution Time: {t_tuned:.3f}s (Speedup: {t_default/max(t_tuned, 0.001):.1f}x)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Dynamic Partitioning via Adaptive Query Execution (AQE)
# MAGIC Enable AQE to automate partition sizing without manual guessing.

# COMMAND ----------

# With AQE enabled, Spark automatically coalesces post-shuffle partitions to ~64MB target size
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(64 * 1024 * 1024)) # 64MB advisory size

print("AQE advisory partition size configured to 64MB!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Target between 100MB and 200MB of compressed data per shuffle partition task.**
# MAGIC - **Default 200 shuffle partitions hurts small queries (< 1GB) and crashes large queries (> 500GB).**
# MAGIC - **Use AQE (`spark.sql.adaptive.advisoryPartitionSizeInBytes`) to let Spark dynamically calculate optimal partition counts.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How do you determine the optimal value for `spark.sql.shuffle.partitions` for a 500GB dataset?
# MAGIC 2. What happens if `spark.sql.shuffle.partitions` is set too low versus set too high?
# MAGIC 3. How does `spark.sql.adaptive.advisoryPartitionSizeInBytes` interact with shuffle partitions?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
