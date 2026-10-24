# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 010: Transformations vs Actions, Lazy Evaluation & Lineage Graph
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A performance engineering lead must diagnose why a data pipeline fails after running for 40 minutes, only to crash on a lightweight `.count()` action. The team must learn to trace lineage graphs and identify narrow vs wide transformations.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Demonstrate Spark's lazy evaluation mechanism. Differentiate between Narrow dependencies (map, filter) and Wide dependencies (groupBy, join, distinct). Inspect the RDD execution lineage graph using `toDebugString()` and profile action execution times.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Spark constructs a Directed Acyclic Graph (DAG) of stages separated by shuffle boundaries. Narrow transformations execute entirely within the same partition in memory (pipelining). Wide transformations require data shuffling across the network to new executor partitions, creating new stages.

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
        .appName("UseCase_010_Transformations_vs_Actions_Lazy_Evaluation_and_Lineage_Graph")
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
# MAGIC ### Step 1: Building a Pipeline Lineage Graph
# MAGIC Chain narrow and wide transformations without triggering computation.

# COMMAND ----------

# Generate sequence of 100,000 numbers
df_nums = spark.range(1, 100001, 1, 4)

# Narrow transformation: filter (stays within partition)
df_narrow = df_nums.filter(F.col("id") % 2 == 0).withColumn("squared", F.col("id") * F.col("id"))

# Wide transformation: groupBy and agg (triggers shuffle across partitions)
df_wide = df_narrow.groupBy(F.col("id") % 10).agg(F.sum("squared").alias("sum_squares"))

print("Transformations registered in Catalyst. No execution has occurred yet!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Inspecting RDD Lineage Graph via toDebugString
# MAGIC Inspect how Spark groups operations into stages separated by shuffles.

# COMMAND ----------

print("=== RDD LINEAGE GRAPH (toDebugString) ===")
print(df_wide.rdd.toDebugString().decode("utf-8"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Triggering Actions & Profiling Execution
# MAGIC Measure execution when actions (`count`, `collect`, `take`) trigger Catalyst optimization, stage generation, and task execution.

# COMMAND ----------

import time

start = time.time()
result_count = df_wide.count()
elapsed = time.time() - start

print(f"Action triggered! Result rows: {result_count} (Execution Time: {elapsed:.3f}s)")
df_wide.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Transformations build the logical plan; actions trigger physical job execution and DAG scheduling.**
# MAGIC - **Narrow transformations pipeline within the same JVM task; wide transformations create stage boundaries and write shuffle files.**
# MAGIC - **Understanding the DAG via `rdd.toDebugString()` helps locate unnecessary shuffles before deployment.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between narrow and wide dependencies in Apache Spark?
# MAGIC 2. Why does calling `.show()` take time while defining 10 `.filter()` statements executes instantaneously?
# MAGIC 3. What causes stage boundary creation in a Spark DAG?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
