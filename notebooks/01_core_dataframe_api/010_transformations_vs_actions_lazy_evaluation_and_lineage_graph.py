# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 010: Transformations vs Actions, Lazy Evaluation & Lineage Graph
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A performance engineering lead must diagnose why a data pipeline fails after running for 40 minutes, only to crash on a lightweight `.count()` action. The team must learn to trace lineage graphs and identify narrow vs wide transformations.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Demonstrate Spark's lazy evaluation mechanism. Differentiate between Narrow dependencies (map, filter) and Wide dependencies (groupBy, join, distinct). Inspect the RDD execution lineage graph using `toDebugString()` and profile action execution times.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Spark constructs a Directed Acyclic Graph (DAG) of stages separated by shuffle boundaries. Narrow transformations execute entirely within the same partition in memory (pipelining). Wide transformations require data shuffling across the network to new executor partitions, creating new stages.

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
    .appName("UseCase_010_Transformations_vs_Actions,_Lazy_Evaluation_and_Lineage_Graph")
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
# MAGIC ## 5. Building a Pipeline Lineage Graph
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
# MAGIC ## 6. Inspecting RDD Lineage Graph via toDebugString
# MAGIC Inspect how Spark groups operations into stages separated by shuffles.

# COMMAND ----------

print("=== RDD LINEAGE GRAPH (toDebugString) ===")
print(df_wide.rdd.toDebugString().decode("utf-8"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Triggering Actions & Profiling Execution
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
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Transformations build the logical plan; actions trigger physical job execution and DAG scheduling.**
# MAGIC - **Narrow transformations pipeline within the same JVM task; wide transformations create stage boundaries and write shuffle files.**
# MAGIC - **Understanding the DAG via `rdd.toDebugString()` helps locate unnecessary shuffles before deployment.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between narrow and wide dependencies in Apache Spark?
# MAGIC 2. Why does calling `.show()` take time while defining 10 `.filter()` statements executes instantaneously?
# MAGIC 3. What causes stage boundary creation in a Spark DAG?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
