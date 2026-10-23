# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 082: Diagnosing and Fixing OutOfMemory (OOM) Errors: Driver vs Executor Heap
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A production ETL job intermittently crashes with `java.lang.OutOfMemoryError: Java heap space`. On-call engineers arbitrarily double the cluster size, increasing cloud costs by $10,000 without resolving the root cause.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Diagnose and resolve Spark OutOfMemory (OOM) failures: differentiate between Driver OOM and Executor OOM, identify anti-patterns (e.g. `df.collect()` on big data, unconstrained broadcast joins, key skew in `groupBy`), and apply memory-safe architectural fixes.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Driver OOM occurs when the master node runs out of memory, usually caused by collecting large DataFrames (`collect()`, `toPandas()`), broadcasting tables larger than driver memory, or accumulating massive lineage DAGs. Executor OOM occurs when worker tasks process partitions exceeding available JVM heap or off-heap memory.

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
    .appName("UseCase_082_Diagnosing_and_Fixing_OutOfMemory_(OOM)_Errors_Driver_vs_Executor_Heap")
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
# MAGIC ## 5. Driver OOM vs Executor OOM Root Cause Diagnostic Matrix
# MAGIC Review architectural causes and solutions for memory crashes.

# COMMAND ----------

matrix = [
    ("Driver OOM", "Calling df.collect() or df.toPandas() on millions of rows", "Use df.take(n), write to Delta, or use limit()"),
    ("Driver OOM", "Broadcasting table larger than spark.driver.memory", "Disable broadcast or increase autoBroadcastJoinThreshold check"),
    ("Driver OOM", "Chaining 100+ withColumn transformations (deep DAG)", "Break lineage using checkpoint() or write intermediate Delta table"),
    ("Executor OOM", "Severe data skew (millions of rows on one join key)", "Apply Salting or enable AQE skew join"),
    ("Executor OOM", "Shuffle partitions too low (5GB per partition)", "Increase spark.sql.shuffle.partitions to target 150MB/partition"),
    ("Executor OOM", "collect_list() on high-cardinality skewed key", "Filter before collect or use window functions")
]

schema = "failure_type STRING, common_root_cause STRING, architectural_remediation STRING"
df_oom_matrix = spark.createDataFrame(matrix, schema)
df_oom_matrix.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Demonstrating Memory-Safe Sampling vs Unsafe collect()
# MAGIC Replace dangerous full collect with safe streaming limit and take.

# COMMAND ----------

df_large_mock = spark.range(1, 1000000, 1, 4)

# UNSAFE: df_large_mock.collect()  <- Pulls all 1,000,000 objects into driver JVM heap!

# SAFE 1: take(n) only collects requested records
sample_rows = df_large_mock.take(5)
print(f"Safely collected {len(sample_rows)} rows without loading full dataset into driver memory.")

# SAFE 2: toLocalIterator() streams partition-by-partition to driver
iterator = df_large_mock.limit(10).toLocalIterator()
first_three = [next(iterator).id for _ in range(3)]
print("First 3 items via streaming iterator:", first_three)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Breaking Deep Lineage Trees with Checkpoint
# MAGIC Truncate deep Catalyst DAG execution graphs to prevent driver stack overflows.

# COMMAND ----------

# Set checkpoint directory
spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoints_082")

df_deep = spark.range(1, 100)
for _ in range(10):
    df_deep = df_deep.filter(F.col("id") > 0)

# checkpoint() truncates logical plan, saving current RDD to storage and resetting lineage
df_checkpointed = df_deep.checkpoint()
print("Lineage graph successfully truncated with checkpoint()!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Driver OOM is caused by `.collect()`, `.toPandas()`, oversized broadcast tables, or deep Catalyst plans.**
# MAGIC - **Executor OOM is caused by key skew, insufficient shuffle partitions, or large in-memory window buffers.**
# MAGIC - **Use `checkpoint()` to truncate deep lineage graphs in iterative ML or recursive algorithms.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How do you determine from Spark UI whether an OOM error occurred on the Driver or on an Executor?
# MAGIC 2. What is the difference between `df.cache()` and `df.checkpoint()` in terms of lineage truncation?
# MAGIC 3. How does off-heap memory (`spark.memory.offHeap.enabled`) prevent JVM garbage collection pauses?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
