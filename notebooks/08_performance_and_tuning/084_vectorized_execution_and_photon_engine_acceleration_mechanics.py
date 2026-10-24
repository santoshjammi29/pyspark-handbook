# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 084: Vectorized Execution and Photon Engine Acceleration Mechanics
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An enterprise evaluates migrating high-throughput SQL analytics from classic Apache Spark clusters to Databricks clusters with Photon Engine enabled. Leadership requires an architectural analysis of C++ vectorized execution vs JVM row-based execution.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Deep dive into Vectorized Execution and Databricks Photon: understand CPU cache locality, SIMD vectorization, columnar in-memory processing, identify operators supported by Photon, and analyze execution speedup profiles.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Classic Apache Spark compiles queries into Java bytecode running inside JVMs. Databricks Photon is a vectorized execution engine written from scratch in C++. It operates on columnar batches in CPU L1/L2 caches and uses SIMD (Single Instruction, Multiple Data) instructions, accelerating SQL and aggregation workloads by 2x to 5x.

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
        .appName("UseCase_084_Vectorized_Execution_and_Photon_Engine_Acceleration_Mechanics")
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
# MAGIC ### Step 1: Architectural Comparison: Classic JVM Spark vs Databricks Photon
# MAGIC Analyze the fundamental architectural differences between engines.

# COMMAND ----------

comparison = [
    ("Core Language", "Java / Scala (JVM Bytecode)", "C++ (Native Machine Code)"),
    ("Execution Model", "Tungsten Whole-Stage CodeGen", "Vectorized Columnar Batch Processing"),
    ("CPU Optimization", "Standard JVM register usage", "SIMD (Single Instruction Multiple Data)"),
    ("Memory Management", "JVM Garbage Collection & Off-Heap", "Direct C++ Memory Management (Zero GC overhead)"),
    ("Supported Workloads", "General PySpark, RDDs, Custom UDFs", "SQL, Aggregations, Joins, Parquet/Delta I/O")
]

schema = "dimension STRING, classic_spark STRING, databricks_photon STRING"
df_comp = spark.createDataFrame(comparison, schema)
df_comp.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Benchmarking Vector-Friendly Aggregations in PySpark
# MAGIC Construct queries optimized for vectorized batch execution.

# COMMAND ----------

import time

# Vectorized engines excel at high-speed columnar filtering and math
df_vec_bench = spark.range(1, 1000000, 1, 4).select(
    F.col("id"),
    (F.col("id") * 1.5).alias("price"),
    (F.col("id") % 5).alias("category")
)

t0 = time.time()
res = df_vec_bench.filter(F.col("price") > 500000).groupBy("category").agg(F.sum("price"), F.avg("price")).collect()
elapsed = time.time() - t0

print(f"Columnar Vector-Friendly Aggregation evaluated in {elapsed:.3f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Photon Compatibility Best Practices
# MAGIC Identify anti-patterns that force Photon to fall back to classic JVM execution.

# COMMAND ----------

rules = [
    ("Native Built-ins", "Always use native Spark SQL functions; Photon accelerates 100% of built-in expressions."),
    ("Avoid Non-Vectorized UDFs", "Standard Python @udf forces Photon to serialize data back into Python subprocesses!"),
    ("Parquet / Delta Storage", "Photon reads directly from Parquet and Delta columnar formats with zero-copy decoding.")
]

for title, rule in rules:
    print(f" {title}: {rule}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Photon is Databricks' vectorized query engine written in C++ for maximum CPU cache locality and SIMD execution.**
# MAGIC - **Photon eliminates Java Virtual Machine garbage collection pauses entirely for data operations.**
# MAGIC - **Custom Python UDFs disable Photon acceleration for that stage, forcing fallback to classic Spark JVM.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is Databricks Photon and how does it differ from classic Apache Spark Tungsten Whole-Stage CodeGen?
# MAGIC 2. What is SIMD (Single Instruction, Multiple Data) and how does vectorized execution leverage it?
# MAGIC 3. Which query operations benefit most from Photon, and which operations force a fallback to the JVM?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
