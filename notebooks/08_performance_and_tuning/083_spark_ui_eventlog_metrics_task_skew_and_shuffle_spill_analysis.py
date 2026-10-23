# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 083: Spark UI, EventLog Metrics, Task Skew and Shuffle Spill Analysis
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A mission-critical nightly reconciliation job slows down by 3x on weekends. The senior engineer must inspect Spark execution metrics to identify whether the slowdown is caused by garbage collection, shuffle spill (Memory vs Disk), task skew, or CPU starvation.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Analyze Spark execution metrics programmatically and via Spark UI: identify Shuffle Spill (Memory) and Shuffle Spill (Disk), detect task stragglers, interpret execution timelines, and query Spark listener bus metrics.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Shuffle Spill occurs when an executor's execution memory buffer cannot hold intermediate shuffle or aggregation data. Spark first spills to uncompressed memory buffer (`Spill (Memory)`), and if that exceeds limits, writes compressed data to local executor scratch disk (`Spill (Disk)`), destroying I/O performance.

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
    .appName("UseCase_083_Spark_UI,_EventLog_Metrics,_Task_Skew_and_Shuffle_Spill_Analysis")
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
# MAGIC ## 5. Spark UI Diagnostic Checklist Blueprint
# MAGIC Review key tabs and metric indicators in the Spark UI.

# COMMAND ----------

ui_checklist = [
    ("Jobs Tab", "Identify which Job ID took the most duration; verify stage dependency boundaries."),
    ("Stages Tab", "Inspect Task Duration Summary (Min, Median, 75th, Max). If Max >> Median, you have TASK SKEW!"),
    ("Spill (Memory/Disk)", "Indicates executor RAM was overwhelmed. Increase shuffle partitions or executor memory."),
    ("Task GC Time", "If GC time > 10% of total task time, executors suffer from heap thrashing. Increase memory or reduce object creation."),
    ("Executors Tab", "Check for dead executors, failed tasks, and uneven distribution of task counts across nodes.")
]

for tab, guidance in ui_checklist:
    print(f"[{tab}]: {guidance}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Programmatically Inspecting Stage & Execution Metrics
# MAGIC Query the Spark status tracker to extract task execution metrics.

# COMMAND ----------

# Run a sample wide-shuffle aggregation to generate metrics
df_metrics_job = spark.range(1, 100000, 1, 4).groupBy(F.col("id") % 10).count()
df_metrics_job.collect()

# Query SparkContext StatusTracker
status_tracker = spark.sparkContext.statusTracker()
active_jobs = status_tracker.getActiveJobIds()
print(f"Active Job IDs: {list(active_jobs)}")
print("StatusTracker accessible for runtime telemetry capture.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Remediation Strategy for Shuffle Spill
# MAGIC Demonstrate configuration adjustments to eliminate shuffle spill.

# COMMAND ----------

# Remediation for Shuffle Spill:
# 1. Increase shuffle partitions to reduce data per partition:
spark.conf.set("spark.sql.shuffle.partitions", "16")

# 2. Adjust fraction of heap dedicated to execution/storage:
# spark.conf.set("spark.memory.fraction", "0.7")

# 3. Ensure AQE is enabled:
spark.conf.set("spark.sql.adaptive.enabled", "true")

print("Shuffle spill mitigation configurations applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Shuffle Spill (Memory/Disk) in the Spark UI is the #1 indicator of undersized shuffle partitions.**
# MAGIC - **Compare 75th percentile task duration to Max task duration in the Stages tab to detect task skew.**
# MAGIC - **If GC time exceeds 10% of task duration, executor JVMs are thrashing; tune off-heap memory or partition sizing.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `Spill (Memory)` and `Spill (Disk)` in the Spark UI Stages tab?
# MAGIC 2. How do you identify a data skew issue versus an undersized cluster issue using Spark UI metrics?
# MAGIC 3. What does high 'Task Deserialization Time' indicate in the Spark UI timeline view?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
