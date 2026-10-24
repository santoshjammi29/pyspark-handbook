# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 075: Adaptive Query Execution (AQE): Dynamic Coalescing, Join Switching and Skew Joins
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A 2TB batch ETL pipeline runs with `spark.sql.shuffle.partitions = 200`. During filtering stages, data volume drops from 2TB to 50MB, but Spark still creates 200 tiny shuffle partitions, wasting task scheduling overhead. At joins, sudden data skew causes single task stragglers.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master Adaptive Query Execution (AQE) enabled by default in Spark 3.0+: demonstrate (1) Dynamically coalescing shuffle partitions, (2) Dynamically converting Sort-Merge Join to Broadcast Hash Join at runtime, and (3) Dynamic skew join optimization (`spark.sql.adaptive.skewJoin.enabled`).
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Traditional Spark plans are fixed before execution starts. AQE introduces feedback loops: at shuffle stage boundaries, AQE inspects runtime partition statistics. If a table became small after filtering, AQE dynamically switches to BroadcastHashJoin; if partitions are tiny, it coalesces them dynamically.

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
        .appName("UseCase_075_Adaptive_Query_Execution_AQE_Dynamic_Coalescing_Join_Switching_and_Skew_Joins")
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
# MAGIC ### Step 1: Verifying AQE Configuration in SparkSession
# MAGIC Check core Adaptive Query Execution settings.

# COMMAND ----------

aqe_configs = [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
    "spark.sql.adaptive.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor"
]

print("Current SparkSession AQE Configurations:")
for conf in aqe_configs:
    print(f"  {conf} = {spark.conf.get(conf, 'NOT_SET')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Demonstrating Dynamic Partition Coalescing
# MAGIC Observe how AQE coalesces 200 initial shuffle partitions down to optimal task count.

# COMMAND ----------

# Set high initial shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "20")

# Run query that filters heavily
df_heavy_filter = (
    spark.range(1, 100000, 1, 8)
    .filter(F.col("id") < 100) # Reduces data to tiny 99 rows
    .groupBy(F.col("id") % 5)
    .count()
)

df_heavy_filter.collect()

print("=== AQE PHYSICAL PLAN (Notice 'AdaptiveSparkPlan' node) ===")
df_heavy_filter.explain(mode="simple")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Understanding Dynamic Skew Join Splitting
# MAGIC Explain how AQE splits skewed partition tasks into parallel sub-tasks.

# COMMAND ----------

# AQE Skew Join Mechanics:
# When a partition exceeds:
# (partitionSize > skewedPartitionFactor * medianPartitionSize) AND (partitionSize > skewedPartitionThresholdInBytes)
# AQE automatically splits the large skewed partition into multiple sub-partitions,
# pairing each sub-partition with the matching key on the other side!

print("AQE Dynamic Skew Join split criteria mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **AQE uses runtime statistics collected at shuffle stage boundaries to re-optimize execution plans on the fly.**
# MAGIC - **Dynamic Partition Coalescing prevents small partition task scheduling overhead after selective filters.**
# MAGIC - **Dynamic Skew Join automatically splits slow straggler partitions without manual salting code.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What are the three major optimization features provided by Adaptive Query Execution (AQE)?
# MAGIC 2. How does AQE dynamically switch a Sort-Merge Join to a Broadcast Hash Join at runtime?
# MAGIC 3. What metric threshold triggers AQE dynamic skew join handling?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
