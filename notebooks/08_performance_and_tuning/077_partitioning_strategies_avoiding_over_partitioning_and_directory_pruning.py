# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 077: Partitioning Strategies: Avoiding Over-Partitioning and Directory Pruning
# MAGIC
# MAGIC **Module:** Module 08: Performance Tuning, Query Optimization & Skew Mitigation
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A telemetry pipeline partitioned data by `year/month/day/hour/device_id`. After 6 months, the table contained 2 million partition directories containing tiny 10KB files. Querying the table caused driver timeouts during file listing before reading a single byte of data.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Evaluate big data partitioning best practices: compare high-cardinality vs low-cardinality partitioning columns, demonstrate Partition Pruning (skipping partition directories), and explain why target partition file sizes must be between 100MB and 1GB.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Hive-style partitioning creates physical filesystem directories (`/date=2023-11-20/`). Partitioning by high-cardinality columns (e.g. timestamp, user_id) generates millions of tiny directories (over-partitioning), overwhelming cloud storage metadata APIs. Partitioning should only be applied to low-cardinality columns (e.g. date, region) where each partition holds at least 1GB of data.

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
        .appName("UseCase_077_Partitioning_Strategies_Avoiding_Over_Partitioning_and_Directory_Pruning")
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
# MAGIC ### Step 1: Simulating Partition Directory Structure on Disk
# MAGIC Create an appropriately partitioned table by year and region.

# COMMAND ----------

partition_path = "/tmp/delta_partitions_077"
if Path(partition_path).exists():
    shutil.rmtree(partition_path)

records = [
    ("ORD-1", 2023, "US", 100.0),
    ("ORD-2", 2023, "US", 250.0),
    ("ORD-3", 2023, "EU", 180.0),
    ("ORD-4", 2022, "US", 90.0),
    ("ORD-5", 2022, "EU", 320.0)
]

schema = "order_id STRING, year INT, region STRING, amount DOUBLE"
df_orders = spark.createDataFrame(records, schema)

# Partition by year and region
df_orders.write.format("delta").partitionBy("year", "region").mode("overwrite").save(partition_path)

print(f"Table partitioned by year and region at {partition_path}")
print("Generated physical partition directories:")
for p in sorted(list(Path(partition_path).glob("*/*"))):
    if p.is_dir() and not p.name.startswith("_"):
        print(f"  {p.parent.name}/{p.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Verifying Partition Pruning in Catalyst Plan
# MAGIC Filter by year and region and verify partition directory skipping in explain plan.

# COMMAND ----------

df_pruned = spark.read.format("delta").load(partition_path).filter((F.col("year") == 2023) & (F.col("region") == "US"))

print("=== PHYSICAL PLAN (Notice PartitionFilters: [EqualTo(year,2023), EqualTo(region,US)]) ===")
df_pruned.explain(mode="formatted")
df_pruned.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Over-Partitioning Golden Rules Cheatsheet
# MAGIC Review engineering guidelines for selecting partition columns.

# COMMAND ----------

rules = [
    ("Target Partition Size", "Each physical partition directory should contain at least 1GB of data."),
    ("Max Partition Count", "Keep total partition count across the entire table under 10,000 directories."),
    ("Avoid Timestamps", "Never partition by timestamp or hour unless processing multi-terabytes per hour."),
    ("Modern Alternative", "In Databricks, prefer Liquid Clustering (`CLUSTER BY`) over traditional Hive partitioning!")
]

for title, rule in rules:
    print(f" {title}: {rule}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Partition pruning skips scanning entire filesystem directories when partition columns are filtered in the WHERE clause.**
# MAGIC - **Over-partitioning causes millions of tiny files, slow metadata discovery, and driver out-of-memory crashes.**
# MAGIC - **Each partition directory should contain at least 100MB to 1GB of data.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What happens under the hood when a query filters on a partition column versus a non-partition column?
# MAGIC 2. What is the negative consequence of partitioning a table by `customer_id` or `timestamp`?
# MAGIC 3. How does Databricks Liquid Clustering eliminate the traditional dilemmas of partition key selection?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
