# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 031: Broadcast Hash Joins, Threshold Tuning and Memory Limits
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A point-of-sale stream joins billions of transaction rows with a small 2MB store metadata lookup table. Default join configuration causes an unnecessary multi-gigabyte shuffle across all executors, degrading cluster performance.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Eliminate shuffle overhead using Broadcast Hash Joins: apply explicit `broadcast(df)` hint, inspect the Catalyst physical plan to verify `BroadcastHashJoinExec`, understand `spark.sql.autoBroadcastJoinThreshold`, and identify the risks of driver OutOfMemory (OOM) when broadcasting large tables.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC In a Broadcast Hash Join, the driver downloads the small DataFrame, builds an in-memory hash table, and broadcasts it to all worker executors via Spark's peer-to-peer torrent protocol. Workers join streaming partition data locally with zero network shuffle. The default auto-broadcast threshold is 10MB (10485760 bytes).

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
        .appName("UseCase_031_Broadcast_Hash_Joins_Threshold_Tuning_and_Memory_Limits")
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
# MAGIC ### Step 1: Generating Fact and Small Dimension Tables
# MAGIC Create large transaction table and small store dimension.

# COMMAND ----------

# Simulate 20,000 transactions (Fact Table)
df_transactions = spark.range(1, 20001, 1, 4).select(
    F.col("id").alias("txn_id"),
    (F.col("id") % 10 + 1).alias("store_id"),
    F.round(F.rand(seed=42) * 200 + 5, 2).alias("amount")
)

# Small Store Dimension Table (10 rows)
stores = [
    (1, "Downtown Flagship", "New York"),
    (2, "Airport Kiosk", "Chicago"),
    (3, "Suburban Mall", "Dallas"),
    (4, "Tech Center", "San Francisco"),
    (5, "Metro Hub", "Atlanta"),
    (6, "Harbor Outlet", "Boston"),
    (7, "Plaza Central", "Seattle"),
    (8, "Galleria", "Houston"),
    (9, "Old Town", "Denver"),
    (10, "Express Mart", "Miami")
]
df_stores = spark.createDataFrame(stores, ["store_id", "store_name", "city"])

print(f"Transactions: {df_transactions.count()} | Stores: {df_stores.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Applying Explicit Broadcast Join Hint & Physical Plan Inspection
# MAGIC Force broadcast join using F.broadcast() and verify execution plan.

# COMMAND ----------

# Join with broadcast hint
df_joined = df_transactions.join(F.broadcast(df_stores), on="store_id", how="inner")

print("Joined Sample:")
df_joined.show(5)

print("=== PHYSICAL PLAN (Notice BroadcastHashJoinExec and absence of Exchange/Shuffle) ===")
df_joined.explain(mode="simple")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Understanding autoBroadcastJoinThreshold
# MAGIC Inspect and tune the auto-broadcast threshold in Spark configurations.

# COMMAND ----------

current_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"Current Auto Broadcast Threshold: {int(current_threshold) / (1024*1024):.1f} MB")

# To disable auto-broadcast: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# To increase threshold to 50MB: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Broadcast joins completely eliminate shuffle exchange, providing order-of-magnitude speedups for asymmetric table joins.**
# MAGIC - **The small broadcasted table is collected to the driver node first—broadcasting tables larger than driver memory triggers Driver OOM.**
# MAGIC - **Default auto-broadcast threshold is 10MB; setting to -1 disables auto-broadcasting.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How does the Driver node distribute a broadcasted table to worker executors?
# MAGIC 2. What error occurs if you broadcast a DataFrame that is larger than the Driver's available memory?
# MAGIC 3. Why can broadcast joins fail or hang on datasets with massive row counts even if disk size is small?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
