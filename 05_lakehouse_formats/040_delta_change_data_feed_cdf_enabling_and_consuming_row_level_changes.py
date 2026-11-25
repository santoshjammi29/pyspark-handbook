# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 040: Delta Change Data Feed (CDF): Enabling and Consuming Row-Level Changes
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An enterprise financial lakehouse architecture maintains a real-time risk dashboard that must react to downstream row-level modifications (inserts, updates, and deletes) occurring in the upstream customer ledger without re-scanning the entire table.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Enable Delta Change Data Feed (`delta.enableChangeDataFeed = true`). Execute inserts, updates, and deletes on the table. Query the resulting change stream using `readChangeFeed` to capture pre-images, post-images, and deleted records.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Delta Change Data Feed (CDF) generates row-level change events alongside commits. When updates occur, CDF logs both the `update_preimage` (row state before update) and `update_postimage` (row state after update), enabling exact financial double-entry reconciliation downstream.

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
        .appName("UseCase_040_Delta_Change_Data_Feed_CDF_Enabling_and_Consuming_Row_Level_Changes")
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
# MAGIC ### Step 1: Creating Delta Table with Change Data Feed Enabled
# MAGIC Enable CDF at table creation via table properties.

# COMMAND ----------

from delta.tables import DeltaTable

delta_cdf_path = "/tmp/delta_cdf_040"
if Path(delta_cdf_path).exists():
    shutil.rmtree(delta_cdf_path)

# Create table with CDF enabled
init_customers = [
    ("C-01", "Alice", 1000.0),
    ("C-02", "Bob", 2500.0)
]

spark.createDataFrame(init_customers, ["cust_id", "name", "credit_limit"])    .write.format("delta")    .option("delta.enableChangeDataFeed", "true")    .mode("overwrite")    .save(delta_cdf_path)

print("Delta Table initialized with Change Data Feed enabled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Executing Downstream Commits: Update, Delete & Insert
# MAGIC Apply subsequent transactions to generate change events.

# COMMAND ----------

dt = DeltaTable.forPath(spark, delta_cdf_path)

# Commit 1: Update credit limit for Alice
dt.update(condition=F.col("cust_id") == "C-01", set={"credit_limit": F.lit(1500.0)})

# Commit 2: Delete Bob
dt.delete(condition=F.col("cust_id") == "C-02")

# Commit 3: Insert Charlie
new_cust = [("C-03", "Charlie", 5000.0)]
spark.createDataFrame(new_cust, ["cust_id", "name", "credit_limit"])    .write.format("delta")    .mode("append")    .save(delta_cdf_path)

print("Applied 3 commits to generate CDF events.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Querying the Change Data Feed Stream
# MAGIC Read row-level changes across versions using readChangeFeed.

# COMMAND ----------

# Query CDF from version 0 to latest version
df_cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .load(delta_cdf_path)
)

print("Complete Change Data Feed Stream:")
df_cdf.select(
    "cust_id",
    "name",
    "credit_limit",
    "_change_type",
    "_commit_version",
    "_commit_timestamp"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Delta Change Data Feed captures row-level mutations: `insert`, `update_preimage`, `update_postimage`, and `delete`.**
# MAGIC - **Enables efficient incremental ETL architectures without doing expensive full-table diffs.**
# MAGIC - **CDF is queried using `.option('readChangeFeed', 'true')` with `startingVersion` or `startingTimestamp`.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `update_preimage` and `update_postimage` in Delta CDF?
# MAGIC 2. How does Delta Lake store CDF records physically on disk without duplicating data unnecessarily?
# MAGIC 3. How does Delta CDF enable downstream streaming pipelines to process updates and deletes incrementally?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
