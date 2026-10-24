# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 036: Delta Time Travel, Audit History and Version Rollback
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A bug in an automated ETL pricing script mistakenly discounted all luxury goods by 90% during an overnight run. The operations team needs to audit what happened, query the table as of yesterday morning, and roll back the table to the pre-bug version.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master Delta Lake Time Travel: inspect table transaction history (`DESCRIBE HISTORY`), query previous states using `VERSION AS OF` and `TIMESTAMP AS OF`, verify data recovery, and restore the table to an earlier version using `RESTORE TABLE`.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Delta Lake maintains physical Parquet files from previous commits until `VACUUM` is executed. Reading a historical version reconstructs the file list from the corresponding `_delta_log` JSON commit file without modifying current data.

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
        .appName("UseCase_036_Delta_Time_Travel_Audit_History_and_Version_Rollback")
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
# MAGIC ### Step 1: Creating Product Pricing Table with Multiple Commits
# MAGIC Simulate three distinct commits: initial data, good update, and accidental bad update.

# COMMAND ----------

delta_price_path = "/tmp/delta_pricing_036"
if Path(delta_price_path).exists():
    shutil.rmtree(delta_price_path)

# Version 0: Initial prices
v0_data = [("PROD-1", 100.0), ("PROD-2", 500.0), ("PROD-3", 1000.0)]
spark.createDataFrame(v0_data, ["id", "price"]).write.format("delta").mode("overwrite").save(delta_price_path)

# Version 1: Routine price increase
v1_data = [("PROD-1", 110.0), ("PROD-2", 525.0), ("PROD-3", 1050.0)]
spark.createDataFrame(v1_data, ["id", "price"]).write.format("delta").mode("overwrite").save(delta_price_path)

# Version 2: Accidental buggy price collapse (Buggy ETL run!)
v2_data = [("PROD-1", 11.0), ("PROD-2", 52.5), ("PROD-3", 105.0)]
spark.createDataFrame(v2_data, ["id", "price"]).write.format("delta").mode("overwrite").save(delta_price_path)

print("Current (Corrupted) Table Version 2:")
spark.read.format("delta").load(delta_price_path).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Auditing Commit History via DESCRIBE HISTORY
# MAGIC Inspect all commits to determine the correct pre-incident version.

# COMMAND ----------

df_history = spark.sql(f"DESCRIBE HISTORY delta.`{delta_price_path}`")
df_history.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Time Travel Querying & Safe Rollback via RESTORE
# MAGIC Query version 1 directly and restore the table.

# COMMAND ----------

# Query Version 1 directly via Time Travel
print("Querying Version 1 (Pre-bug snapshot):")
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(delta_price_path)
df_v1.show()

# Restore table to Version 1 using RESTORE command (creates Version 3 that matches Version 1)
spark.sql(f"RESTORE TABLE delta.`{delta_price_path}` TO VERSION AS OF 1")

print("Table Successfully Restored to Pre-Incident State:")
spark.read.format("delta").load(delta_price_path).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Time Travel allows querying historical states via `.option('versionAsOf', v)` or `.option('timestampAsOf', ts)`.**
# MAGIC - **`RESTORE TABLE ... TO VERSION AS OF n` rolls back a table atomically without manually copying files.**
# MAGIC - **Time travel availability is bounded by the retention period configured before `VACUUM` cleans old files.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How does Delta Lake read historical versions without storing duplicate copies of unchanged data?
# MAGIC 2. What is the impact of running `VACUUM` on the ability to query historical table versions?
# MAGIC 3. Does `RESTORE TABLE` erase the bad commits from the transaction history or create a new commit?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
