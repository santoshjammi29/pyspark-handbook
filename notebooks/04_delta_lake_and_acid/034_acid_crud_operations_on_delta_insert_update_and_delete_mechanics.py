# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 034: ACID CRUD Operations on Delta: Insert, Update and Delete Mechanics
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A GDPR/CCPA privacy compliance pipeline requires modifying existing customer records: deleting individuals exercising 'Right to be Forgotten' and updating consent preferences without taking the database offline.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Execute ACID CRUD operations on Delta Lake using `DeltaTable` API and Spark SQL: append new records (`insert`), selectively modify attributes (`update`), and purge records (`delete`). Inspect file modifications and tombstone markers.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Delta Lake files are immutable. An UPDATE or DELETE operation reads existing Parquet files, rewrites modified rows into brand-new Parquet files, and writes tombstone `remove` actions into the Delta transaction log. The old files remain on disk for Time Travel until removed by `VACUUM`.

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
        .appName("UseCase_034_ACID_CRUD_Operations_on_Delta_Insert_Update_and_Delete_Mechanics")
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
# MAGIC ### Step 1: Initializing Customer Privacy Table in Delta
# MAGIC Create customers table with consent flags.

# COMMAND ----------

from delta.tables import DeltaTable

delta_privacy_path = "/tmp/delta_privacy_034"
if Path(delta_privacy_path).exists():
    shutil.rmtree(delta_privacy_path)

privacy_data = [
    ("U-01", "alice@corp.com", True, "2023-01-10"),
    ("U-02", "bob@spam.com", False, "2023-02-15"),
    ("U-03", "charlie@privacy.org", True, "2023-03-20")
]

df_priv = spark.createDataFrame(privacy_data, ["user_id", "email", "marketing_consent", "created_date"])
df_priv.write.format("delta").mode("overwrite").save(delta_privacy_path)

delta_table = DeltaTable.forPath(spark, delta_privacy_path)
delta_table.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Executing ACID UPDATE and DELETE via DeltaTable API
# MAGIC Update consent flags and execute GDPR deletion.

# COMMAND ----------

# 1. Update: Revoke consent for bob@spam.com
delta_table.update(
    condition=F.col("email") == "bob@spam.com",
    set={"marketing_consent": F.lit(False)}
)

# 2. Delete: Purge user exercising GDPR Right to be Forgotten (U-03)
delta_table.delete(condition=F.col("user_id") == "U-03")

print("Table State after UPDATE and DELETE:")
delta_table.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Inspecting Commit History & File Rewrites
# MAGIC Observe UPDATE and DELETE operations logged as distinct atomic transactions.

# COMMAND ----------

df_hist = delta_table.history()
df_hist.select("version", "operation", "operationMetrics").show(truncate=False)

print(f"Current version of table: {df_hist.first()['version']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Delta Lake brings row-level UPDATE and DELETE capabilities to Apache Spark without rewriting whole partitions manually.**
# MAGIC - **Updates and Deletes create new Parquet files and log `remove` actions in `_delta_log/` without immediately deleting physical files.**
# MAGIC - **All operations are fully ACID-compliant: concurrent readers always observe a consistent snapshot.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why are UPDATE and DELETE operations in Delta Lake relatively expensive compared to simple append writes?
# MAGIC 2. How does Copy-on-Write (CoW) in Delta Lake compare to Merge-on-Read (MoR) in Apache Iceberg/Hudi?
# MAGIC 3. What happens to the physical Parquet files on disk when a row is deleted in Delta Lake?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
