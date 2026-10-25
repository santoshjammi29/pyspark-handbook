# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 035: Delta Upsert and MERGE INTO Mastery with Multiple Conditions
# MAGIC
# MAGIC **Module:** Module 04: Delta Lake Architecture, ACID Operations & Time Travel
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A retail inventory management system synchronizes daily inventory updates from 500 physical warehouses. If an item exists in the central inventory table, update stock quantity and last_restocked date; if missing, insert the new SKU; if stock drops to -999 (discontinued), delete the item.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master the `MERGE INTO` API in Delta Lake: implement complex upserts with `whenMatchedUpdate`, `whenMatchedDelete`, `whenNotMatchedInsert`, and conditional update clauses to handle real-world CDC synchronization in a single atomic transaction.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Delta MERGE is an atomic operation that performs a full outer join between source updates and target Delta files to identify matching keys. It rewrites only the specific files containing matching rows, avoiding full table rewrites.

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
        .appName("UseCase_035_Delta_Upsert_and_MERGE_INTO_Mastery_with_Multiple_Conditions")
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
# MAGIC ### Step 1: Initializing Current Warehouse Inventory (Target Delta Table)
# MAGIC Create target inventory table.

# COMMAND ----------

from delta.tables import DeltaTable

delta_inv_path = "/tmp/delta_inventory_035"
if Path(delta_inv_path).exists():
    shutil.rmtree(delta_inv_path)

current_inventory = [
    ("SKU-101", "Wireless Mouse", 50, 24.99, "2023-11-01"),
    ("SKU-102", "Mechanical Keyboard", 20, 89.99, "2023-11-01"),
    ("SKU-103", "USB-C Hub", 15, 35.00, "2023-11-01"),
    ("SKU-104", "Obsolete Cable", 5, 4.99, "2023-10-01")
]

schema = "sku STRING, product_name STRING, quantity INT, unit_price DOUBLE, last_updated STRING"
spark.createDataFrame(current_inventory, schema).write.format("delta").mode("overwrite").save(delta_inv_path)

target_table = DeltaTable.forPath(spark, delta_inv_path)
print("Initial Target Inventory:")
target_table.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Preparing Inbound Daily Warehouse Updates (Source Batch)
# MAGIC Create batch with updates, new SKUs, and a discontinued item (quantity = -999).

# COMMAND ----------

incoming_updates = [
    ("SKU-101", "Wireless Mouse", 80, 24.99, "2023-11-20"),      # Update quantity to 80
    ("SKU-104", "Obsolete Cable", -999, 0.00, "2023-11-20"),      # Discontinued -> Delete
    ("SKU-105", "Noise Cancelling Headset", 40, 199.99, "2023-11-20") # New SKU -> Insert
]

df_updates = spark.createDataFrame(incoming_updates, schema)
print("Incoming Daily Updates Batch:")
df_updates.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Executing Multi-Condition MERGE INTO
# MAGIC Execute conditional update, conditional delete, and insert in a single atomic pass.

# COMMAND ----------

(
    target_table.alias("target")
    .merge(
        source=df_updates.alias("source"),
        condition="target.sku = source.sku"
    )
    .whenMatchedDelete(
        condition="source.quantity = -999"
    )
    .whenMatchedUpdate(
        set={
            "quantity": F.col("source.quantity"),
            "unit_price": F.col("source.unit_price"),
            "last_updated": F.col("source.last_updated")
        }
    )
    .whenNotMatchedInsert(
        values={
            "sku": F.col("source.sku"),
            "product_name": F.col("source.product_name"),
            "quantity": F.col("source.quantity"),
            "unit_price": F.col("source.unit_price"),
            "last_updated": F.col("source.last_updated")
        }
    )
    .execute()
)

print("Target Table after MERGE INTO:")
target_table.toDF().orderBy("sku").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`MERGE INTO` executes inserts, updates, and deletes in a single unified atomic transaction.**
# MAGIC - **Always ensure the source dataset is deduplicated on the merge condition key; duplicate source keys will throw a `DeltaAnalysisException`.**
# MAGIC - **Specify partition pruning filters in the merge condition whenever merging into partitioned Delta tables to skip untouched partitions.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What happens when the source table in a Delta `MERGE` statement contains duplicate keys?
# MAGIC 2. How does Delta Lake's merge operation identify which Parquet files need to be rewritten?
# MAGIC 3. How can you optimize a MERGE operation on a multi-terabyte partitioned Delta table?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
