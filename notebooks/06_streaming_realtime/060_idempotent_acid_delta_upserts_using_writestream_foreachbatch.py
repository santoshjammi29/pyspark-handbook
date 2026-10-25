# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 060: Idempotent ACID Delta Upserts using writeStream.foreachBatch
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A payments streaming pipeline consumes live financial transactions that include both new purchases and refund updates to existing transactions. Standard streaming `append` creates duplicate rows; the stream must perform idempotent ACID `MERGE INTO` operations into target Delta tables.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master `writeStream.foreachBatch()`: design a micro-batch function that executes an atomic `DeltaTable.merge()` operation, ensures idempotency across micro-batch retries, and avoids race conditions in distributed streaming sinks.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC `foreachBatch` gives access to the output DataFrame of each micro-batch as a standard static DataFrame. This allows executing non-streaming operations—such as Delta Lake `MERGE INTO`, writing to multiple destinations, or executing custom API calls—inside a streaming query.

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
        .appName("UseCase_060_Idempotent_ACID_Delta_Upserts_using_writeStreamforeachBatch")
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
# MAGIC ### Step 1: Initializing Target Delta Silver Ledger Table
# MAGIC Create target Delta table for transactions.

# COMMAND ----------

from delta.tables import DeltaTable

silver_ledger_path = "/tmp/delta_silver_ledger_060"
if Path(silver_ledger_path).exists():
    shutil.rmtree(silver_ledger_path)

initial_txns = [
    ("TXN-101", "CUST-1", 100.0, "PENDING", "2023-11-20 10:00:00"),
    ("TXN-102", "CUST-2", 250.0, "SETTLED", "2023-11-20 10:05:00")
]

schema = "txn_id STRING, customer_id STRING, amount DOUBLE, status STRING, updated_at STRING"
spark.createDataFrame(initial_txns, schema).write.format("delta").mode("overwrite").save(silver_ledger_path)

target_delta = DeltaTable.forPath(spark, silver_ledger_path)
print("Initial Silver Ledger Delta Table:")
target_delta.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Defining the Idempotent foreachBatch Upsert Function
# MAGIC Implement micro-batch merge function with deduplication and ACID upsert.

# COMMAND ----------

def upsert_to_delta(micro_batch_df, batch_id):
    """
    Invoked for each micro-batch. Performs idempotent MERGE into target Delta table.
    """
    print(f"--> Processing Micro-Batch ID: {batch_id} (Rows: {micro_batch_df.count()})")

    # 1. Deduplicate micro-batch by taking the latest update per txn_id
    window_spec = Window.partitionBy("txn_id").orderBy(F.col("updated_at").desc())
    deduped_batch = micro_batch_df.withColumn("rn", F.row_number().over(window_spec))                                 .filter(F.col("rn") == 1)                                 .drop("rn")

    # 2. Atomic MERGE into target Delta table
    target_table = DeltaTable.forPath(spark, silver_ledger_path)
    (
        target_table.alias("target")
        .merge(
            source=deduped_batch.alias("source"),
            condition="target.txn_id = source.txn_id"
        )
        .whenMatchedUpdate(
            set={
                "amount": F.col("source.amount"),
                "status": F.col("source.status"),
                "updated_at": F.col("source.updated_at")
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"--> Micro-Batch {batch_id} merged successfully into Delta table.")

print("foreachBatch upsert function defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Testing foreachBatch Execution with Incoming Stream Batch
# MAGIC Simulate streaming micro-batch executing the upsert logic.

# COMMAND ----------

# Incoming micro-batch: TXN-101 is updated from PENDING to SETTLED; TXN-103 is a new transaction
micro_batch_1 = [
    ("TXN-101", "CUST-1", 100.0, "SETTLED", "2023-11-20 10:15:00"), # Update
    ("TXN-103", "CUST-3", 75.0,  "SETTLED", "2023-11-20 10:20:00")  # Insert
]

df_mb1 = spark.createDataFrame(micro_batch_1, schema)

# Execute the upsert function directly (simulating writeStream.foreachBatch)
upsert_to_delta(df_mb1, batch_id=1)

print("Final Target Delta Table (Notice TXN-101 updated, TXN-103 inserted):")
target_delta.toDF().orderBy("txn_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`foreachBatch()` bridges streaming DataFrames with batch APIs like Delta Lake `MERGE INTO`.**
# MAGIC - **Always deduplicate the micro-batch DataFrame before calling `merge()` to avoid `DeltaAnalysisException` on duplicate source keys.**
# MAGIC - **Ensure operations inside `foreachBatch` are idempotent so pipeline retries upon failure do not duplicate data.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why can't you use standard `.writeStream.format('delta')` with `.mode('update')` directly in PySpark?
# MAGIC 2. How does `foreachBatch` handle failure recovery and ensure exactly-once semantics?
# MAGIC 3. What performance issues can arise if the micro-batch in `foreachBatch` is not pre-deduplicated?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
