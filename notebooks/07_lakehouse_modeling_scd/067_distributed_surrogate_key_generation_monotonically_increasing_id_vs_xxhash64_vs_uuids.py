# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 067: Distributed Surrogate Key Generation: monotonically_increasing_id vs xxhash64 vs UUIDs
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A lakehouse dimensional model builds customer and account dimension tables. Business analysts require synthetic integer or hash surrogate keys to decouple dimensional records from operational natural keys and enable SCD Type 2 version tracking.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Evaluate distributed surrogate key generation strategies in PySpark: (1) `monotonically_increasing_id()`, (2) Deterministic cryptographic hashing (`xxhash64` / `sha2`), and (3) Universally unique identifiers (`uuid`). Contrast deterministic replayability, performance, and storage footprint.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC `monotonically_increasing_id()` generates 64-bit integers where the upper 33 bits represent the partition ID and the lower 31 bits represent the row ID within the partition. While fast, IDs are NOT contiguous and change if the partition count changes. `xxhash64(concat_ws())` produces deterministic 64-bit hash keys that remain identical across pipeline replays.

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
        .appName("UseCase_067_Distributed_Surrogate_Key_Generation_monotonically_increasing_id_vs_xxhash64_vs_UUIDs")
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
# MAGIC ### Step 1: Generating Customer Natural Keys Across Partitions
# MAGIC Create customers distributed across multiple partitions.

# COMMAND ----------

natural_customers = [
    ("NAT-1001", "Alice Smith", "2023-01-10"),
    ("NAT-1002", "Bob Jones", "2023-02-15"),
    ("NAT-1003", "Charlie Day", "2023-03-20"),
    ("NAT-1004", "Diana Prince", "2023-04-25")
]

schema = "natural_key STRING, name STRING, created_at STRING"
df_cust = spark.createDataFrame(natural_customers, schema).repartition(2)
print(f"Partitions: {df_cust.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Comparing Key Generation Strategies
# MAGIC Implement monotonically_increasing_id, xxhash64, and UUID generation.

# COMMAND ----------

df_keys = (
    df_cust
    # 1. Monotonically increasing ID (Fast, non-contiguous, partition-dependent)
    .withColumn("surrogate_monotonic_id", F.monotonically_increasing_id())
    # 2. Deterministic 64-bit integer hash (Replayable, 100% deterministic, 8 bytes)
    .withColumn("surrogate_xxhash64", F.xxhash64(F.col("natural_key"), F.col("created_at")))
    # 3. MD5/SHA256 string hash (Deterministic, 32/64 character string, higher storage cost)
    .withColumn("surrogate_sha256", F.sha2(F.col("natural_key"), 256))
    # 4. Random UUID (Unique, non-deterministic across pipeline reruns)
    .withColumn("surrogate_uuid", F.expr("uuid()"))
)

df_keys.select("natural_key", "surrogate_monotonic_id", "surrogate_xxhash64", "surrogate_uuid").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Deterministic Replayability Verification
# MAGIC Verify that xxhash64 produces the exact same key upon re-running the pipeline.

# COMMAND ----------

# Re-evaluating xxhash64 on the same inputs yields identical hash values
df_recheck = df_cust.withColumn("recheck_hash", F.xxhash64(F.col("natural_key"), F.col("created_at")))
diff_count = df_keys.join(df_recheck, on="natural_key").filter(F.col("surrogate_xxhash64") != F.col("recheck_hash")).count()

assert diff_count == 0, "Deterministic hash mismatch detected!"
print("xxhash64 verified as 100% deterministic across distributed runs!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`xxhash64(concat_ws('||', *keys))` is the recommended surrogate key strategy for distributed Lakehouses: 64-bit integer, fast, and 100% deterministic.**
# MAGIC - **`monotonically_increasing_id()` generates gaps between partitions and changes if data is repartitioned.**
# MAGIC - **UUIDs are non-deterministic and prevent idempotent pipeline reruns.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How does `monotonically_increasing_id()` construct its 64-bit integer across partitions?
# MAGIC 2. Why is `xxhash64` preferred over UUIDs for dimensional surrogate keys in Lakehouse architectures?
# MAGIC 3. What is the risk of hash collisions in 64-bit hashes on tables with billions of rows?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
