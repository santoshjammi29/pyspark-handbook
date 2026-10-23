# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 002: Schema Enforcement, Safe Type Casting & Try-Cast Handling
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API  
# MAGIC **Difficulty:** `Beginner`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A payments processor receives raw transaction dumps where numeric amounts contain currency symbols, timestamps are in non-standard formats, and corrupted text entries cause pipeline aborts on type casting.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement resilient schema transformations: safely cast string columns to numeric/date types, handle malformed rows using `try_cast` (or regex sanitization), and separate valid records from corrupted data without halting execution.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC In ANSI SQL mode (`spark.sql.ansi.enabled = true`), standard `.cast()` throws runtime exceptions on invalid values (e.g. casting 'INVALID' to INT). Using `try_cast` or `when` expressions ensures invalid values convert to NULL rather than failing long-running batch jobs.

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
    .appName("UseCase_002_Schema_Enforcement,_Safe_Type_Casting_and_Try-Cast_Handling")
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
# MAGIC ## 5. Mocking Raw Unclean Ingestion Feed
# MAGIC We simulate an external vendor feed with varied formatting, currency signs, and corrupt values.

# COMMAND ----------

raw_feed = [
    ("TXN-001", "$1,450.50", "2023-10-01 10:30:00", "APPROVED"),
    ("TXN-002", "€320.00", "01/10/2023 11:15", "APPROVED"),
    ("TXN-003", "CORRUPTED_AMT", "2023-10-02", "FAILED"),
    ("TXN-004", "$8,900.75", "2023-10-03 14:00:00", "APPROVED"),
    ("TXN-005", "N/A", "INVALID_DATE", "PENDING")
]

df_raw = spark.createDataFrame(raw_feed, ["txn_id", "raw_amount", "raw_timestamp", "status"])
df_raw.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sanitization & Resilient Type Casting
# MAGIC Strip currency symbols, commas, and employ `regexp_replace` paired with safe casting.

# COMMAND ----------

# 1. Cleanse raw_amount by stripping non-numeric characters (except period)
clean_amount_expr = F.regexp_replace(F.col("raw_amount"), r"[^\d.]", "")

# 2. Apply safe casting: if sanitized string is empty or invalid, evaluate to NULL
df_cleansed = df_raw.withColumn(
    "clean_amount",
    F.when(clean_amount_expr == "", None).otherwise(clean_amount_expr.cast(T.DoubleType()))
)

# 3. Standardize timestamp handling
df_typed = df_cleansed.withColumn(
    "txn_timestamp",
    F.coalesce(
        F.to_timestamp(F.col("raw_timestamp"), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col("raw_timestamp"), "dd/MM/yyyy HH:mm"),
        F.to_timestamp(F.col("raw_timestamp"), "yyyy-MM-dd")
    )
)

df_typed.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Quarantine Corrupted Records vs Clean Silver Stream
# MAGIC Split data into valid records and a quarantine/dead-letter dataset for investigation.

# COMMAND ----------

# Valid records have both parsed amount and parsed timestamp
df_valid = df_typed.filter(F.col("clean_amount").isNotNull() & F.col("txn_timestamp").isNotNull())
df_quarantine = df_typed.filter(F.col("clean_amount").isNull() | F.col("txn_timestamp").isNull())

print(f"Valid Silver Records: {df_valid.count()}")
df_valid.show(truncate=False)

print(f"Quarantined Dead-Letter Records: {df_quarantine.count()}")
df_quarantine.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **In Spark 3.2+, `F.expr('try_cast(col AS TYPE)')` safely converts invalid strings to NULL without raising runtime exceptions.**
# MAGIC - **Always quarantine records where mission-critical keys or metrics fail type validation.**
# MAGIC - **Combine `F.coalesce` with multiple date format strings to parse heterogeneous date inputs in a single distributed pass.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the impact of `spark.sql.ansi.enabled` on type casting in Spark 3.x?
# MAGIC 2. How does `try_cast` differ from standard `cast` in terms of pipeline failure recovery?
# MAGIC 3. How do you design a Silver layer to capture unparseable rows without failing the entire stream?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
