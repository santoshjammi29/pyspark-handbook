# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 020: Handling Corrupted & Malformed JSON: Permissive vs DropMalformed vs FailFast
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A public data ingestion pipeline scrapes third-party web endpoints. Up to 5% of incoming payloads contain syntax errors (truncated braces, mismatched quotes, non-JSON tokens). The pipeline must never crash and must capture all corrupted lines into an audit column.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Compare Spark's JSON read modes: `PERMISSIVE`, `DROPMALFORMED`, and `FAILFAST`. Configure the `columnNameOfCorruptRecord` option to capture raw malformed strings into a `_corrupt_record` quarantine column for compliance review.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC `PERMISSIVE` is Spark's default mode: corrupted fields evaluate to NULL and the unparseable string is diverted to `_corrupt_record`. `FAILFAST` halts execution immediately upon encountering syntax errors. `DROPMALFORMED` silently drops damaged records.

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
    .appName("UseCase_020_Handling_Corrupted_and_Malformed_JSON_Permissive_vs_DropMalformed_vs_FailFast")
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
# MAGIC ## 5. Creating Dataset with Intentionally Malformed JSON Strings
# MAGIC Simulate mixed clean and corrupted JSON inputs.

# COMMAND ----------

bad_json_lines = [
    '{"id": "EVT-1", "user": "alice", "score": 95}',
    '{"id": "EVT-2", "user": "bob", "score": INVALID_NUMBER}', # Malformed token
    '{"id": "EVT-3", "user": "charlie", "score": 82}',
    'NOT_EVEN_JSON_PAYLOAD',                                      # Completely unparseable
    '{"id": "EVT-4", "user": "diana", "score": 100}'
]

rdd_dirty = spark.sparkContext.parallelize(bad_json_lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resilient Ingestion with PERMISSIVE Mode & _corrupt_record
# MAGIC Define schema with _corrupt_record to quarantine broken payloads.

# COMMAND ----------

# Define schema including special corrupt record column
quarantine_schema = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("user", T.StringType(), True),
    T.StructField("score", T.IntegerType(), True),
    T.StructField("_corrupt_record", T.StringType(), True)
])

# Read with PERMISSIVE mode
df_permissive = (
    spark.read
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(quarantine_schema)
    .json(rdd_dirty)
)

print("Ingested Records with Quarantine Column:")
df_permissive.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Quarantine Routing: Splitting Clean Records from Corrupted Rows
# MAGIC Filter clean rows for production and route corrupt payloads to a dead-letter sink.

# COMMAND ----------

df_clean = df_permissive.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
df_dead_letter = df_permissive.filter(F.col("_corrupt_record").isNotNull()).select("_corrupt_record")

print(f"Clean Records: {df_clean.count()}")
df_clean.show()

print(f"Corrupted Dead-Letter Records: {df_dead_letter.count()}")
df_dead_letter.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`PERMISSIVE` mode ensures zero pipeline downtime by capturing unparseable strings into `_corrupt_record`.**
# MAGIC - **`FAILFAST` is recommended for high-integrity financial reconciliation where no data corruption can be tolerated.**
# MAGIC - **`DROPMALFORMED` should be used with extreme caution because it silently discards records without audit trails.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What are the three JSON read modes supported by Apache Spark?
# MAGIC 2. How does Databricks Auto Loader's `_rescued_data` column differ from Spark's `_corrupt_record`?
# MAGIC 3. Why must `_corrupt_record` be explicitly declared in the StructType schema?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
