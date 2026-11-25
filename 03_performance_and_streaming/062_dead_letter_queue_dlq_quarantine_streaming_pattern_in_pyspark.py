# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 062: Dead-Letter Queue (DLQ) Quarantine Streaming Pattern in PySpark
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A real-time clickstream ingestion pipeline processes 10,000 events/second. A software release on the web client introduces malformed JSON payloads. Crashing the streaming query halts all company analytics; silently ignoring errors causes undetected data loss.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement the enterprise Dead-Letter Queue (DLQ) streaming pattern: split incoming streams into a clean Silver table and a Quarantine DLQ Delta table within `foreachBatch`, capturing failure stack traces and raw payloads without interrupting the live pipeline.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC A Dead-Letter Queue pattern ensures continuous pipeline uptime (zero downtime). Clean records flow unimpeded to production analytical tables, while corrupted, unparseable, or schema-violating rows are diverted to a quarantine table for engineering alerting and automated replay.

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
        .appName("UseCase_062_Dead_Letter_Queue_DLQ_Quarantine_Streaming_Pattern_in_PySpark")
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
# MAGIC ### Step 1: Simulating Mixed Clean and Corrupted Streaming Ingestion
# MAGIC Create a batch with valid and damaged message payloads.

# COMMAND ----------

quarantine_dlq_path = "/tmp/delta_dlq_062"
clean_silver_path = "/tmp/delta_clean_062"

for p in [quarantine_dlq_path, clean_silver_path]:
    if Path(p).exists():
        shutil.rmtree(p)

incoming_events = [
    ("E-01", '{"user_id": "U1", "amount": 150.0}'),
    ("E-02", 'CORRUPTED_NON_JSON_STRING'),
    ("E-03", '{"user_id": "U2", "amount": -9999.0}'), # Semantic business violation
    ("E-04", '{"user_id": "U3", "amount": 85.0}')
]

schema = "event_id STRING, raw_payload STRING"
df_incoming = spark.createDataFrame(incoming_events, schema)
df_incoming.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: DLQ Routing Logic with Schema Parsing and Validation
# MAGIC Parse JSON with schema and apply validation rules to segregate clean vs dirty rows.

# COMMAND ----------

payload_schema = T.StructType([
    T.StructField("user_id", T.StringType(), True),
    T.StructField("amount", T.DoubleType(), True)
])

# Attempt parsing
df_attempt = df_incoming.withColumn("parsed", F.from_json("raw_payload", payload_schema))

# Classify validation errors
df_classified = df_attempt.withColumn(
    "error_reason",
    F.when(F.col("parsed").isNull() | F.col("parsed.user_id").isNull(), "MALFORMED_JSON_SYNTAX")
     .when(F.col("parsed.amount") <= 0, "INVALID_NEGATIVE_AMOUNT")
     .otherwise(None)
)

df_classified.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Routing to Clean Silver Table vs Dead-Letter Queue Sink
# MAGIC Write clean records to Silver and route failed records to DLQ with audit metadata.

# COMMAND ----------

# 1. Clean stream
df_clean = df_classified.filter(F.col("error_reason").isNull()).select(
    "event_id",
    "parsed.user_id",
    "parsed.amount",
    F.current_timestamp().alias("processed_at")
)

df_clean.write.format("delta").mode("append").save(clean_silver_path)

# 2. Dead-letter quarantine stream
df_dlq = df_classified.filter(F.col("error_reason").isNotNull()).select(
    "event_id",
    "raw_payload",
    "error_reason",
    F.lit("PENDING_TRIAGE").alias("dlq_status"),
    F.current_timestamp().alias("quarantined_at")
)

df_dlq.write.format("delta").mode("append").save(quarantine_dlq_path)

print("Clean Silver Table:")
spark.read.format("delta").load(clean_silver_path).show(truncate=False)

print("Dead-Letter Queue Quarantine Table:")
spark.read.format("delta").load(quarantine_dlq_path).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Executable DLQ Replay, Remediation & End-to-End Reconciliation
# MAGIC Demonstrate the complete recovery cycle: read quarantined items, apply repair transformations, replay back into Silver, update DLQ status to `REPLAYED`, and verify reconciliation counts.

# COMMAND ----------

# 1. Read pending records from Quarantine DLQ
df_dlq_pending = (
    spark.read.format("delta").load(quarantine_dlq_path)
    .filter(F.col("dlq_status") == "PENDING_TRIAGE")
)

print(f"Quarantined Records Pending Replay: {df_dlq_pending.count()}")

# 2. Apply business repair logic based on error_reason
# For INVALID_NEGATIVE_AMOUNT: take absolute value
# For MALFORMED_JSON_SYNTAX: extract surrogate default or repaired payload
df_repaired = (
    df_dlq_pending
    .withColumn(
        "repaired_user_id",
        F.when(F.col("error_reason") == "MALFORMED_JSON_SYNTAX", F.lit("U_UNKNOWN_FALLBACK"))
         .otherwise(F.get_json_object("raw_payload", "$.user_id"))
    )
    .withColumn(
        "repaired_amount",
        F.when(F.col("error_reason") == "INVALID_NEGATIVE_AMOUNT", F.abs(F.get_json_object("raw_payload", "$.amount").cast("double")))
         .otherwise(F.lit(0.0))
    )
    .select(
        F.col("event_id"),
        F.col("repaired_user_id").alias("user_id"),
        F.col("repaired_amount").alias("amount"),
        F.current_timestamp().alias("processed_at")
    )
)

print("Repaired Records Ready for Replay:")
df_repaired.show(truncate=False)

# 3. Idempotent replay into Clean Silver table
df_repaired.write.format("delta").mode("append").save(clean_silver_path)

# 4. Update Quarantine DLQ table status to REPLAYED (Delta Lake ACID update)
from delta.tables import DeltaTable
dlq_delta = DeltaTable.forPath(spark, quarantine_dlq_path)
dlq_delta.update(
    condition=F.col("dlq_status") == "PENDING_TRIAGE",
    set={"dlq_status": F.lit("REPLAYED")}
)

print("Updated Dead-Letter Queue Status:")
spark.read.format("delta").load(quarantine_dlq_path).show(truncate=False)

# 5. Source-to-Target Reconciliation Check
total_incoming = df_incoming.count()
total_silver = spark.read.format("delta").load(clean_silver_path).count()
total_replayed = spark.read.format("delta").load(quarantine_dlq_path).filter(F.col("dlq_status") == "REPLAYED").count()

print("=== FINAL RECONCILIATION AUDIT ===")
print(f"Total Incoming Ingested: {total_incoming}")
print(f"Total Silver Committed: {total_silver} (Original Clean + Repaired)")
print(f"Total DLQ Replayed: {total_replayed}")
assert total_silver == total_incoming, f"Reconciliation Error: Silver count {total_silver} != Incoming {total_incoming}"
print("RECONCILIATION VERIFIED: Zero data loss across quarantine and replay cycle!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **The Dead-Letter Queue (DLQ) pattern isolates malformed data without stopping production streaming pipelines.**
# MAGIC - **Always capture the raw unparsed payload and an explicit `error_reason` column in the quarantine sink.**
# MAGIC - **Build replay mechanisms so that once upstream schema fixes are deployed, quarantined data can be re-ingested.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is a Dead Letter Queue (DLQ) pattern and why is it essential for 24/7 streaming SLAs?
# MAGIC 2. How do you alert on-call data engineers when the DLQ error rate exceeds a safety threshold?
# MAGIC 3. How do you re-process or replay records from a Delta DLQ table back into the main pipeline?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
