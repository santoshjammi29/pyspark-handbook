# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 057: Stream-Static Joins with Slowly Changing Dimension Lookups
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A financial fraud detection system processes streaming credit card swipe authorizations. Each incoming transaction contains only a `customer_id`. The stream must be joined with a static Customer Profile table to enrich swipes with user credit limit, billing address, and risk score.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement Stream-Static relational joins in PySpark: join an unbounded streaming DataFrame with a static Delta dimension table, observe how Spark automatically refreshes static table reads across micro-batches, and evaluate performance considerations.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC In a Stream-Static join, the static DataFrame is re-evaluated by Spark at the beginning of each streaming micro-batch. If the static table is stored in Delta Lake, updates made to the static table are picked up automatically by subsequent micro-batches without restarting the streaming query.

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
    .appName("UseCase_057_Stream-Static_Joins_with_Slowly_Changing_Dimension_Lookups")
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
# MAGIC ## 5. Creating Static Customer Profile Dimension Table in Delta
# MAGIC Create static customer lookup table.

# COMMAND ----------

static_dim_path = "/tmp/delta_dim_customers_057"
if Path(static_dim_path).exists():
    shutil.rmtree(static_dim_path)

cust_profiles = [
    ("C-101", "Alice Smith", "PLATINUM", 25000.0, "US"),
    ("C-102", "Bob Jones", "GOLD", 10000.0, "UK"),
    ("C-103", "Charlie Day", "STANDARD", 2000.0, "CA")
]

schema = "customer_id STRING, customer_name STRING, tier STRING, credit_limit DOUBLE, country STRING"
spark.createDataFrame(cust_profiles, schema).write.format("delta").mode("overwrite").save(static_dim_path)

# Load as static DataFrame
df_static_customers = spark.read.format("delta").load(static_dim_path)
print("Static Customer Dimension Table:")
df_static_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulating Streaming Transaction Batch
# MAGIC Create streaming transactions to join against static customer dimension.

# COMMAND ----------

transactions = [
    ("TXN-01", "C-101", 450.00, "2023-11-20 14:00:00"),
    ("TXN-02", "C-102", 12000.00, "2023-11-20 14:02:00"), # Exceeds credit limit!
    ("TXN-03", "C-103", 85.50, "2023-11-20 14:05:00"),
    ("TXN-04", "C-999", 50.00, "2023-11-20 14:10:00")    # Unknown customer
]

schema_txn = "txn_id STRING, customer_id STRING, amount DOUBLE, txn_time STRING"
df_stream_mock = spark.createDataFrame(transactions, schema_txn)
df_stream_mock.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Executing Stream-Static Join & Fraud Flag Derivation
# MAGIC Join streaming authorizations with static profile to detect credit limit breaches.

# COMMAND ----------

# Left join streaming transactions with static customer profiles
df_enriched_stream = df_stream_mock.join(
    df_static_customers,
    on="customer_id",
    how="left"
).withColumn(
    "is_limit_breached",
    F.when(F.col("amount") > F.col("credit_limit"), True).otherwise(False)
).withColumn(
    "fraud_alert",
    F.when(F.col("customer_name").isNull(), "UNKNOWN_CUSTOMER")
     .when(F.col("is_limit_breached"), "CREDIT_LIMIT_EXCEEDED")
     .otherwise("APPROVED")
)

print("Enriched Stream Results with Fraud Flagging:")
df_enriched_stream.select(
    "txn_id", "customer_id", "customer_name", "amount", "credit_limit", "fraud_alert"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Stream-Static joins do NOT require state tracking or watermarks because the static table is fully bounded.**
# MAGIC - **When joining with a static Delta table, Spark reads the latest snapshot of the Delta table for each micro-batch.**
# MAGIC - **Outer stream-static joins are supported: `stream.join(static, how='left')` retains all stream records.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. Does a Stream-Static join require watermarking or state management?
# MAGIC 2. What happens if new rows are added to the static Delta table while the stream is running?
# MAGIC 3. Can you perform a right-outer join where the stream is on the right and static table is on the left?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
