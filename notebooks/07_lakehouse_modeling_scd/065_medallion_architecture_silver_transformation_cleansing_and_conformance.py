# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 065: Medallion Architecture: Silver Transformation, Cleansing and Conformance
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC Raw invoices from the Bronze layer contain duplicate entries, unstandardized status codes ('appr', 'APPROVED', 'approved'), missing tax codes, and stringified numeric values. The Silver layer must conform and cleanse these into enterprise-ready records.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Build the Silver layer transformation: filter and clean raw data, standardize categorical codes, enforce data quality rules (non-negative amounts), cast data to strict primitive types, and execute deterministic deduplication into a conformed Delta Silver table.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC The Silver layer represents the enterprise-conformed, cleansed version of truth. It standardizes naming conventions, parses strings into structured types, deduplicates records using business keys, and joins disparate Bronze feeds into consolidated entity tables.

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
        .appName("UseCase_065_Medallion_Architecture_Silver_Transformation_Cleansing_and_Conformance")
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
# MAGIC ### Step 1: Reading from Upstream Bronze Layer
# MAGIC Read uncleaned records containing duplicates and messy categories.

# COMMAND ----------

raw_bronze = [
    ("INV-1001", "V-01", "1,500.00", "appr", "2023-11-20 10:00:00"),
    ("INV-1001", "V-01", "1,500.00", "APPROVED", "2023-11-20 10:05:00"), # Duplicate update
    ("INV-1002", "V-02", "420.50", "PENDING", "2023-11-20 10:01:00"),
    ("INV-1003", "V-03", "-50.00", "REJECTED", "2023-11-20 10:02:00")   # Invalid negative invoice!
]

schema = "invoice_no STRING, vendor_id STRING, raw_amount STRING, raw_status STRING, updated_at STRING"
df_bronze_in = spark.createDataFrame(raw_bronze, schema)
df_bronze_in.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Applying Cleansing, Type Casting and Status Standardization
# MAGIC Standardize uppercase status, clean currency strings, and enforce positive amounts.

# COMMAND ----------

# Clean amount and standardize status
df_cleansed = (
    df_bronze_in
    .withColumn("clean_amount", F.regexp_replace("raw_amount", r"[^\d.]", "").cast(T.DoubleType()))
    .withColumn(
        "standard_status",
        F.when(F.upper(F.col("raw_status")).isin(["APPR", "APPROVED"]), "APPROVED")
         .when(F.upper(F.col("raw_status")).isin(["PEND", "PENDING"]), "PENDING")
         .otherwise(F.upper(F.col("raw_status")))
    )
    # Quality filter: reject invoices with non-positive amounts
    .filter(F.col("clean_amount") > 0)
)

df_cleansed.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Deterministic Deduplication and Silver Table Landing
# MAGIC Retain only the latest update per invoice_no using window deduplication.

# COMMAND ----------

silver_path = "/tmp/lakehouse_silver_065"
if Path(silver_path).exists():
    shutil.rmtree(silver_path)

win_dedup = Window.partitionBy("invoice_no").orderBy(F.col("updated_at").desc())

df_silver = (
    df_cleansed
    .withColumn("rn", F.row_number().over(win_dedup))
    .filter(F.col("rn") == 1)
    .select(
        "invoice_no",
        "vendor_id",
        F.col("clean_amount").alias("invoice_amount"),
        F.col("standard_status").alias("invoice_status"),
        F.to_timestamp("updated_at").alias("last_updated_at"),
        F.current_timestamp().alias("_silver_processed_at")
    )
)

df_silver.write.format("delta").mode("overwrite").save(silver_path)

print("Conformed Silver Delta Table:")
spark.read.format("delta").load(silver_path).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Silver layer provides conformed, cleansed, deduplicated data for cross-functional consumption.**
# MAGIC - **Standardize categorical codes and parse strings into strictly typed columns.**
# MAGIC - **Always filter out records violating fundamental business domain rules before landing in Silver.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What transformations belong in the Silver layer versus the Gold layer?
# MAGIC 2. How do you handle records that fail data quality rules during Silver transformation?
# MAGIC 3. How do you ensure Silver tables maintain low query latency for analytical users?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
