# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 093: Lakeflow Declarative Pipelines and Delta Live Tables (DLT) Expectations
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC An enterprise financial lakehouse migrates manual notebook pipelines to Databricks Lakeflow Declarative Pipelines (Delta Live Tables). The platform requires declarative Medallion pipelines with embedded Data Quality Expectations that log, drop, or fail on bad data.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Master Delta Live Tables (DLT) & Lakeflow Declarative Pipelines: understand the `@dlt.table` and `@dlt.view` decorators, compare the 3 expectation types (`expect`, `expect_or_drop`, `expect_or_fail`), configure streaming tables vs materialized views, and inspect pipeline event logs.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Lakeflow Declarative Pipelines (DLT) allow data engineers to define *what* tables to produce rather than *how* to orchestrate them. DLT automatically constructs the DAG, provisions clusters, manages checkpoints, and captures data quality metrics per expectation rule.

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
    .appName("UseCase_093_Lakeflow_Declarative_Pipelines_and_Delta_Live_Tables_(DLT)_Expectations")
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
# MAGIC ## 5. Lakeflow / DLT Declarative Pipeline Blueprint
# MAGIC Display the production PySpark syntax for Delta Live Tables with expectations.

# COMMAND ----------

# Production Delta Live Tables / Lakeflow Pipeline:
dlt_pipeline_code = """
import dlt
from pyspark.sql import functions as F

# 1. Bronze Streaming Table (Raw Ingestion)
@dlt.table(
    comment="Raw streaming invoices ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
def invoices_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/enterprise/raw/invoices/")
    )

# 2. Silver Table with Data Quality Expectations
@dlt.table(
    comment="Cleansed invoices with data quality rules"
)
@dlt.expect("valid_invoice_no", "invoice_no IS NOT NULL")
@dlt.expect_or_drop("valid_positive_amount", "amount > 0")
@dlt.expect_or_fail("valid_status", "status IN ('APPROVED', 'PENDING', 'REJECTED')")
def invoices_silver():
    return dlt.read_stream("invoices_raw").select("invoice_no", "amount", "status")

# 3. Gold Materialized View (Business Summary)
@dlt.table(
    comment="Daily aggregated sales summary"
)
def invoices_gold():
    return (
        dlt.read("invoices_silver")
        .groupBy("status")
        .agg(F.sum("amount").alias("total_amount"))
    )
"""

print("Lakeflow / DLT Declarative Pipeline Blueprint defined:")
print(dlt_pipeline_code)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulating the 3 DLT Expectation Behaviors in Standard PySpark
# MAGIC Demonstrate expect (log only), expect_or_drop, and expect_or_fail.

# COMMAND ----------

# Mock incoming data with varying levels of quality issues
raw_batch = [
    ("INV-01", 100.0, "APPROVED"),   # Valid
    ("INV-02", -50.0, "APPROVED"),   # Violates positive amount -> DROP
    (None,     250.0, "PENDING"),    # Violates non-null -> LOG (retain row)
    ("INV-03", 500.0, "INVALID_STATE")# Violates valid_status -> FAIL PIPELINE
]

schema = "invoice_no STRING, amount DOUBLE, status STRING"
df_test = spark.createDataFrame(raw_batch, schema)

# Behavior 1: expect_or_drop (Filters out violating rows)
df_clean = df_test.filter(F.col("amount") > 0)
print(f"After expect_or_drop (amount > 0): {df_clean.count()} rows retained.")
df_clean.show()

# Behavior 2: expect_or_fail (Throws exception if any row violates rule)
has_fatal_error = df_clean.filter(~F.col("status").isin(["APPROVED", "PENDING", "REJECTED"])).count() > 0
if has_fatal_error:
    print("ALERT: expect_or_fail triggered! Pipeline run aborted due to invalid status values.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. DLT Expectations Cheatsheet
# MAGIC Review the operational behavior of DLT expectation modes.

# COMMAND ----------

exp_modes = [
    ("@dlt.expect('rule', 'condition')", "Logs violation count to event log; retains row in table."),
    ("@dlt.expect_or_drop('rule', 'condition')", "Logs violation; automatically drops invalid rows from target table."),
    ("@dlt.expect_or_fail('rule', 'condition')", "Logs violation; immediately aborts pipeline execution with error.")
]

for mode, behavior in exp_modes:
    print(f" {mode}\n   -> {behavior}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Delta Live Tables (DLT) provides a declarative framework for building reliable Medallion lakehouses.**
# MAGIC - **Use `@dlt.expect` to log metrics, `@dlt.expect_or_drop` to clean data, and `@dlt.expect_or_fail` for strict compliance.**
# MAGIC - **DLT automatically manages streaming checkpoints, schema evolution, and optimal cluster sizing.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between a Streaming Table and a Materialized View in Delta Live Tables?
# MAGIC 2. Explain the 3 types of DLT Data Quality Expectations and when to use each.
# MAGIC 3. How does DLT handle pipeline failure recovery compared to standard Databricks Jobs?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
