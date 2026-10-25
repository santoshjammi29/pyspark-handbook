# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 066: Medallion Architecture: Gold Business Aggregations and Dimensional Serving
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC Executive leadership and Power BI dashboards require pre-aggregated quarterly vendor performance scorecards: total invoiced volume, approval rate percentages, average turnaround time, and vendor tier classifications.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Build the Gold layer of the Medallion Architecture: read from conformed Silver Delta tables, compute executive business metrics, apply business KPIs and risk score calculations, and write high-performance Gold aggregate tables optimized for BI consumption.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC The Gold layer consists of project-specific, business-level aggregated tables, star schema dimension and fact tables, or feature store tables. Gold tables are typically optimized with Z-Ordering or Liquid Clustering to ensure sub-second dashboard query response times.

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
        .appName("UseCase_066_Medallion_Architecture_Gold_Business_Aggregations_and_Dimensional_Serving")
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
# MAGIC ### Step 1: Reading from Silver Layer Invoices
# MAGIC Create realistic Silver invoice dataset.

# COMMAND ----------

silver_data = [
    ("INV-1", "V-01", 1500.0, "APPROVED", "2023-10-01"),
    ("INV-2", "V-01", 3200.0, "APPROVED", "2023-10-15"),
    ("INV-3", "V-01", 800.0,  "REJECTED", "2023-10-20"),
    ("INV-4", "V-02", 450.0,  "APPROVED", "2023-10-05"),
    ("INV-5", "V-02", 900.0,  "PENDING",  "2023-10-25"),
    ("INV-6", "V-03", 12500.0,"APPROVED", "2023-10-12")
]

schema = "invoice_no STRING, vendor_id STRING, amount DOUBLE, status STRING, invoice_date STRING"
df_silver_in = spark.createDataFrame(silver_data, schema)
df_silver_in.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Computing Gold KPI Aggregations and Business Tiers
# MAGIC Calculate approval rate, total volume, and classify vendor risk tiers.

# COMMAND ----------

df_gold_kpis = (
    df_silver_in
    .groupBy("vendor_id")
    .agg(
        F.count("invoice_no").alias("total_invoices_submitted"),
        F.round(F.sum("amount"), 2).alias("total_invoiced_spend"),
        F.round(F.avg("amount"), 2).alias("avg_invoice_value"),
        F.sum(F.when(F.col("status") == "APPROVED", 1).otherwise(0)).alias("approved_invoices_count"),
        F.sum(F.when(F.col("status") == "REJECTED", 1).otherwise(0)).alias("rejected_invoices_count")
    )
    .withColumn(
        "approval_rate_pct",
        F.round((F.col("approved_invoices_count") / F.col("total_invoices_submitted")) * 100, 1)
    )
    .withColumn(
        "vendor_volume_tier",
        F.when(F.col("total_invoiced_spend") >= 10000, "STRATEGIC_TIER_1")
         .when(F.col("total_invoiced_spend") >= 2000, "PREFERRED_TIER_2")
         .otherwise("STANDARD_TIER_3")
    )
)

print("Gold Vendor Performance Scorecard:")
df_gold_kpis.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Writing to Gold Delta Lake Table with Liquid Clustering
# MAGIC Save to Gold destination ready for BI and semantic metrics.

# COMMAND ----------

gold_path = "/tmp/lakehouse_gold_066"
if Path(gold_path).exists():
    shutil.rmtree(gold_path)

df_gold_kpis.write.format("delta").mode("overwrite").save(gold_path)

print("Gold Table successfully landed. Ready for Power BI / Tableau consumption!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Gold tables represent domain-specific business aggregates and dimensional models designed for end-user analytics.**
# MAGIC - **Pre-aggregating KPIs in Gold reduces compute consumption on BI tools and guarantees single-version-of-truth metrics.**
# MAGIC - **Apply clustering (`CLUSTER BY`) on Gold tables based on typical dashboard filter columns.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why should BI tools query Gold tables rather than directly querying Silver tables?
# MAGIC 2. How does the Medallion Architecture ensure compliance with GDPR 'Right to be Forgotten' across Bronze, Silver, and Gold?
# MAGIC 3. What is the difference between a Gold table and a materialized view in Databricks?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
