# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 092: FinOps and DBU Cost Calculation Using system.billing.usage System Tables
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A Director of Data Engineering needs to track monthly Databricks DBU (Databricks Unit) consumption across business units (Finance, Marketing, ML). The FinOps team must identify expensive rogue jobs, untagged clusters, and cost spikes.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Query Databricks Unity Catalog system billing tables: analyze `system.billing.usage`, compute DBU consumption by job, cluster, and user, calculate estimated dollar expenditures, and build an automated FinOps cost anomaly detection query in PySpark SQL.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Databricks system tables (`system.billing.usage`, `system.compute.clusters`) provide historical operational telemetry. The `system.billing.usage` table records DBU consumption per SKU, account, workspace, cluster, and custom billing tags every hour.

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
        .appName("UseCase_092_FinOps_and_DBU_Cost_Calculation_Using_systembillingusage_System_Tables")
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
# MAGIC ### Step 1: Mocking system.billing.usage Telemetry Data
# MAGIC Create representative billing usage records.

# COMMAND ----------

usage_records = [
    ("WS-1", "JOB-101", "Nightly Ingestion", "ALL_PURPOSE", 45.2, "Finance", "2023-11-20"),
    ("WS-1", "JOB-102", "Customer 360", "JOBS", 12.8, "Marketing", "2023-11-20"),
    ("WS-1", "JOB-103", "Fraud Model Training", "DLT", 65.0, "Fraud", "2023-11-20"),
    ("WS-1", None, "Ad-Hoc Data Science", "ALL_PURPOSE", 110.5, "DataScience", "2023-11-20"),
    ("WS-1", "JOB-101", "Nightly Ingestion", "ALL_PURPOSE", 48.0, "Finance", "2023-11-21")
]

schema = "workspace_id STRING, job_id STRING, workload_name STRING, sku_name STRING, dbus DOUBLE, department STRING, usage_date STRING"
df_billing = spark.createDataFrame(usage_records, schema)
df_billing.createOrReplaceTempView("system_billing_usage")
df_billing.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: FinOps DBU and Dollar Cost Calculation by Department
# MAGIC Aggregate DBU consumption and apply average contract DBU rate ($0.30/DBU).

# COMMAND ----------

finops_query = """
SELECT
    department,
    sku_name,
    COUNT(*) AS run_events,
    ROUND(SUM(dbus), 2) AS total_dbus,
    ROUND(SUM(dbus) * 0.30, 2) AS estimated_cost_usd
FROM system_billing_usage
GROUP BY department, sku_name
ORDER BY total_dbus DESC
"""

df_finops = spark.sql(finops_query)
print("Departmental FinOps Cost Report:")
df_finops.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Detecting Cost Anomalies and Untagged Workloads
# MAGIC Identify interactive all-purpose cluster usage that should run as scheduled jobs.

# COMMAND ----------

anomaly_query = """
SELECT
    workload_name,
    SUM(dbus) AS total_dbus,
    ROUND(SUM(dbus) * 0.30, 2) AS cost_usd,
    'WARNING: High All-Purpose Compute! Convert to Automated Jobs Cluster to save 60%' AS recommendation
FROM system_billing_usage
WHERE sku_name = 'ALL_PURPOSE' AND job_id IS NULL
GROUP BY workload_name
"""

spark.sql(anomaly_query).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **`system.billing.usage` provides granular operational visibility into DBU spend across workspaces.**
# MAGIC - **Automated Jobs compute is ~60% cheaper than interactive All-Purpose compute for scheduled pipelines.**
# MAGIC - **Enforce custom cluster tags (e.g. `Department`, `CostCenter`, `Environment`) to attribute costs accurately.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is a DBU (Databricks Unit) and how is it calculated?
# MAGIC 2. What is the cost difference between All-Purpose Compute and Jobs Compute in Databricks?
# MAGIC 3. How do you query `system.billing.usage` to identify untagged compute resources?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
