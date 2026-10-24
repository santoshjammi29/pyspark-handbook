# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 087: Unity Catalog Volumes for Unstructured and Semi-Structured Data Governance
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A machine learning team manages non-tabular files (PDF documents, medical images, audio files, and model weights) in cloud storage. Legacy DBFS mounts lack row/file-level access control. The platform requires Unity Catalog Volumes for secure governance.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master Unity Catalog Volumes: compare Managed Volumes vs External Volumes, manage files inside volumes using `dbutils.fs` and standard POSIX `/Volumes/catalog/schema/volume` paths, apply governance privileges, and ingest raw documents into Spark.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Unity Catalog Volumes govern non-tabular files (CSVs, images, PDFs, models) alongside tabular tables. Unlike legacy DBFS mounts that grant broad workspace-wide access, Volumes enforce fine-grained access control using the standard 3-tier namespace (`/Volumes/catalog/schema/volume_name/`).

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
        .appName("UseCase_087_Unity_Catalog_Volumes_for_Unstructured_and_Semi_Structured_Data_Governance")
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
# MAGIC ### Step 1: Unity Catalog Volumes DDL Specification
# MAGIC Create Managed and External Volumes within schemas.

# COMMAND ----------

# Volume DDL Syntax:
volume_sql = """
-- Managed Volume: Databricks manages underlying storage path
CREATE VOLUME IF NOT EXISTS enterprise_dev.finance.raw_receipts_managed
COMMENT 'Managed volume storing customer invoice PDFs and scans';

-- External Volume: Governs existing cloud bucket directory
CREATE EXTERNAL VOLUME IF NOT EXISTS enterprise_dev.finance.partner_drops_external
LOCATION 's3://vendor-inbound-bucket/claims/'
COMMENT 'External volume pointing to vendor claims inbox';
"""

print("Volume DDL defined:")
print(volume_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Interacting with Volumes via POSIX Paths and DBUtils
# MAGIC Read and write files directly using the unified /Volumes path.

# COMMAND ----------

# Databricks supports standard POSIX path access in Python:
# volume_file_path = "/Volumes/enterprise_dev/finance/raw_receipts_managed/invoice_1001.pdf"
# with open(volume_file_path, "rb") as f:
#     pdf_bytes = f.read()

# Using dbutils.fs on Volumes:
# dbutils.fs.ls("/Volumes/enterprise_dev/finance/raw_receipts_managed")

print("Volumes provide direct POSIX file path access for Python libraries (OpenCV, PIL, PyPDF2)!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Governing File Access with Unity Catalog Permissions
# MAGIC Grant read and write privileges on Volumes.

# COMMAND ----------

# Access Control Commands for Volumes:
# GRANT READ VOLUME ON VOLUME enterprise_dev.finance.raw_receipts_managed TO `finance_auditors`;
# GRANT WRITE VOLUME ON VOLUME enterprise_dev.finance.raw_receipts_managed TO `data_engineers`;

print("Fine-grained volume access control mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Unity Catalog Volumes extend data governance to non-tabular files (PDFs, images, CSVs, models).**
# MAGIC - **Volumes provide standard POSIX path syntax (`/Volumes/catalog/schema/volume/file.ext`) compatible with Python libraries.**
# MAGIC - **Volumes deprecate insecure legacy DBFS mounts by bringing unified access controls and audit logs.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is a Unity Catalog Volume and why was it introduced to replace legacy DBFS mounts?
# MAGIC 2. What is the difference between a Managed Volume and an External Volume?
# MAGIC 3. How do data scientists access files inside a Volume using standard Python `open()`?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
