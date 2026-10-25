# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 086: Unity Catalog Three-Tier Namespace: Catalogs, Schemas and Managed vs External Tables
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An enterprise migrates from the legacy Hive Metastore (`hive_metastore.default`) to Databricks Unity Catalog. Platform administrators need to establish a secure 3-tier namespace (`catalog.schema.table`) separating dev, staging, and prod environments, and differentiate between Managed and External tables.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master Unity Catalog architecture: create 3-tier namespaces (`catalog.schema.table`), configure Managed tables (storage lifecycle managed by Databricks) vs External tables (pointing to existing cloud object storage paths), grant access permissions, and query table metadata.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Unity Catalog introduces a centralized governance model across workspaces using a 3-tier namespace: `catalog` -> `schema` -> `table/view/volume`. Dropping a Managed table deletes both metadata and underlying Delta files; dropping an External table removes ONLY metadata, preserving underlying cloud storage data files.

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
        .appName("UseCase_086_Unity_Catalog_Three_Tier_Namespace_Catalogs_Schemas_and_Managed_vs_External_Tables")
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
# MAGIC ### Step 1: Configuring 3-Tier Namespace Hierarchy in SQL
# MAGIC Create catalogs and schemas representing development and production environments.

# COMMAND ----------

# SQL Blueprint for Unity Catalog 3-Tier Hierarchy:
uc_setup_sql = """
-- 1. Create Catalogs for environment isolation
CREATE CATALOG IF NOT EXISTS enterprise_dev COMMENT 'Development Sandbox Catalog';
CREATE CATALOG IF NOT EXISTS enterprise_prod COMMENT 'Production Analytical Catalog';

-- 2. Create Schemas within Catalog
CREATE SCHEMA IF NOT EXISTS enterprise_dev.finance COMMENT 'Finance development schema';
CREATE SCHEMA IF NOT EXISTS enterprise_dev.marketing COMMENT 'Marketing development schema';

-- 3. Set current active context
USE CATALOG enterprise_dev;
USE SCHEMA finance;
"""

print("Unity Catalog 3-Tier DDL Hierarchy defined:")
print(uc_setup_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Comparing Managed vs External Tables
# MAGIC Illustrate differences in storage lifecycle and DDL syntax.

# COMMAND ----------

# Managed Table: Storage location managed by Unity Catalog metastore root
# CREATE TABLE enterprise_dev.finance.t_managed_invoices (
#     invoice_id STRING,
#     amount DOUBLE
# ) USING DELTA;

# External Table: References explicit cloud storage location (S3 / ADLS / GCS)
# CREATE TABLE enterprise_dev.finance.t_external_invoices (
#     invoice_id STRING,
#     amount DOUBLE
# ) USING DELTA
# LOCATION 's3://my-enterprise-data-bucket/finance/invoices/';

table_comparison = [
    ("Managed Table", "Managed by Unity Catalog root", "Both metadata AND physical data files are DELETED"),
    ("External Table", "Custom user-supplied S3/ADLS/GCS URL", "Metadata deleted; physical Parquet files are PRESERVED")
]

schema = "table_type STRING, storage_lifecycle STRING, drop_table_behavior STRING"
df_comp = spark.createDataFrame(table_comparison, schema)
df_comp.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Granting Access Control Privileges
# MAGIC Apply role-based access control grants in Unity Catalog.

# COMMAND ----------

# Production Grant Examples:
# GRANT USAGE ON CATALOG enterprise_dev TO `data_engineers`;
# GRANT SELECT, MODIFY ON SCHEMA enterprise_dev.finance TO `data_engineers`;
# GRANT SELECT ON TABLE enterprise_dev.finance.t_managed_invoices TO `finance_analysts`;

print("Unity Catalog RBAC privilege model mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Unity Catalog replaces the legacy 2-tier Hive Metastore with a structured 3-tier namespace (`catalog.schema.table`).**
# MAGIC - **Managed tables manage both file storage and metadata; dropping them purges physical data files.**
# MAGIC - **External tables decouple metadata from underlying cloud storage, preserving files upon DROP.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between Managed Tables and External Tables in Databricks Unity Catalog?
# MAGIC 2. What happens to underlying data files when an External Table is dropped versus a Managed Table?
# MAGIC 3. Why is a 3-tier namespace superior to the legacy 2-tier Hive Metastore for enterprise multi-tenancy?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
