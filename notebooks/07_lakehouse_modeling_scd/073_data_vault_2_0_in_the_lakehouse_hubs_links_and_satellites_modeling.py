# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 073: Data Vault 2.0 in the Lakehouse: Hubs, Links and Satellites Modeling
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A large banking institution integrates 30 disparate core banking, loan, and credit systems. Source systems change schemas frequently. The enterprise architecture mandates Data Vault 2.0 to provide 100% auditability, parallel loading, and agile schema evolution.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement the core entities of Data Vault 2.0 in PySpark: (1) Hubs (unique business keys and hash keys), (2) Links (relationships between business keys), and (3) Satellites (contextual descriptive attributes with load timestamps).
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Data Vault 2.0 separates business keys (Hubs), relationships (Links), and descriptive attributes (Satellites). Hash keys (`sha2(natural_key)`) allow distributed parallel ingestion into Hubs, Links, and Satellites simultaneously without foreign key lookup bottlenecks.

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
        .appName("UseCase_073_Data_Vault_20_in_the_Lakehouse_Hubs_Links_and_Satellites_Modeling")
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
# MAGIC ### Step 1: 1. Building Hub Customer (Hubs: Unique Business Keys)
# MAGIC Create Hub table containing hash keys and business keys.

# COMMAND ----------

raw_cust = [("CUST-101", "CRM"), ("CUST-102", "CORE_BANKING")]
df_raw_cust = spark.createDataFrame(raw_cust, ["customer_id", "record_source"])

hub_customer = df_raw_cust.select(
    F.sha2(F.col("customer_id"), 256).alias("hk_customer"),
    F.col("customer_id").alias("bk_customer_id"),
    F.current_timestamp().alias("load_date"),
    F.col("record_source")
)

print("Hub Customer (Hub):")
hub_customer.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: 2. Building Link Customer-Account (Links: Relationships)
# MAGIC Create Link table capturing the relationship between Customer and Account hash keys.

# COMMAND ----------

raw_links = [("CUST-101", "ACC-9001", "CORE_BANKING")]
df_raw_links = spark.createDataFrame(raw_links, ["customer_id", "account_id", "record_source"])

link_cust_acc = df_raw_links.select(
    F.sha2(F.concat(F.col("customer_id"), F.lit("||"), F.col("account_id")), 256).alias("hk_link_cust_acc"),
    F.sha2(F.col("customer_id"), 256).alias("hk_customer"),
    F.sha2(F.col("account_id"), 256).alias("hk_account"),
    F.current_timestamp().alias("load_date"),
    F.col("record_source")
)

print("Link Customer Account (Link):")
link_cust_acc.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: 3. Building Satellite Customer Details (Satellites: Descriptive Data)
# MAGIC Create Satellite capturing point-in-time descriptive attributes with hash diff.

# COMMAND ----------

raw_sat = [("CUST-101", "Alice Smith", "Gold", "CORE_BANKING")]
df_raw_sat = spark.createDataFrame(raw_sat, ["customer_id", "name", "tier", "record_source"])

sat_customer = df_raw_sat.select(
    F.sha2(F.col("customer_id"), 256).alias("hk_customer"),
    F.current_timestamp().alias("load_date"),
    # Hashdiff detects if descriptive attributes have changed since last load
    F.sha2(F.concat_ws("||", F.col("name"), F.col("tier")), 256).alias("hashdiff"),
    F.col("name"),
    F.col("tier"),
    F.col("record_source")
)

print("Satellite Customer (Satellite):")
sat_customer.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Data Vault 2.0 decomposes data into Hubs (business keys), Links (associations), and Satellites (context).**
# MAGIC - **Hash keys (`sha2`) eliminate dependency bottlenecks, allowing Hubs, Links, and Satellites to load concurrently.**
# MAGIC - **`hashdiff` columns enable instant change detection without checking every individual descriptive column.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What problem does Data Vault 2.0 solve compared to traditional Inmon 3NF and Kimball Star Schemas?
# MAGIC 2. Why are cryptographic hash keys used instead of database sequences in Data Vault 2.0?
# MAGIC 3. What is the role of the `hashdiff` column in a Data Vault Satellite table?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
