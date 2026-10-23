# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 030: Relational Joins Masterclass: Inner, Outer, Semi and Anti Joins
# MAGIC
# MAGIC **Module:** Module 03: Advanced SQL, Aggregations & Analytical Window Functions  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC An enterprise CRM data reconciler syncs accounts between Salesforce and a billing database. The reconciliation team needs to find: (1) Fully matched customer profiles, (2) Customers in CRM with zero billing records, and (3) Active billing records missing from CRM.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement and compare all relational join types in PySpark: `inner`, `left`, `right`, `full_outer`, `left_semi`, and `left_anti`. Explain why `left_semi` and `left_anti` are preferred over left joins with null checks.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC `left_semi` join filters rows from the left table that have at least one match in the right table without duplicating left rows or including right columns. `left_anti` returns rows from the left table that have NO match in the right table. Both are far more memory-efficient than standard joins.

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
    .appName("UseCase_030_Relational_Joins_Masterclass_Inner,_Outer,_Semi_and_Anti_Joins")
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
# MAGIC ## 5. Generating CRM and Billing Datasets
# MAGIC Create overlapping CRM and billing customer tables.

# COMMAND ----------

crm_data = [
    ("C-01", "Acme Corp", "Enterprise"),
    ("C-02", "Beta Ltd", "Mid-Market"),
    ("C-03", "Cyber Dyne", "SMB"),
    ("C-04", "Delta Air", "Enterprise")
]

billing_data = [
    ("C-01", 15000.0, "ACTIVE"),
    ("C-02", 4500.0, "ACTIVE"),
    ("C-05", 2200.0, "OVERDUE") # In billing, but missing in CRM
]

df_crm = spark.createDataFrame(crm_data, ["cust_id", "company_name", "tier"])
df_bill = spark.createDataFrame(billing_data, ["cust_id", "mrr", "billing_status"])

print("CRM Dataset:")
df_crm.show()
print("Billing Dataset:")
df_bill.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inner, Full Outer & Duplicate Key Avoidance
# MAGIC Execute standard relational joins and resolve duplicate join column names.

# COMMAND ----------

# Inner join: only customers present in both
df_inner = df_crm.join(df_bill, on="cust_id", how="inner")
print("Inner Join (Matched Customers):")
df_inner.show()

# Full Outer join: all records from both systems
df_outer = df_crm.join(df_bill, on="cust_id", how="full_outer")
print("Full Outer Join (Complete System Footprint):")
df_outer.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. High-Efficiency Left Semi and Left Anti Joins
# MAGIC Filter without duplicating left-side rows or loading right-side columns.

# COMMAND ----------

# Left Semi: Keep CRM customers that have billing records (retains CRM columns only)
df_semi = df_crm.join(df_bill, on="cust_id", how="left_semi")
print("Left Semi Join (CRM Accounts with Active Billing):")
df_semi.show()

# Left Anti: Keep CRM customers that have NO billing records (unbilled prospects)
df_anti = df_crm.join(df_bill, on="cust_id", how="left_anti")
print("Left Anti Join (CRM Accounts Missing from Billing):")
df_anti.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Use `on='col_name'` instead of `df1['col'] == df2['col']` to avoid producing duplicate join column names.**
# MAGIC - **`left_semi` acts as an `EXISTS` filter: it does not duplicate left rows if multiple right rows match.**
# MAGIC - **`left_anti` acts as a `NOT EXISTS` filter: highly optimized for finding orphaned or missing records.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `left_semi` join and an `inner` join followed by `.select(df1['*']).distinct()`?
# MAGIC 2. Why does `left_anti` outperform `left` join followed by `.filter(df2['key'].isNull())`?
# MAGIC 3. How does Catalyst Optimizer choose between SortMergeJoin and ShuffleHashJoin?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
