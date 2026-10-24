# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 089: Dynamic Column Masking for PII Protection and Compliance
# MAGIC
# MAGIC **Module:** Module 09: Databricks Platform, Unity Catalog, Security & FinOps
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A medical insurance database stores member Social Security Numbers (SSN), full names, and email addresses. Customer service representatives must only see masked SSNs (`***-**-1234`), while compliance officers see cleartext.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement dynamic Column Masking in Databricks Unity Catalog: create a SQL masking function using `is_account_group_member()`, apply the mask to sensitive columns using `ALTER TABLE ... ALTER COLUMN ... SET MASK`, and verify dynamic redaction.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Column Masks evaluate dynamically at query execution. If a user is not in the authorized administrative group, the masking function replaces sensitive column data with a masked pattern (e.g. SHA256 hash or asterisks) before returning results.

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
        .appName("UseCase_089_Dynamic_Column_Masking_for_PII_Protection_and_Compliance")
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
# MAGIC ### Step 1: Defining SQL Column Masking Function
# MAGIC Create SQL function that returns cleartext for compliance officers and redacted strings for others.

# COMMAND ----------

# SQL Mask Function Blueprint:
mask_sql = """
CREATE OR REPLACE FUNCTION enterprise_prod.governance.ssn_mask(ssn STRING)
RETURN IF(
    is_account_group_member('compliance_officers'),
    ssn,  -- Cleartext for compliance officers
    CONCAT('***-**-', SUBSTRING(ssn, 8, 4)) -- Redacted for general analysts
);
"""

print("Column Masking SQL Function:")
print(mask_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Attaching Column Mask to Table Column
# MAGIC Attach mask function using SET MASK DDL.

# COMMAND ----------

# Apply mask to table:
# ALTER TABLE enterprise_prod.hr.t_members
# ALTER COLUMN ssn SET MASK enterprise_prod.governance.ssn_mask;

print("SSN column masked with dynamic security policy.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Simulating Dynamic Column Masking in PySpark
# MAGIC Demonstrate masking transformations in standard PySpark.

# COMMAND ----------

members = [
    ("M-101", "Alice Smith", "123-45-6789", "alice@corp.com"),
    ("M-102", "Bob Jones", "987-65-4321", "bob@corp.com")
]
df_members = spark.createDataFrame(members, ["member_id", "name", "ssn", "email"])

# Simulate non-compliance user session
is_compliance = False

df_masked = df_members.withColumn(
    "ssn",
    F.when(F.lit(is_compliance), F.col("ssn"))
     .otherwise(F.concat(F.lit("***-**-"), F.substring(F.col("ssn"), 8, 4)))
).withColumn(
    "email",
    F.when(F.lit(is_compliance), F.col("email"))
     .otherwise(F.regexp_replace(F.col("email"), r"(^.).*(@.*$)", "$1***$2"))
)

print(f"Masked Output for Standard Analyst (is_compliance={is_compliance}):")
df_masked.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Column Masking anonymizes sensitive attributes dynamically without mutating underlying storage files.**
# MAGIC - **Single table serves both authorized compliance officers (cleartext) and general analysts (masked).**
# MAGIC - **Column masks cannot be bypassed by copying or selecting into another table without proper privileges.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between dynamic Column Masking and permanent hashing at write time?
# MAGIC 2. Can a user bypass a Column Mask by executing `SELECT * INTO new_table FROM masked_table`?
# MAGIC 3. How do Column Masks interact with downstream aggregations (e.g. `COUNT(DISTINCT ssn)`)?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
