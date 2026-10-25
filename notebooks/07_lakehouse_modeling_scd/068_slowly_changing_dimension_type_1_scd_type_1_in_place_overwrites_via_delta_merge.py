# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 068: Slowly Changing Dimension Type 1 (SCD Type 1) In-Place Overwrites via Delta Merge
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A master customer dimension stores current contact email, phone number, and address. Under business policy, when a customer moves or updates contact details, past values are overwritten immediately—no historical records are retained.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement Slowly Changing Dimension Type 1 (SCD Type 1) using Delta Lake `MERGE INTO`: update existing records in-place with latest attributes, insert newly discovered customers, and maintain deterministic idempotency.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC SCD Type 1 overwrites old data with new data in-place without maintaining historical versions. In Delta Lake, this is executed efficiently via `DeltaTable.merge()` with `whenMatchedUpdate` and `whenNotMatchedInsert`.

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
        .appName("UseCase_068_Slowly_Changing_Dimension_Type_1_SCD_Type_1_In_Place_Overwrites_via_Delta_Merge")
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
# MAGIC ### Step 1: Initializing Current Customer Dimension Table (SCD1 Target)
# MAGIC Create target customer dimension table.

# COMMAND ----------

from delta.tables import DeltaTable

scd1_path = "/tmp/delta_dim_customer_scd1_068"
if Path(scd1_path).exists():
    shutil.rmtree(scd1_path)

initial_customers = [
    ("C-01", "Alice Smith", "alice@oldmail.com", "Austin", "2023-01-01"),
    ("C-02", "Bob Jones", "bob@corp.com", "Seattle", "2023-01-01")
]

schema = "cust_id STRING, full_name STRING, email STRING, city STRING, last_updated STRING"
spark.createDataFrame(initial_customers, schema).write.format("delta").mode("overwrite").save(scd1_path)

dt_scd1 = DeltaTable.forPath(spark, scd1_path)
print("Initial Customer Dimension (Target):")
dt_scd1.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Preparing Inbound CDC Updates (Source Feed)
# MAGIC Alice updates her email and moves to Denver; Charlie is a new customer.

# COMMAND ----------

incoming_cdc = [
    ("C-01", "Alice Smith", "alice@newmail.com", "Denver", "2023-11-20"), # Overwrite existing
    ("C-03", "Charlie Day", "charlie@bar.com", "Philadelphia", "2023-11-20") # Insert new
]

df_cdc = spark.createDataFrame(incoming_cdc, schema)
print("Incoming Inbound CDC Updates:")
df_cdc.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Executing SCD Type 1 Delta MERGE INTO
# MAGIC Merge source batch into target table in-place.

# COMMAND ----------

(
    dt_scd1.alias("target")
    .merge(
        source=df_cdc.alias("source"),
        condition="target.cust_id = source.cust_id"
    )
    .whenMatchedUpdate(
        set={
            "full_name": F.col("source.full_name"),
            "email": F.col("source.email"),
            "city": F.col("source.city"),
            "last_updated": F.col("source.last_updated")
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Target Customer Dimension after SCD Type 1 Merge:")
dt_scd1.toDF().orderBy("cust_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **SCD Type 1 overwrites old attributes in-place, keeping only the latest current state.**
# MAGIC - **Executed atomically in Delta Lake via `whenMatchedUpdate` and `whenNotMatchedInsertAll()`.**
# MAGIC - **Ideal for master data attributes where historical lineage is irrelevant or legally forbidden to store.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is the business impact of choosing SCD Type 1 versus SCD Type 2?
# MAGIC 2. How does Delta Lake optimize MERGE operations on tables with millions of rows?
# MAGIC 3. Can an SCD Type 1 table track previous historical states without Time Travel?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
