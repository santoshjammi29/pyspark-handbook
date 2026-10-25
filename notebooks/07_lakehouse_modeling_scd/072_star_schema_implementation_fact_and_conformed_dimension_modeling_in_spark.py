# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 072: Star Schema Implementation: Fact and Conformed Dimension Modeling in Spark
# MAGIC
# MAGIC **Module:** Module 07: Lakehouse Data Modeling, CDC & SCD Implementation
# MAGIC **Difficulty:** `Intermediate`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A retail enterprise transitions from a monolithic relational data warehouse to a Databricks Lakehouse. BI analysts need classic Star Schema structures (Sales Fact table surrounded by Date, Customer, Store, and Product conformed dimensions).
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Design and build a complete Star Schema in PySpark: construct conformed dimensions with surrogate keys, create the centralized Sales Fact table with foreign keys and degenerate dimensions, and execute star-join queries optimized for BI tools.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Star Schemas remain the gold standard for BI reporting in Lakehouses due to user familiarity and tool compatibility (Power BI / Tableau). Spark optimizes star joins using Dynamic Partition Pruning (DPP) and Broadcast Hash Joins.

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
        .appName("UseCase_072_Star_Schema_Implementation_Fact_and_Conformed_Dimension_Modeling_in_Spark")
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
# MAGIC ### Step 1: Building Conformed Dimensions (Customer & Product)
# MAGIC Construct dimension tables with surrogate keys.

# COMMAND ----------

dim_cust = spark.createDataFrame([
    (101, "CUST-A", "Alice Smith", "Gold", "US"),
    (102, "CUST-B", "Bob Jones", "Silver", "UK")
], ["dim_customer_sk", "customer_id", "full_name", "loyalty_tier", "country"])

dim_prod = spark.createDataFrame([
    (501, "PROD-X", "Mechanical Keyboard", "Hardware", 89.99),
    (502, "PROD-Y", "USB-C Hub", "Hardware", 29.99)
], ["dim_product_sk", "product_id", "product_name", "category", "unit_price"])

print("Customer Dimension:")
dim_cust.show()
print("Product Dimension:")
dim_prod.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Building the Centralized Sales Fact Table
# MAGIC Construct fact table referencing dimension surrogate keys.

# COMMAND ----------

fact_sales = spark.createDataFrame([
    ("TXN-901", 101, 501, 2, 179.98, "2023-11-20"),
    ("TXN-902", 101, 502, 1, 29.99,  "2023-11-20"),
    ("TXN-903", 102, 501, 1, 89.99,  "2023-11-20")
], ["txn_id", "dim_customer_sk", "dim_product_sk", "quantity", "total_sales_amount", "order_date"])

print("Sales Fact Table:")
fact_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Executing Star-Join Query with Dynamic Pruning
# MAGIC Join fact table with dimensions for executive slice-and-dice reporting.

# COMMAND ----------

df_star_report = (
    fact_sales
    .join(dim_cust, on="dim_customer_sk")
    .join(dim_prod, on="dim_product_sk")
    .groupBy("country", "category")
    .agg(
        F.sum("total_sales_amount").alias("gross_revenue"),
        F.sum("quantity").alias("units_sold")
    )
    .orderBy(F.desc("gross_revenue"))
)

print("Star-Schema Analytics Report:")
df_star_report.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Star Schemas feature a central numeric Fact table surrounded by denormalized Dimension tables.**
# MAGIC - **Surrogate keys (integers) improve join performance and decouple reporting from source transactional IDs.**
# MAGIC - **Dynamic Partition Pruning (DPP) allows Spark to skip fact table partitions dynamically based on dimension filters.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How does Dynamic Partition Pruning (DPP) optimize Star Schema queries in Apache Spark?
# MAGIC 2. What is the difference between a Star Schema and a Snowflake Schema in terms of normalization and query performance?
# MAGIC 3. What is a Degenerate Dimension and where is it stored in a Star Schema?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
