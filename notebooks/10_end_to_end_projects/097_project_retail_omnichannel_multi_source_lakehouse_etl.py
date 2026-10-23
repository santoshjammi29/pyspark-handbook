# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 097: Project Retail: Omnichannel Multi-Source Lakehouse ETL
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A major omnichannel retailer operates physical retail stores, an e-commerce website, and customer support via Salesforce. Data resides in three separate silos: PostgreSQL (store inventory), Clickstream logs (web), and Salesforce (customer CRM). Marketing requires a consolidated 360 customer purchase profile.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Execute an end-to-end retail multi-source integration pipeline: (1) Ingest multi-format sources (Postgres JDBC simulation, Web clickstream JSON, and Salesforce CSV), (2) Reconcile identity across customer identifiers, (3) Perform Delta Lake `MERGE INTO` upserts, and (4) Build a multi-channel revenue analytics model.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Omnichannel retail requires resolving disparate identity keys (e.g. loyalty card ID from store POS vs email from web cookies). The Silver layer performs identity resolution, linking disparate keys to a conformed master `customer_sk`.

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
    .appName("UseCase_097_Project_Retail_Omnichannel_Multi-Source_Lakehouse_ETL")
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
# MAGIC ## 5. Step 1: Simulating Multi-Source Ingestion Feeds
# MAGIC Create store POS transactions, web clickstream sessions, and CRM profiles.

# COMMAND ----------

# Source 1: In-Store POS (from PostgreSQL)
pos_sales = [("POS-1", "CUST-A", 120.0, "STORE_01"), ("POS-2", "CUST-B", 45.0, "STORE_02")]
df_pos = spark.createDataFrame(pos_sales, ["txn_id", "cust_id", "amount", "channel_id"]).withColumn("channel", F.lit("IN_STORE"))

# Source 2: Web E-Commerce Orders (from Web JSON logs)
web_sales = [("WEB-91", "CUST-A", 89.99, "WEB_US"), ("WEB-92", "CUST-C", 310.0, "WEB_US")]
df_web = spark.createDataFrame(web_sales, ["txn_id", "cust_id", "amount", "channel_id"]).withColumn("channel", F.lit("ONLINE"))

# Source 3: CRM Customer Profiles (from Salesforce)
crm_customers = [("CUST-A", "Alice Smith", "VIP"), ("CUST-B", "Bob Jones", "Standard"), ("CUST-C", "Charlie Day", "VIP")]
df_crm = spark.createDataFrame(crm_customers, ["cust_id", "name", "segment"])

print("Multi-Source Ingestion Feeds Prepared.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Step 2: Unified Silver Transaction Conformance
# MAGIC Union all sales channels and enrich with CRM customer segmentation.

# COMMAND ----------

# Union POS and Web sales into unified Silver transactions stream
df_all_sales = df_pos.unionByName(df_web)

# Enrich with Salesforce CRM attributes
df_silver_omnichannel = (
    df_all_sales
    .join(df_crm, on="cust_id", how="left")
    .withColumn("_conformed_at", F.current_timestamp())
)

print("Conformed Silver Omnichannel Transactions:")
df_silver_omnichannel.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Step 3: Gold Customer Omnichannel Lifetime Value Summary
# MAGIC Calculate spend per channel and omnichannel behavior flags.

# COMMAND ----------

df_gold_clv = (
    df_silver_omnichannel
    .groupBy("cust_id", "name", "segment")
    .agg(
        F.round(F.sum("amount"), 2).alias("total_lifetime_spend"),
        F.count("txn_id").alias("total_purchases"),
        F.sum(F.when(F.col("channel") == "IN_STORE", F.col("amount")).otherwise(0)).alias("store_spend"),
        F.sum(F.when(F.col("channel") == "ONLINE", F.col("amount")).otherwise(0)).alias("online_spend"),
        F.countDistinct("channel").alias("active_channels_count")
    )
    .withColumn(
        "shopper_type",
        F.when(F.col("active_channels_count") > 1, "OMNICHANNEL_HYBRID")
         .when(F.col("store_spend") > 0, "IN_STORE_ONLY")
         .otherwise("ONLINE_ONLY")
    )
)

print("Gold Customer Omnichannel Value Scorecard:")
df_gold_clv.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Omnichannel data pipelines reconcile disparate source systems into a unified Lakehouse schema.**
# MAGIC - **`unionByName()` combines disparate streams while preserving column order and alignment.**
# MAGIC - **Omnichannel customer classification identifies high-value hybrid shoppers for personalized marketing.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How do you perform identity resolution across disparate retail channels without a universal user ID?
# MAGIC 2. What is the difference between `union()` and `unionByName()` in PySpark?
# MAGIC 3. How do you design a Delta Lake medallion architecture to handle multi-source CDC merges?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
