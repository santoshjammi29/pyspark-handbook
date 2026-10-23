# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 102: Project Supply Chain: Inventory Replenishment Forecasting and Reorder Triggers
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A national grocery chain manages 10,000 SKUs across 20 regional distribution centers. Supply chain disruptions cause frequent stockouts of essential items. The forecasting team needs an automated Lakehouse pipeline that calculates Daily Run-Rate (DRR), Safety Stock, and triggers automated purchase orders.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Build an automated supply chain replenishment model in PySpark: (1) Compute 14-day moving daily sales velocity, (2) Estimate lead-time demand and safety stock using standard formulas ($SS = Z \times \sigma_{LTD}$), (3) Calculate dynamic Reorder Points (ROP), and (4) Generate automated Purchase Order requisitions.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Supply chain optimization requires reconciling historical sales velocity with supplier delivery lead times. Calculating dynamic reorder points in PySpark enables automated daily replenishment recommendations across millions of warehouse-SKU combinations.

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
    .appName("UseCase_102_Project_Supply_Chain_Inventory_Replenishment_Forecasting_and_Reorder_Triggers")
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
# MAGIC ## 5. Preparing Inventory Levels and Daily Demand Feed
# MAGIC Create SKU inventory stock levels and supplier lead-time parameters.

# COMMAND ----------

inventory = [
    ("SKU-101", "Milk Whole Gallon", 150, 4, 35.0, 5.0), # Current stock: 150, Lead time: 4 days, Daily sales: 35
    ("SKU-102", "Organic Eggs 12pk", 40,  3, 20.0, 4.0), # Current stock: 40, Lead time: 3 days, Daily sales: 20 -> Stockout imminent!
    ("SKU-103", "Artisan Bread",     80,  2, 15.0, 2.0)  # Current stock: 80, Lead time: 2 days, Daily sales: 15
]

schema = "sku STRING, product_name STRING, current_stock INT, lead_time_days INT, avg_daily_sales DOUBLE, stddev_daily_sales DOUBLE"
df_inventory = spark.createDataFrame(inventory, schema)
df_inventory.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Calculating Safety Stock & Dynamic Reorder Point (ROP)
# MAGIC Apply standard industrial supply chain formulas in PySpark.

# COMMAND ----------

# Service factor Z = 1.65 (95% service level / stockout protection)
Z_FACTOR = 1.65

df_rop = (
    df_inventory
    # Lead time demand = lead_time_days * avg_daily_sales
    .withColumn("lead_time_demand", F.round(F.col("lead_time_days") * F.col("avg_daily_sales"), 1))
    # Safety stock = Z * sqrt(lead_time_days) * stddev_daily_sales
    .withColumn(
        "safety_stock",
        F.round(F.lit(Z_FACTOR) * F.sqrt(F.col("lead_time_days")) * F.col("stddev_daily_sales"), 0).cast(T.IntegerType())
    )
    # Reorder Point (ROP) = Lead Time Demand + Safety Stock
    .withColumn("reorder_point", F.round(F.col("lead_time_demand") + F.col("safety_stock"), 0).cast(T.IntegerType()))
)

df_rop.select("sku", "product_name", "current_stock", "lead_time_demand", "safety_stock", "reorder_point").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Generating Automated Purchase Orders (PO Requisitions)
# MAGIC Trigger purchase orders when current stock drops below reorder point.

# COMMAND ----------

df_purchase_orders = (
    df_rop
    .withColumn("needs_reorder", F.col("current_stock") <= F.col("reorder_point"))
    .withColumn(
        "suggested_order_qty",
        F.when(F.col("needs_reorder"), (F.col("reorder_point") * 2) - F.col("current_stock")).otherwise(0)
    )
    .filter(F.col("needs_reorder") == True)
)

print("Automated Purchase Order Requisitions Triggered:")
df_purchase_orders.select("sku", "product_name", "current_stock", "reorder_point", "suggested_order_qty").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Reorder Point (ROP) equals Lead Time Demand plus statistical Safety Stock.**
# MAGIC - **Automating ROP calculations prevents stockouts while minimizing expensive warehouse carrying costs.**
# MAGIC - **Gold replenishment tables trigger automated ERP purchase order webhooks via Databricks Workflows.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How do you calculate Safety Stock statistically and what is the role of the Z-score service factor?
# MAGIC 2. How do you handle supplier lead time variability in supply chain data models?
# MAGIC 3. What is the difference between periodic review and continuous review inventory policies?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
