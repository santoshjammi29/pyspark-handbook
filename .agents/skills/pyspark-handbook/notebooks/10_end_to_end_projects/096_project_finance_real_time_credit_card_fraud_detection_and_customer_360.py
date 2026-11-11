# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 096: Project Finance: Real-Time Credit Card Fraud Detection and Customer 360
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A tier-1 retail bank processes 50 million credit card swipe transactions daily. Risk engineering must build an end-to-end Medallion pipeline that ingests raw swipes, calculates real-time risk scores (geographical velocity, transaction spikes > 3x average, rapid successive swipes within 2 minutes), and flags fraudulent transactions for real-time card blocking.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Build an end-to-end financial data engineering project: (1) Ingest raw swipes into Bronze Delta table with audit columns, (2) Cleanse, parse timestamps, and join against customer profiles in Silver, (3) Apply window-based fraud detection rules (swipes within 2 minutes in different cities), and (4) Publish a Gold Customer 360 Risk Summary table.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Financial fraud detection requires low latency and high consistency. Combining Delta Lake ACID guarantees with window-based temporal heuristics allows detecting impossible geographic travel velocities and abnormal spend spikes without complex external ML infrastructure.

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
        .appName("UseCase_096_Project_Finance_Real_Time_Credit_Card_Fraud_Detection_and_Customer_360")
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
# MAGIC ### Step 1: Ingesting Raw Swipes into Bronze Delta Table
# MAGIC Create raw card swipe feed and land into an append-only Bronze Delta table.

# COMMAND ----------

bronze_fraud_path = "/tmp/project_finance_bronze_096"
if Path(bronze_fraud_path).exists():
    shutil.rmtree(bronze_fraud_path)

raw_swipes = [
    ("SW-01", "CARD-101", 85.50,  "New York", "2023-11-20 12:00:00"),
    ("SW-02", "CARD-101", 1200.00,"London",   "2023-11-20 12:05:00"), # 5 min later in London -> FRAUD!
    ("SW-03", "CARD-102", 45.00,  "Chicago",  "2023-11-20 12:00:00"),
    ("SW-04", "CARD-102", 52.00,  "Chicago",  "2023-11-20 12:45:00"),
    ("SW-05", "CARD-103", 9500.00,"Miami",    "2023-11-20 12:10:00")  # Huge transaction
]

schema = "swipe_id STRING, card_id STRING, amount DOUBLE, city STRING, swipe_time STRING"
df_bronze_swipes = spark.createDataFrame(raw_swipes, schema).withColumn("_ingested_at", F.current_timestamp())

df_bronze_swipes.write.format("delta").mode("append").save(bronze_fraud_path)
print("Step 1 Complete: Raw card swipes landed in Bronze Delta table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Silver Cleansing & Velocity-Based Fraud Scoring
# MAGIC Enrich with previous swipe city/time using window functions and identify geographical fraud velocity.

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_fraud_path).withColumn("ts", F.to_timestamp("swipe_time"))

card_window = Window.partitionBy("card_id").orderBy("ts")

df_silver_fraud = (
    df_bronze
    .withColumn("prev_city", F.lag("city").over(card_window))
    .withColumn("prev_ts", F.lag("ts").over(card_window))
    .withColumn("elapsed_minutes", (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0)
    .withColumn(
        "is_geo_velocity_fraud",
        F.when(
            (F.col("prev_city").isNotNull()) &
            (F.col("city") != F.col("prev_city")) &
            (F.col("elapsed_minutes") < 30.0),
            True
        ).otherwise(False)
    )
    .withColumn(
        "is_high_value_alert",
        F.when(F.col("amount") >= 5000.0, True).otherwise(False)
    )
    .withColumn(
        "fraud_risk_score",
        F.when(F.col("is_geo_velocity_fraud"), 95)
         .when(F.col("is_high_value_alert"), 75)
         .otherwise(10)
    )
)

print("Step 2 Complete: Silver Cleansed Swipes with Fraud Risk Scores:")
df_silver_fraud.select("swipe_id", "card_id", "amount", "city", "prev_city", "elapsed_minutes", "fraud_risk_score").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Gold Customer 360 Risk Summary
# MAGIC Aggregate fraud metrics to produce a card-level risk scorecard.

# COMMAND ----------

df_gold_card360 = (
    df_silver_fraud
    .groupBy("card_id")
    .agg(
        F.count("swipe_id").alias("total_swipes"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.max("fraud_risk_score").alias("peak_risk_score"),
        F.sum(F.when(F.col("fraud_risk_score") >= 75, 1).otherwise(0)).alias("suspicious_events_count")
    )
    .withColumn(
        "account_status",
        F.when(F.col("peak_risk_score") >= 90, "LOCKED_FRAUD_HOLD")
         .when(F.col("peak_risk_score") >= 70, "FLAGGED_FOR_REVIEW")
         .otherwise("GOOD_STANDING")
    )
)

print("Step 3 Complete: Gold Customer 360 Risk Scorecard:")
df_gold_card360.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Medallion architecture provides clear separation between raw append (Bronze), enriched logic (Silver), and aggregates (Gold).**
# MAGIC - **Temporal window functions (`lag`) detect velocity-based fraud without external ML inference.**
# MAGIC - **Gold summary tables drive automated real-time action systems like card locking.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How do you design a real-time fraud pipeline that processes 50,000 transactions/second with sub-second latency?
# MAGIC 2. How does the Medallion Architecture ensure auditability during financial regulatory reviews?
# MAGIC 3. What is the difference between rule-based fraud detection in Spark and ML-based fraud detection?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
