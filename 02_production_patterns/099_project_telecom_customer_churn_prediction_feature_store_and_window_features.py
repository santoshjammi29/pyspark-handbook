# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 099: Project Telecom: Customer Churn Prediction Feature Store and Window Features
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A major mobile carrier experiences 2% monthly customer churn. Data scientists need a centralized Lakehouse Feature Store containing 30-day rolling aggregate features (drop in data usage, spike in customer service calls, late bill payments) to train machine learning churn models.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Build an enterprise Telecom Feature Engineering pipeline in PySpark: (1) Compute rolling 30-day temporal window aggregations per subscriber, (2) Calculate trend ratios (current 7-day usage vs 30-day average), (3) Assemble features into a Databricks Feature Store format, and (4) Validate feature drift.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Feature stores prevent online/offline training-serving skew. Point-in-time correctness ensures features computed for model training reflect only the data available at the historical prediction timestamp, preventing data leakage.

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
        .appName("UseCase_099_Project_Telecom_Customer_Churn_Prediction_Feature_Store_and_Window_Features")
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
# MAGIC ### Step 1: Generating Daily Subscriber Usage Telemetry
# MAGIC Create daily data and voice usage logs per subscriber.

# COMMAND ----------

daily_usage = [
    ("SUB-1", "2023-11-01", 1.5, 45, 0),
    ("SUB-1", "2023-11-05", 2.0, 30, 0),
    ("SUB-1", "2023-11-15", 0.5, 10, 1), # Usage dropping, customer service call!
    ("SUB-1", "2023-11-20", 0.1, 5,  2), # Severe drop, multiple calls
    ("SUB-2", "2023-11-01", 3.0, 60, 0),
    ("SUB-2", "2023-11-20", 3.2, 55, 0)
]

schema = "subscriber_id STRING, usage_date STRING, data_gb DOUBLE, call_mins INT, support_calls INT"
df_usage = spark.createDataFrame(daily_usage, schema).withColumn("ts", F.to_date("usage_date"))
df_usage.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Computing 30-Day Windowed Feature Aggregations
# MAGIC Calculate rolling 30-day average usage and support call counts using window specifications.

# COMMAND ----------

# Convert date to epoch days for rangeBetween window
df_with_days = df_usage.withColumn("day_num", F.col("ts").cast("date").cast("long"))

# Rolling 30-day window (29 preceding days + current day)
win_30d = Window.partitionBy("subscriber_id").orderBy("day_num").rangeBetween(-29, 0)

df_features = (
    df_with_days
    .withColumn("avg_data_gb_30d", F.round(F.avg("data_gb").over(win_30d), 2))
    .withColumn("total_support_calls_30d", F.sum("support_calls").over(win_30d))
    .withColumn("max_daily_call_mins_30d", F.max("call_mins").over(win_30d))
)

df_features.select("subscriber_id", "usage_date", "data_gb", "avg_data_gb_30d", "total_support_calls_30d").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Deriving Churn Risk Indicators & Feature Store Packaging
# MAGIC Calculate feature ratios and assemble final ML training dataset.

# COMMAND ----------

df_final_features = (
    df_features
    # Usage ratio: current data vs 30-day average (ratio < 0.5 indicates drop)
    .withColumn(
        "data_usage_drop_ratio",
        F.when(F.col("avg_data_gb_30d") > 0, F.round(F.col("data_gb") / F.col("avg_data_gb_30d"), 2)).otherwise(1.0)
    )
    .withColumn(
        "is_high_churn_risk",
        F.when((F.col("data_usage_drop_ratio") < 0.4) & (F.col("total_support_calls_30d") >= 2), 1).otherwise(0)
    )
    .select(
        "subscriber_id",
        "usage_date",
        "avg_data_gb_30d",
        "total_support_calls_30d",
        "data_usage_drop_ratio",
        "is_high_churn_risk"
    )
)

print("Final ML Feature Store Table:")
df_final_features.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Feature Stores decouple ML feature engineering from real-time model serving.**
# MAGIC - **Use `rangeBetween` on timestamp or epoch day columns to compute rolling time-based features.**
# MAGIC - **Ratios comparing short-term activity to long-term baseline (e.g. 7-day vs 30-day) are powerful predictors.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is point-in-time correctness in Feature Store design and how does it prevent data leakage?
# MAGIC 2. What is the difference between online and offline feature stores?
# MAGIC 3. How do you implement rolling 30-day window features in PySpark on irregular daily observations?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
