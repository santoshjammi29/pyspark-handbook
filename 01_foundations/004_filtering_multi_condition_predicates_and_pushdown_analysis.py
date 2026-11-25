# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 004: Filtering, Multi-Condition Predicates & Pushdown Analysis
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API
# MAGIC **Difficulty:** `Beginner`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A logistics fleet management company needs to identify critical alerts: vehicles with engine temperatures exceeding safe thresholds, low tire pressure, or battery degradation across specific geographic territories.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Build complex multi-condition filters using bitwise operators (`&`, `|`, `~`), evaluate `isin()`, string regex matching (`rlike`), and inspect the Catalyst physical plan to verify predicate pushdown into the file reader.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Predicate pushdown allows the storage layer (Parquet/Delta) to skip entire row groups and data files using file-level min/max statistics without deserializing data into the JVM heap. Complex functions wrapped around columns can prevent pushdown, forcing full table scans.

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
        .appName("UseCase_004_Filtering_Multi_Condition_Predicates_and_Pushdown_Analysis")
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
# MAGIC ### Step 1: Creating Fleet Telemetry Records
# MAGIC Prepare sensor readings from IoT vehicle diagnostics.

# COMMAND ----------

telemetry = [
    ("V-01", "North America", 102.5, 32.0, 94.0, "WARN_TEMP"),
    ("V-02", "Europe", 88.0, 33.5, 98.0, "NORMAL"),
    ("V-03", "North America", 115.0, 26.0, 65.0, "CRITICAL_PRESSURE"),
    ("V-04", "Asia Pacific", 92.5, 31.0, 45.0, "WARN_BATTERY"),
    ("V-05", "Europe", 110.2, 27.5, 50.0, "CRITICAL_TEMP_PRESSURE"),
    ("V-06", "Latin America", 85.0, 34.0, 99.0, "NORMAL")
]

schema = "vehicle_id STRING, region STRING, engine_temp DOUBLE, tire_pressure DOUBLE, battery_pct DOUBLE, alert_code STRING"
df_fleet = spark.createDataFrame(telemetry, schema)
df_fleet.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Executing Multi-Condition Logic
# MAGIC Construct complex boolean predicates using bitwise syntax (`&`, `|`, `~`). Parentheses around each condition are mandatory in PySpark.

# COMMAND ----------

# 1. Vehicles with severe alerts in North America or Europe
target_regions = ["North America", "Europe"]

severe_filter = (
    (F.col("region").isin(target_regions)) &
    (
        (F.col("engine_temp") > 105.0) |
        (F.col("tire_pressure") < 28.0) |
        (F.col("battery_pct") < 60.0)
    ) &
    (~F.col("alert_code").isin(["NORMAL"]))
)

df_severe_alerts = df_fleet.filter(severe_filter)
df_severe_alerts.show()

# 2. Regex matching on alert descriptions
df_critical = df_fleet.filter(F.col("alert_code").rlike("^CRITICAL_"))
df_critical.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Inspecting Catalyst Physical Plan for Predicate Pushdown
# MAGIC Analyze the physical plan to verify Filter operator placement.

# COMMAND ----------

print("=== PHYSICAL PLAN FOR FILTER QUERY ===")
df_severe_alerts.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Always wrap individual filter conditions in parentheses `(cond1) & (cond2)` due to Python bitwise operator precedence.**
# MAGIC - **Predicate pushdown relies on keeping column expressions clean—avoid `func(col) == val` which breaks pushdown; use `col == inverse_func(val)`.**
# MAGIC - **`where()` and `filter()` are exact synonyms in the PySpark DataFrame API.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why do PySpark filter operations require bitwise operators (`&`, `|`) instead of Python keywords (`and`, `or`)?
# MAGIC 2. How does Parquet row-group statistics enable predicate pushdown?
# MAGIC 3. What is partition pruning versus data skipping via file-level stats?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
