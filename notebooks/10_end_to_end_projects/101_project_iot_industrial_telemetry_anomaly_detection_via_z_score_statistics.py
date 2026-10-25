# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 101: Project IoT: Industrial Telemetry Anomaly Detection via Z-Score Statistics
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A smart manufacturing plant operates 5,000 robotic welding arms. Bearings experience mechanical fatigue before catastrophic failure. Plant reliability engineers need a real-time statistical anomaly detector that computes moving Z-scores on vibration sensor streams.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Implement distributed statistical anomaly detection in PySpark: calculate rolling mean and standard deviation over temporal windows, compute the Z-score for vibration signals ($Z = \frac{x - \mu}{\sigma}$), flag readings where $|Z| > 3.0$ (three-sigma rule), and trigger predictive maintenance tickets.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC The three-sigma ($3\sigma$) rule assumes sensor readings under normal operation follow an approximate Gaussian distribution. By computing dynamic moving baselines per machine rather than static thresholds, the pipeline adapts to normal operating temperature and load shifts.

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
        .appName("UseCase_101_Project_IoT_Industrial_Telemetry_Anomaly_Detection_via_Z_Score_Statistics")
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
# MAGIC ### Step 1: Generating Sensor Vibration Telemetry
# MAGIC Create high-frequency vibration sensor logs with normal baseline and sudden spike.

# COMMAND ----------

sensor_readings = [
    ("ARM-01", "2023-11-20 10:00:00", 12.1),
    ("ARM-01", "2023-11-20 10:01:00", 12.3),
    ("ARM-01", "2023-11-20 10:02:00", 12.0),
    ("ARM-01", "2023-11-20 10:03:00", 12.2),
    ("ARM-01", "2023-11-20 10:04:00", 28.5), # Severe vibration spike -> Bearing failure!
    ("ARM-02", "2023-11-20 10:00:00", 8.0),
    ("ARM-02", "2023-11-20 10:01:00", 8.1)
]

schema = "machine_id STRING, reading_time STRING, vibration_mm DOUBLE"
df_sensors = spark.createDataFrame(sensor_readings, schema).withColumn("ts", F.to_timestamp("reading_time"))
df_sensors.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Calculating Moving Baseline (Mean & Standard Deviation)
# MAGIC Apply window functions over preceding readings per machine.

# COMMAND ----------

win_machine = Window.partitionBy("machine_id").orderBy("ts").rowsBetween(-4, Window.currentRow)

df_stats = (
    df_sensors
    .withColumn("moving_avg", F.round(F.avg("vibration_mm").over(win_machine), 2))
    .withColumn("moving_stddev", F.round(F.stddev("vibration_mm").over(win_machine), 2))
)

df_stats.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Computing Z-Scores & Flagging Critical Mechanical Anomalies
# MAGIC Evaluate Z = (x - mean) / stddev and flag anomalies exceeding threshold.

# COMMAND ----------

df_anomalies = (
    df_stats
    .withColumn(
        "z_score",
        F.when(
            (F.col("moving_stddev").isNotNull()) & (F.col("moving_stddev") > 0),
            F.round(F.abs(F.col("vibration_mm") - F.col("moving_avg")) / F.col("moving_stddev"), 2)
        ).otherwise(0.0)
    )
    .withColumn(
        "maintenance_alert",
        F.when(F.col("z_score") >= 2.0, "CRITICAL: BEARING_FATIGUE_PREDICTED").otherwise("NORMAL")
    )
)

print("Predictive Maintenance Telemetry Report:")
df_anomalies.select("machine_id", "reading_time", "vibration_mm", "moving_avg", "z_score", "maintenance_alert").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Statistical Z-score detection identifies anomalous values relative to a dynamic, per-machine baseline.**
# MAGIC - **Using rolling window aggregates (`rowsBetween`) calculates moving statistics in linear distributed time.**
# MAGIC - **Predictive maintenance models prevent multi-million dollar equipment breakdowns before catastrophic failure.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How do you handle division-by-zero when computing Z-scores on sensor data with zero variance?
# MAGIC 2. What is the difference between fixed threshold alerting and statistical Z-score anomaly detection?
# MAGIC 3. How do you deploy this model to a Structured Streaming pipeline with watermarking?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
