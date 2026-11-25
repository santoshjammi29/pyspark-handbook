# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 006: Date & Timestamp Mastery: Parsing, Arithmetic & Timezones
# MAGIC
# MAGIC **Module:** Module 01: Core PySpark Foundations & DataFrame API
# MAGIC **Difficulty:** `Beginner`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A global ride-sharing platform records ride dispatch, pickup, and dropoff events across multiple international timezones. Operations teams require accurate trip duration in minutes, local business hour classifications, and SLA reporting.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Master datetime transformations: parse timestamps with explicit timezone offsets, convert between UTC and local timezones (`from_utc_timestamp`), perform date arithmetic (`date_add`, `datediff`), and compute precise duration intervals.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Spark 3.0 introduced Proleptic Gregorian calendar support by default (`spark.sql.session.timeZone`). Spark stores timestamps internally as UTC epoch microseconds. Using timezone-aware functions ensures consistency across distributed worker nodes regardless of server OS timezone settings.

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
        .appName("UseCase_006_Date_and_Timestamp_Mastery_Parsing_Arithmetic_and_Timezones")
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
# MAGIC ### Step 1: Mocking Global Ride Events
# MAGIC Generate ride event timestamps with varied timezones.

# COMMAND ----------

rides_data = [
    ("RIDE-01", "2023-11-15T08:15:00Z", "2023-11-15T08:42:30Z", "America/New_York"),
    ("RIDE-02", "2023-11-15T14:30:00Z", "2023-11-15T15:05:00Z", "Europe/London"),
    ("RIDE-03", "2023-11-15T22:45:00Z", "2023-11-15T23:25:15Z", "Asia/Tokyo"),
    ("RIDE-04", "2023-11-16T01:10:00Z", "2023-11-16T01:35:00Z", "America/Los_Angeles")
]

df_rides = spark.createDataFrame(rides_data, ["ride_id", "pickup_utc", "dropoff_utc", "tz_city"])
df_rides.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Parsing Timestamps & Computing Duration
# MAGIC Convert ISO strings to native timestamps and compute duration in seconds and fractional minutes.

# COMMAND ----------

df_parsed = df_rides.withColumn(
    "pickup_ts", F.to_timestamp(F.col("pickup_utc"))
).withColumn(
    "dropoff_ts", F.to_timestamp(F.col("dropoff_utc"))
)

# Compute duration using unix timestamp difference
df_duration = df_parsed.withColumn(
    "duration_seconds",
    F.col("dropoff_ts").cast(T.LongType()) - F.col("pickup_ts").cast(T.LongType())
).withColumn(
    "duration_minutes",
    F.round(F.col("duration_seconds") / 60.0, 2)
)

df_duration.select("ride_id", "pickup_ts", "dropoff_ts", "duration_minutes").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Converting to Local Time & Extracting Temporal Dimensions
# MAGIC Translate UTC timestamps to local city timezones and derive reporting dimensions.

# COMMAND ----------

df_local = df_duration.withColumn(
    "pickup_local",
    F.from_utc_timestamp(F.col("pickup_ts"), F.col("tz_city"))
).withColumn(
    "pickup_hour",
    F.hour(F.col("pickup_local"))
).withColumn(
    "day_of_week",
    F.date_format(F.col("pickup_local"), "EEEE")
).withColumn(
    "is_peak_hour",
    F.when(F.col("pickup_hour").between(7, 9) | F.col("pickup_hour").between(17, 19), True).otherwise(False)
)

df_local.select("ride_id", "tz_city", "pickup_local", "pickup_hour", "day_of_week", "is_peak_hour").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Always store raw ingestion timestamps in UTC to prevent daylight saving ambiguities.**
# MAGIC - **Use `from_utc_timestamp` and `to_utc_timestamp` for timezone shifts rather than manual hour additions.**
# MAGIC - **Calculate sub-minute precision by casting timestamps to `LongType` (epoch seconds) or using `unix_timestamp`.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How does Apache Spark handle daylight saving time transitions?
# MAGIC 2. What changed between the Legacy Julian/Gregorian calendar and Spark 3.0 Proleptic Gregorian calendar?
# MAGIC 3. How do you calculate the business days between two dates in PySpark without a UDF?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
