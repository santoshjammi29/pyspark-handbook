# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 058: Stream-Stream Dual Watermark Joins with Interval Conditions
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC An online retail logistics platform matches checkout order events with payment confirmation events arriving from Stripe/PayPal across separate event streams. Orders and payments can arrive seconds or minutes apart.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement Stream-Stream joins in PySpark: configure dual watermarks on both streaming sides, specify mandatory time-range interval join constraints (`payment_time BETWEEN order_time AND order_time + INTERVAL 1 HOUR`), and explain how state retention boundaries are enforced.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC In a stream-stream join, both datasets are unbounded. Spark must buffer incoming rows from stream A in state store until a matching row arrives from stream B. To prevent state store memory from growing infinitely, Spark strictly requires both a watermark on each stream and a time interval constraint.

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
    .appName("UseCase_058_Stream-Stream_Dual_Watermark_Joins_with_Interval_Conditions")
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
# MAGIC ## 5. Stream-Stream Join Architectural Requirements Blueprint
# MAGIC Detail the three mandatory requirements for stream-stream joins in PySpark.

# COMMAND ----------

# Mandatory Requirements for Stream-Stream Joins:
# 1. Watermark defined on Stream 1: df_orders.withWatermark("order_time", "2 hours")
# 2. Watermark defined on Stream 2: df_payments.withWatermark("payment_time", "3 hours")
# 3. Time Interval Condition:
#    (orders.order_id == payments.order_id) &
#    (payments.payment_time >= orders.order_time) &
#    (payments.payment_time <= orders.order_time + expr("INTERVAL 1 HOUR"))

print("Stream-Stream state retention constraints blueprint mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulating Dual Order and Payment Streams
# MAGIC Create representative streams with varied timestamp offsets.

# COMMAND ----------

from datetime import datetime, timedelta

base_ts = datetime(2023, 11, 20, 10, 0, 0)

orders_batch = [
    ("ORD-1", 120.00, base_ts),
    ("ORD-2", 450.00, base_ts + timedelta(minutes=5)),
    ("ORD-3", 85.00,  base_ts + timedelta(minutes=10))
]

payments_batch = [
    ("ORD-1", "PAY-901", base_ts + timedelta(minutes=2)),   # Matched (2 min after order)
    ("ORD-2", "PAY-902", base_ts + timedelta(minutes=45)),  # Matched (45 min after order)
    ("ORD-99", "PAY-999", base_ts + timedelta(minutes=15))  # Unmatched payment
]

schema_ord = "order_id STRING, amount DOUBLE, order_time TIMESTAMP"
schema_pay = "order_id STRING, payment_id STRING, payment_time TIMESTAMP"

df_ord = spark.createDataFrame(orders_batch, schema_ord)
df_pay = spark.createDataFrame(payments_batch, schema_pay)

print("Orders Stream Batch:")
df_ord.show(truncate=False)
print("Payments Stream Batch:")
df_pay.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Executing Stream-Stream Join with Interval Condition
# MAGIC Join with time range boundary constraints.

# COMMAND ----------

# Join condition pairing primary key match with time range window
join_cond = (
    (df_ord["order_id"] == df_pay["order_id"]) &
    (df_pay["payment_time"] >= df_ord["order_time"]) &
    (df_pay["payment_time"] <= df_ord["order_time"] + F.expr("INTERVAL 1 HOUR"))
)

df_matched_orders = df_ord.join(df_pay, on=join_cond, how="inner").select(
    df_ord["order_id"],
    "amount",
    "order_time",
    "payment_id",
    "payment_time",
    F.round((F.col("payment_time").cast("long") - F.col("order_time").cast("long")) / 60.0, 1).alias("payment_delay_minutes")
)

print("Stream-Stream Matched Order-Payment Events:")
df_matched_orders.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Stream-Stream joins require both watermarks AND time interval boundaries to limit state store retention.**
# MAGIC - **Without interval constraints, Spark would need to buffer every un-joined order forever in memory.**
# MAGIC - **Outer stream-stream joins (left/right outer) emit null records only after the watermark passes the interval window.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. Why are time interval constraints mandatory for stream-stream joins in Apache Spark?
# MAGIC 2. When does an outer stream-stream join emit a row with NULLs for the missing side?
# MAGIC 3. How does RocksDB state store manage memory across dual-stream joins?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
