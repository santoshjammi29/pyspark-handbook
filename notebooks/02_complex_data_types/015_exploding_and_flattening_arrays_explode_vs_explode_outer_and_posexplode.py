# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 015: Exploding and Flattening Arrays: explode vs explode_outer and posexplode
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A flight booking aggregator normalizes JSON payloads of airline bookings. Bookings contain lists of passengers and stops. Some bookings have empty passenger lists (infant reservation hold) that must not be accidentally dropped during normalization.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Compare `explode()` vs `explode_outer()` to prevent data loss on empty/null arrays. Use `posexplode()` to capture element sequence positions (e.g. flight leg order 0, 1, 2). Flatten multi-dimensional nested arrays (`flatten()`).
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Standard `explode()` drops rows where the array is empty or NULL, leading to silent data loss. `explode_outer()` emits a row with NULL for the exploded element, preserving the parent record. `posexplode()` generates both position (zero-based index) and element.

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
    .appName("UseCase_015_Exploding_and_Flattening_Arrays_explode_vs_explode_outer_and_posexplode")
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
# MAGIC ## 5. Mocking Multi-Leg Booking Records
# MAGIC Create bookings with multiple flight legs and edge cases (empty passengers).

# COMMAND ----------

bookings = [
    ("BKG-001", "Delta", ["ATL", "JFK", "LHR"], ["Alice", "Bob"]),
    ("BKG-002", "United", ["ORD", "FRA"], ["Charlie"]),
    ("BKG-003", "Lufthansa", ["MUC"], []),          # Empty passenger array
    ("BKG-004", "British Airways", ["LHR", "SIN"], None) # Null passenger array
]

schema = "booking_id STRING, airline STRING, flight_legs ARRAY<STRING>, passengers ARRAY<STRING>"
df_bookings = spark.createDataFrame(bookings, schema)
df_bookings.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Standard explode() vs explode_outer() Data Loss Comparison
# MAGIC Demonstrate how standard explode drops records BKG-003 and BKG-004.

# COMMAND ----------

# Standard explode: drops rows with empty or null arrays
df_standard = df_bookings.select("booking_id", "airline", F.explode("passengers").alias("passenger"))
print(f"Standard explode row count: {df_standard.count()} (BKG-003 and BKG-004 dropped!)")
df_standard.show()

# explode_outer: retains parent row with null
df_outer = df_bookings.select("booking_id", "airline", F.explode_outer("passengers").alias("passenger"))
print(f"explode_outer row count: {df_outer.count()} (Preserves all bookings!)")
df_outer.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. posexplode() for Leg Sequencing & Array Flattening
# MAGIC Extract zero-based stop sequences for flight itineraries.

# COMMAND ----------

# posexplode generates leg_sequence and airport
df_legs = df_bookings.select(
    "booking_id",
    "airline",
    F.posexplode("flight_legs").alias("leg_seq", "airport")
)
df_legs.show()

# Flatten 2D nested arrays: [[A, B], [C, D]] -> [A, B, C, D]
df_nested_arr = spark.createDataFrame([("TEST", [["A", "B"], ["C", "D", "E"]])], ["id", "matrix"])
df_flat = df_nested_arr.withColumn("flat_matrix", F.flatten("matrix"))
df_flat.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Always use `explode_outer()` if the array column can contain nulls or empty lists to avoid losing parent rows.**
# MAGIC - **`posexplode()` provides the original ordinal index of each element within its array.**
# MAGIC - **`flatten()` combines an `ARRAY<ARRAY<T>>` into a single `ARRAY<T>` without expanding rows.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. Under what condition does `explode()` drop rows from a DataFrame?
# MAGIC 2. What is the difference between `posexplode` and `posexplode_outer`?
# MAGIC 3. How does exploding large arrays impact partition skew and shuffle memory?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
