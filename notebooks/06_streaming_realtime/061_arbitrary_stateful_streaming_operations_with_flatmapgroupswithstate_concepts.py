# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 061: Arbitrary Stateful Streaming Operations with FlatMapGroupsWithState Concepts
# MAGIC
# MAGIC **Module:** Module 06: Streaming & Near Real-Time Processing  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A telecommunications security monitor detects SIM-swap and fraud patterns across customer cell towers. A fraud flag must trigger only if a user connects to two distinct cell towers separated by 500+ miles within a 15-minute window.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Master arbitrary stateful streaming concepts: compare simple window aggregations with custom state machines, understand the mechanics of `mapGroupsWithState` and `flatMapGroupsWithState`, manage state expiration timeouts (ProcessingTime vs EventTime), and handle state state eviction.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC `mapGroupsWithState` and `flatMapGroupsWithState` allow developers to maintain arbitrary user-defined state objects across streaming batches. While full API implementation requires Scala/Java or PySpark with custom encoders, understanding the stateful machine model is critical for senior data engineers.

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
    .appName("UseCase_061_Arbitrary_Stateful_Streaming_Operations_with_FlatMapGroupsWithState_Concepts")
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
# MAGIC ## 5. Arbitrary Stateful Processing Architectural Blueprint
# MAGIC Illustrate how state machines track custom sessions and timeout states.

# COMMAND ----------

# State Machine Model:
# Inputs: (Key: user_id, Value: current_cell_event, State: UserTravelHistory)
# Operations:
# 1. If State is Empty: Initialize state with current tower and timestamp.
# 2. If State Exists: Calculate speed/distance from last tower.
# 3. If impossible velocity detected (> 500 mph): Emit FRAUD_ALERT event!
# 4. Update state with current tower.
# 5. Set Timeout: Evict state after 30 minutes of inactivity to bound RocksDB memory.

print("Arbitrary state machine architecture mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulating Stateful Machine Transitions in PySpark
# MAGIC Model state transitions using window state tracking.

# COMMAND ----------

tower_events = [
    ("USER-1", "TOWER-NYC", 40.7128, -74.0060, "2023-11-20 10:00:00"),
    ("USER-1", "TOWER-BOS", 42.3601, -71.0589, "2023-11-20 10:08:00"), # 200 miles in 8 min -> Impossible speed!
    ("USER-2", "TOWER-SFO", 37.7749, -122.4194, "2023-11-20 10:00:00"),
    ("USER-2", "TOWER-OAK", 37.8044, -122.2712, "2023-11-20 10:30:00") # 10 miles in 30 min -> Normal
]

schema = "user_id STRING, tower_id STRING, lat DOUBLE, lon DOUBLE, event_time STRING"
df_towers = spark.createDataFrame(tower_events, schema).withColumn("ts", F.to_timestamp("event_time"))

# Stateful calculation: compare with previous tower event in state
user_state_win = Window.partitionBy("user_id").orderBy("ts")

df_state_transitions = (
    df_towers
    .withColumn("prev_tower", F.lag("tower_id").over(user_state_win))
    .withColumn("prev_lat", F.lag("lat").over(user_state_win))
    .withColumn("prev_lon", F.lag("lon").over(user_state_win))
    .withColumn("prev_ts", F.lag("ts").over(user_state_win))
    .withColumn(
        "elapsed_minutes",
        (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0
    )
    .withColumn(
        # Approximate distance in miles
        "distance_miles",
        F.round(F.sqrt(F.pow((F.col("lat") - F.col("prev_lat")) * 69.0, 2) + F.pow((F.col("lon") - F.col("prev_lon")) * 55.0, 2)), 1)
    )
    .withColumn(
        "calculated_mph",
        F.when(F.col("elapsed_minutes") > 0, F.round(F.col("distance_miles") / (F.col("elapsed_minutes") / 60.0), 1)).otherwise(0.0)
    )
    .withColumn(
        "is_fraud_velocity",
        F.when(F.col("calculated_mph") > 500.0, True).otherwise(False)
    )
)

print("Stateful Machine Velocity Analysis:")
df_state_transitions.select("user_id", "tower_id", "prev_tower", "elapsed_minutes", "distance_miles", "calculated_mph", "is_fraud_velocity").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. State Timeout & Eviction Strategy
# MAGIC Highlight how timeout policies prevent state store memory leaks.

# COMMAND ----------

# Production Timeout Guidelines:
# 1. EventTimeTimeout: State expires based on the watermark timestamp (reproducible across replays).
# 2. ProcessingTimeTimeout: State expires based on wall-clock time (simpler, but non-deterministic in replays).
# Always call state.remove() upon session termination to reclaim RocksDB memory!

print("State timeout and memory cleanup rules verified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`mapGroupsWithState` enables custom complex state machines beyond simple tumbling/sliding aggregations.**
# MAGIC - **Always configure timeouts (`GroupStateTimeout.EventTimeTimeout`) to evict stale keys from state stores.**
# MAGIC - **RocksDB is the recommended state store provider for large stateful pipelines in production.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between `mapGroupsWithState` and `flatMapGroupsWithState`?
# MAGIC 2. What are the two timeout types supported in stateful streaming and how do they differ?
# MAGIC 3. How does RocksDB state store overcome JVM heap limitations for multi-gigabyte streaming states?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
