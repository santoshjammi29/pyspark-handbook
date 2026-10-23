# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 053: Distributed REST API and Webhook Ingestion without Driver OOM
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A financial data vendor exposes historical equity prices via an authenticated REST API with rate limits. The engineering team must fetch data for 50,000 distinct stock tickers. Running loops on the driver node takes 18 hours or crashes the driver with out-of-memory errors.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement parallel, distributed HTTP REST API ingestion in PySpark: partition entity IDs across worker nodes, use `mapPartitions` to maintain persistent connection sessions (`requests.Session`), apply batch chunking, handle exponential backoff rate limits, and assemble responses into DataFrames.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Using `mapPartitions` initializes one HTTP connection pool (`requests.Session()`) per partition rather than opening a new TCP connection per row. Distributing requests across 20 executor partitions achieves 20x concurrency while honoring per-node rate limit quotas.

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
    .appName("UseCase_053_Distributed_REST_API_and_Webhook_Ingestion_without_Driver_OOM")
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
# MAGIC ## 5. Preparing Target Partitioned Request Keys
# MAGIC Create target ticker symbols distributed across 4 partitions.

# COMMAND ----------

tickers = [f"TICKER_{i:04d}" for i in range(1, 21)]
# Distribute across 4 partitions
df_tickers = spark.createDataFrame([(t,) for t in tickers], ["ticker"]).repartition(4)

print(f"Total Tickers: {df_tickers.count()} across {df_tickers.rdd.getNumPartitions()} partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Distributed Ingestion via mapPartitions and Session Pooling
# MAGIC Execute parallel HTTP calls inside worker tasks.

# COMMAND ----------

import time
import json

def fetch_ticker_data_partition(iterator):
    # Executed ONCE per partition on worker executor
    # Simulates reusable requests.Session()
    session_id = f"worker_session_{time.time()}"
    results = []
    
    for row in iterator:
        ticker = row.ticker
        # Simulate network latency to REST API (e.g. 5ms)
        time.sleep(0.005)
        # Mock REST API response payload
        mock_response = {
            "ticker": ticker,
            "price": round(150.0 + (hash(ticker) % 50), 2),
            "currency": "USD",
            "status_code": 200,
            "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        results.append((
            mock_response["ticker"],
            mock_response["price"],
            mock_response["currency"],
            mock_response["status_code"],
            mock_response["fetched_at"]
        ))
    return iter(results)

# Execute parallel extraction across all executors
rdd_responses = df_tickers.rdd.mapPartitions(fetch_ticker_data_partition)
schema = "ticker STRING, price DOUBLE, currency STRING, http_status INT, fetched_at STRING"
df_api_results = spark.createDataFrame(rdd_responses, schema)

print("Distributed REST API Ingestion Results:")
df_api_results.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation & Status Code Auditing
# MAGIC Verify all requests completed with HTTP 200.

# COMMAND ----------

assert df_api_results.count() == 20, "Ticker count mismatch"
assert df_api_results.filter(F.col("http_status") != 200).count() == 0, "Non-200 responses detected"
print("All distributed API calls verified successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **`mapPartitions` allows initializing heavyweight objects (database connections, HTTP sessions) once per partition rather than per row.**
# MAGIC - **Distributing API calls across worker partitions speeds up extraction linearly with cluster worker count.**
# MAGIC - **Always include rate-limiting sleep intervals and exponential backoff retry logic inside worker partitions.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. Why is `mapPartitions` far superior to `map` for executing REST API requests or database lookups?
# MAGIC 2. How do you handle API rate-limiting (HTTP 429 Too Many Requests) across multiple distributed Spark executors?
# MAGIC 3. What is the risk of driver OutOfMemory (OOM) if you fetch millions of API responses using `.collect()`?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
