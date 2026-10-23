# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 100: Project Cybersecurity: Web Server Access Log Analytics and Threat Detection
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows  
# MAGIC **Difficulty:** `Advanced`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A cyber defense operations center monitors 100 million web server access logs daily. Security analysts must parse unformatted Nginx/Apache logs, detect DDoS attacks (IPs generating > 100 requests in 1 minute), identify SQL injection attempts, and geolocate threat origins.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Build an end-to-end cybersecurity log analytics pipeline: (1) Parse Common Log Format (CLF) / Nginx access logs using regex, (2) Detect HTTP 4xx/5xx error bursts and potential SQL injection signatures (`UNION SELECT`, `' OR 1=1`), (3) Execute sliding window velocity checks, and (4) Output an active threat IP blacklist.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Log parsing in big data pipelines is CPU-intensive. Using native `regexp_extract()` with pre-compiled regex groups in Catalyst operates inside Tungsten UTF-8 binary format, avoiding Java String object garbage collection pressure.

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
    .appName("UseCase_100_Project_Cybersecurity_Web_Server_Access_Log_Analytics_and_Threat_Detection")
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
# MAGIC ## 5. Simulating Raw Nginx Access Log Entries
# MAGIC Create representative access logs with normal traffic and malicious attacks.

# COMMAND ----------

raw_logs = [
    ('192.168.1.10 - - [20/Nov/2023:12:00:01 +0000] "GET /index.html HTTP/1.1" 200 4500'),
    ('192.168.1.10 - - [20/Nov/2023:12:00:05 +0000] "GET /about.html HTTP/1.1" 200 2300'),
    # Attack 1: SQL Injection attempt
    ('10.0.0.99 - - [20/Nov/2023:12:01:00 +0000] "GET /login?user=admin'%20OR%201=1-- HTTP/1.1" 401 520'),
    # Attack 2: Directory traversal attempt
    ('10.0.0.99 - - [20/Nov/2023:12:01:02 +0000] "GET /../../etc/passwd HTTP/1.1" 403 310'),
    # Attack 3: High-frequency brute force
    ('10.0.0.99 - - [20/Nov/2023:12:01:04 +0000] "POST /api/v1/auth HTTP/1.1" 401 120')
]

df_raw_logs = spark.createDataFrame([(l,) for l in raw_logs], ["raw_log"])
df_raw_logs.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Parsing Common Log Format (CLF) via Regex
# MAGIC Extract client IP, timestamp, HTTP method, URL, status code, and response bytes.

# COMMAND ----------

clf_regex = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) \S+" (\d{3}) (\d+)'

df_parsed_logs = (
    df_raw_logs
    .withColumn("client_ip", F.regexp_extract("raw_log", clf_regex, 1))
    .withColumn("request_ts_str", F.regexp_extract("raw_log", clf_regex, 2))
    .withColumn("http_method", F.regexp_extract("raw_log", clf_regex, 3))
    .withColumn("request_uri", F.regexp_extract("raw_log", clf_regex, 4))
    .withColumn("status_code", F.regexp_extract("raw_log", clf_regex, 5).cast(T.IntegerType()))
    .withColumn("bytes_sent", F.regexp_extract("raw_log", clf_regex, 6).cast(T.LongType()))
)

df_parsed_logs.select("client_ip", "http_method", "request_uri", "status_code").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Threat Signature Detection & Blacklist Generation
# MAGIC Identify SQLi, path traversal, and aggregate high-risk attack IPs.

# COMMAND ----------

df_threats = (
    df_parsed_logs
    .withColumn(
        "threat_type",
        F.when(F.lower(F.col("request_uri")).rlike(r"(or%201=1|union%20select|drop%20table)"), "SQL_INJECTION")
         .when(F.col("request_uri").contains("../"), "PATH_TRAVERSAL")
         .when((F.col("status_code") == 401) & (F.col("http_method") == "POST"), "BRUTE_FORCE_AUTH")
         .otherwise(None)
    )
)

print("Identified Cybersecurity Incidents:")
df_threats.filter(F.col("threat_type").isNotNull()).select("client_ip", "request_uri", "status_code", "threat_type").show(truncate=False)

# Build active IP blacklist table
df_blacklist = (
    df_threats
    .filter(F.col("threat_type").isNotNull())
    .groupBy("client_ip")
    .agg(
        F.count("threat_type").alias("incident_count"),
        F.collect_set("threat_type").alias("detected_attacks")
    )
    .withColumn("action_taken", F.lit("BLOCK_AT_FIREWALL"))
)

print("Automated Firewall IP Blacklist Feed:")
df_blacklist.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **High-throughput log analytics leverages regex pattern extraction inside Tungsten binary representations.**
# MAGIC - **Detect threat signatures (SQLi, path traversal, credential stuffing) using vectorized regex expressions.**
# MAGIC - **Automated Gold threat tables feed downstream firewall APIs (Cloudflare, AWS WAF) for immediate IP blocking.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. How do you process 500GB of daily unstructured Nginx logs in PySpark without driver OOM?
# MAGIC 2. What is the performance difference between `regexp_extract()` and Python's `re.search()` inside a UDF?
# MAGIC 3. How do you implement real-time sliding window DDoS detection using Structured Streaming?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
