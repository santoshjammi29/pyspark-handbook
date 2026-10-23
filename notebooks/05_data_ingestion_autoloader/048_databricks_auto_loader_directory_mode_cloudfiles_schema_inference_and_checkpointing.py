# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 048: Databricks Auto Loader Directory Mode: cloudFiles, Schema Inference and Checkpointing
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A cloud storage bucket receives thousands of landing files (JSON and CSV) every hour from IoT devices. Traditional batch file listings (`os.listdir` or `dbutils.fs.ls`) become prohibitively slow as the bucket scales past 100,000 files.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Implement Databricks Auto Loader using the `cloudFiles` format: configure directory listing mode, enable automatic schema inference (`cloudFiles.inferColumnTypes`), maintain state checkpointing, and stream landing files into a Bronze Delta table with exactly-once guarantees.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Auto Loader uses `cloudFiles` source to incrementally discover landing files without re-scanning previously processed files. Directory listing mode scans file metadata incrementally, tracking discovered files in RocksDB state storage within the checkpoint directory.

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
    .appName("UseCase_048_Databricks_Auto_Loader_Directory_Mode_cloudFiles,_Schema_Inference_and_Checkpointing")
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
# MAGIC ## 5. Creating Mock Landing Storage Directory
# MAGIC Create staging directories for landing files and checkpoint state.

# COMMAND ----------

landing_dir = Path("/tmp/autoloader_landing_048")
checkpoint_dir = Path("/tmp/autoloader_chk_048")
bronze_table_dir = Path("/tmp/autoloader_bronze_048")

for p in [landing_dir, checkpoint_dir, bronze_table_dir]:
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)

# Write initial landing JSON file
batch1_file = landing_dir / "sensor_batch_1.json"
with open(batch1_file, "w") as f:
    f.write('{"device_id": "IOT-1", "temp": 24.5, "reading_ts": "2023-11-20T10:00:00Z"}\n')
    f.write('{"device_id": "IOT-2", "temp": 31.2, "reading_ts": "2023-11-20T10:00:00Z"}\n')

print(f"Landing batch 1 written to {batch1_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Auto Loader Configuration Blueprint (cloudFiles)
# MAGIC Display the production Databricks Auto Loader syntax.

# COMMAND ----------

# Production Databricks Auto Loader Streaming Pipeline:
autoloader_stream_code = f"""
df_autoloader = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "{checkpoint_dir}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("{landing_dir}")
)

query = (
    df_autoloader.writeStream
    .format("delta")
    .option("checkpointLocation", "{checkpoint_dir}/cp")
    .trigger(availableNow=True)
    .start("{bronze_table_dir}")
)
query.awaitTermination()
"""

print("Auto Loader Blueprint Code:")
print(autoloader_stream_code)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Simulating Incremental Auto Loader Discovery Locally
# MAGIC Process batch files incrementally into Delta Bronze table.

# COMMAND ----------

# In standard PySpark local mode, we ingest the landing JSON and write to Delta Bronze
df_landing = spark.read.json(str(landing_dir))
df_landing.write.format("delta").mode("append").save(str(bronze_table_dir))

print("Bronze Table after First Ingestion:")
spark.read.format("delta").load(str(bronze_table_dir)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Databricks Auto Loader (`format('cloudFiles')`) incrementally ingests millions of files with low latency and zero duplicate processing.**
# MAGIC - **`cloudFiles.schemaLocation` persists inferred schemas across stream restarts, preventing schema drift failures.**
# MAGIC - **Auto Loader supports Directory Listing mode (for smaller loads) and File Notification mode (using AWS SQS / Azure Event Grid) for millions of files.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What is the difference between Directory Listing mode and File Notification mode in Databricks Auto Loader?
# MAGIC 2. Why is Auto Loader superior to standard `spark.read.json('s3://bucket/*.json')` on large cloud buckets?
# MAGIC 3. How does Auto Loader ensure exactly-once ingestion semantics?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
