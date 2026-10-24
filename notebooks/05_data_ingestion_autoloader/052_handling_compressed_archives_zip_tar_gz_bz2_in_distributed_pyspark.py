# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 052: Handling Compressed Archives (.zip, .tar.gz, .bz2) in Distributed PySpark
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A regulatory agency deposits quarterly compliance archives as massive 5GB ZIP and TAR.GZ files. Standard Spark readers cannot split ZIP archives across executors, causing single-node driver OutOfMemory (OOM) crashes.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Ingest and process compressed archives in distributed Spark: understand file splittability differences across codecs (Gzip, Bzip2, Snappy, Zip), implement parallel extraction using `binaryFiles` and distributed RDD partitions, and convert decompressed streams into DataFrames.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC Compression formats without sync markers (GZIP, ZIP) are not splittable: a single 5GB GZIP file must be processed by exactly one Spark task. Bzip2 and Snappy (within container formats like Parquet/Avro) are splittable across tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Setup & SparkSession Initialization
# MAGIC If running inside a Databricks notebook, the `spark` session is pre-initialized by the cluster.
# MAGIC If executing outside Databricks (e.g. VS Code, local CLI, CI/CD runner), we initialize a local SparkSession with Delta Lake support.

# COMMAND ----------

import os
import sys

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_052_Handling_Compressed_Archives_zip_targz_bz2_in_Distributed_PySpark")
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
# MAGIC ### Step 1: Creating a Mock In-Memory Compressed ZIP Archive
# MAGIC Generate a sample ZIP file containing multiple CSV files.

# COMMAND ----------

import zipfile
import io

zip_dir = Path("/tmp/zip_archives_052")
if zip_dir.exists():
    shutil.rmtree(zip_dir)
zip_dir.mkdir(parents=True, exist_ok=True)

# Create a zip file containing 2 inner CSV files
zip_path = zip_dir / "quarterly_reports.zip"
with zipfile.ZipFile(zip_path, "w") as zf:
    zf.writestr("q1_sales.csv", "report_id,amount\nR1,500.0\nR2,800.0\n")
    zf.writestr("q2_sales.csv", "report_id,amount\nR3,650.0\nR4,920.0\n")

print(f"Created sample ZIP archive at {zip_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Distributed Extraction via binaryFiles and FlatMap
# MAGIC Extract compressed files in parallel across worker nodes without driver bottlenecks.

# COMMAND ----------

# binaryFiles returns (file_path, PortableDataStream)
rdd_binary = spark.sparkContext.binaryFiles(str(zip_dir))

def extract_zip_entries(tuple_item):
    file_path, byte_stream = tuple_item
    records = []
    # Read bytes into ZipFile in memory on executor
    with zipfile.ZipFile(io.BytesIO(byte_stream)) as z:
        for inner_filename in z.namelist():
            if inner_filename.endswith(".csv"):
                content = z.read(inner_filename).decode("utf-8")
                lines = content.strip().split("\n")
                # Skip header, extract rows
                for line in lines[1:]:
                    parts = line.split(",")
                    records.append((inner_filename, parts[0], float(parts[1])))
    return records

# Distributed flatMap extracts files across cluster executors
rdd_extracted = rdd_binary.flatMap(extract_zip_entries)
df_decompressed = spark.createDataFrame(rdd_extracted, ["archive_member", "report_id", "amount"])

print("Decompressed Records Extracted in Parallel:")
df_decompressed.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Codecs Splittability Cheatsheet
# MAGIC Review splittability of common big data compression codecs.

# COMMAND ----------

codecs = [
    ("GZIP (.gz)", "No", "High", "Text / Web Logs", "Cannot split without container format"),
    ("BZIP2 (.bz2)", "Yes", "Very High", "Cold Archival", "Splittable via 48-bit sync markers"),
    ("SNAPPY", "Yes (in Parquet/Avro)", "Medium", "Active Analytics", "High-speed decompression"),
    ("ZSTD", "Yes (in Parquet)", "Very High", "Modern Lakehouse", "Balanced compression and speed"),
    ("ZIP (.zip)", "No", "Medium", "Legacy Vendor Feeds", "Requires custom parallel unpacking")
]

df_codecs = spark.createDataFrame(codecs, ["codec", "splittable", "compression_ratio", "primary_use_case", "notes"])
df_codecs.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Standard ZIP and raw GZIP files cannot be split across multiple tasks—one worker core must decompress the whole file.**
# MAGIC - **Use `sparkContext.binaryFiles` with `flatMap` to unpack multi-file ZIP archives in parallel across executors.**
# MAGIC - **Always store analytical data in splittable columnar formats (Snappy/ZSTD-compressed Parquet or Delta).**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. Why is a 10GB single GZIP file an anti-pattern in Apache Spark?
# MAGIC 2. What makes Bzip2 splittable compared to standard Gzip?
# MAGIC 3. How does Parquet enable splittable reads even when using non-splittable block codecs like Snappy?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
