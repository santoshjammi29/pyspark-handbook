# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 050: Building a Metadata-Driven Multi-Format Ingestion Engine
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC A centralized lakehouse ingestion platform must onboard 200+ distinct source datasets daily spanning CSV, Parquet, JSON, and ORC. Writing bespoke ingestion notebooks for each dataset is unmaintainable and error-prone.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Design an extensible, configuration-driven multi-format ingestion class `UnifiedIngestionEngine` in PySpark that takes a JSON/dictionary configuration spec, validates format options, applies standard audit metadata columns (`_ingested_at`, `_source_file`), and writes standardized Delta tables.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC A metadata-driven framework separates orchestration logic from data transformation code. Pipeline configuration (source paths, file formats, options, partition keys, schema definitions) can be stored in Delta configuration tables or YAML files and executed dynamically.

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
        .appName("UseCase_050_Building_a_Metadata_Driven_Multi_Format_Ingestion_Engine")
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
# MAGIC ### Step 1: Designing Metadata Ingestion Specification Dictionary
# MAGIC Define configuration objects for heterogeneous data sources.

# COMMAND ----------

pipeline_specs = [
    {
        "table_name": "src_orders",
        "format": "json",
        "raw_path": "/tmp/src_orders_050.json",
        "target_path": "/tmp/delta_orders_050",
        "partition_cols": ["order_status"],
        "options": {"multiline": "false"}
    },
    {
        "table_name": "src_products",
        "format": "csv",
        "raw_path": "/tmp/src_products_050.csv",
        "target_path": "/tmp/delta_products_050",
        "partition_cols": [],
        "options": {"header": "true", "inferSchema": "true"}
    }
]

print(f"Loaded {len(pipeline_specs)} pipeline specifications.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Implementing the UnifiedIngestionEngine Framework
# MAGIC Create framework class that reads sources, enriches with audit columns, and writes Delta tables.

# COMMAND ----------

from datetime import datetime

class UnifiedIngestionEngine:
    def __init__(self, spark_session):
        self.spark = spark_session

    def ingest(self, spec: dict):
        table_name = spec["table_name"]
        fmt = spec["format"]
        raw_path = spec["raw_path"]
        target_path = spec["target_path"]
        partition_cols = spec.get("partition_cols", [])
        options = spec.get("options", {})

        print(f"--> Ingesting {table_name} [Format: {fmt}] from {raw_path}")

        # 1. Read dynamically using format and options
        reader = self.spark.read.format(fmt)
        for k, v in options.items():
            reader = reader.option(k, v)
        df_raw = reader.load(raw_path)

        # 2. Add standard audit columns
        df_enriched = (
            df_raw
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(datetime.utcnow().strftime("%Y%m%d%H%M%S")))
        )

        # 3. Write standardized Delta table
        writer = df_enriched.write.format("delta").mode("append")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)

        count = df_enriched.count()
        print(f"--> Successfully landed {count} records into {target_path}")
        return df_enriched

print("UnifiedIngestionEngine class ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Testing Framework with Synthetic Source Files
# MAGIC Create synthetic files and execute the dynamic pipeline.

# COMMAND ----------

# Prepare sample files
with open("/tmp/src_orders_050.json", "w") as f:
    f.write('{"order_id": "O1", "amount": 120.0, "order_status": "COMPLETED"}\n')
    f.write('{"order_id": "O2", "amount": 85.0, "order_status": "PENDING"}\n')

with open("/tmp/src_products_050.csv", "w") as f:
    f.write("prod_id,prod_name,cost\nP1,Widget,10.50\nP2,Gizmo,25.00\n")

engine = UnifiedIngestionEngine(spark)
for spec in pipeline_specs:
    engine.ingest(spec)

# Verify written Delta table
print("Verified Delta Table Output (src_orders):")
spark.read.format("delta").load("/tmp/delta_orders_050").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **Metadata-driven engines decouple data engineering transformation rules from execution infrastructure.**
# MAGIC - **Always enrich ingested Bronze data with audit columns: `_ingested_at`, `_source_file`, and `_batch_id`.**
# MAGIC - **Using a unified engine ensures consistent security, error handling, and telemetry across hundreds of pipelines.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. How do you design a metadata-driven ingestion pipeline that dynamically scales to 500+ tables?
# MAGIC 2. What is `_input_file_name()` in PySpark and why is it invaluable for audit tracing in Bronze tables?
# MAGIC 3. How do you store and version ingestion pipeline metadata (JSON, YAML, or Delta tables)?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
