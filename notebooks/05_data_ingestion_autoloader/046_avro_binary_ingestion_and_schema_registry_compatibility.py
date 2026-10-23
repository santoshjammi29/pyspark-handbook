# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 046: Avro Binary Ingestion and Schema Registry Compatibility
# MAGIC
# MAGIC **Module:** Module 05: Data Ingestion Patterns, Auto Loader & File Formats  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC An enterprise Apache Kafka event bus publishes high-throughput customer account updates serialized in Apache Avro format. Data engineering must consume Avro payloads using explicit Avro schemas compatible with the Confluent Schema Registry.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Ingest and serialize Apache Avro binary records in PySpark: use `from_avro()` and `to_avro()`, define JSON Avro schema specifications, handle schema evolution between writer and reader schemas, and package binary payloads.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC Apache Avro is a row-oriented binary serialization format widely used for streaming pipelines due to its compact size and schema evolution rules. The `from_avro` function decodes binary Kafka bytes into Spark StructType columns using an Avro schema string.

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
    .appName("UseCase_046_Avro_Binary_Ingestion_and_Schema_Registry_Compatibility")
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
# MAGIC ## 5. Defining Avro Schema JSON Specification
# MAGIC Define standard Avro JSON schema.

# COMMAND ----------

avro_schema_json = """{
  "type": "record",
  "name": "CustomerEvent",
  "namespace": "com.enterprise.events",
  "fields": [
    {"name": "customer_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "account_balance", "type": "double"},
    {"name": "event_timestamp", "type": "long"}
  ]
}"""

print("Avro Schema JSON defined successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulating Avro Ingestion and Struct Conversion
# MAGIC Create events and demonstrate Avro integration patterns.

# COMMAND ----------

# Create representative structured records
events = [
    ("CUST-101", "LOGIN", 1450.50, 1700560800),
    ("CUST-102", "PURCHASE", 320.00, 1700560950),
    ("CUST-103", "LOGOUT", 8900.00, 1700561100)
]

cols = ["customer_id", "event_type", "account_balance", "event_timestamp"]
df_events = spark.createDataFrame(events, cols)
df_events.printSchema()
df_events.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Packaging Structs for Streaming Message Buses
# MAGIC Bundle fields into structs for downstream message publishing.

# COMMAND ----------

# Convert to unified message payload struct matching Avro schema
df_payload = df_events.select(
    F.col("customer_id").alias("key"),
    F.struct("customer_id", "event_type", "account_balance", "event_timestamp").alias("value")
)

df_payload.printSchema()
df_payload.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Avro is the industry standard format for streaming message buses (Kafka, Event Hubs) due to compact binary encoding.**
# MAGIC - **`from_avro(col, schema)` parses binary payloads using the exact Avro schema definition.**
# MAGIC - **Schema evolution in Avro allows adding optional fields with default values without breaking downstream consumers.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. Why is Avro preferred for message bus streaming while Parquet is preferred for analytical storage?
# MAGIC 2. How does Confluent Schema Registry prevent incompatible schema changes in Kafka-Spark pipelines?
# MAGIC 3. What is the difference between Writer Schema and Reader Schema in Apache Avro?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
