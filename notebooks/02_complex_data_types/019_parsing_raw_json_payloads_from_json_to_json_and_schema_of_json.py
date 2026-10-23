# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook 019: Parsing Raw JSON Payloads: from_json, to_json and schema_of_json
# MAGIC
# MAGIC **Module:** Module 02: Complex Data Types & Nested Structures Handling  
# MAGIC **Difficulty:** `Intermediate`  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Business Scenario & Real-World Context
# MAGIC A payment gateway streams raw JSON transaction events via Apache Kafka or cloud storage. Each record arrives as a single raw string column containing nested transaction data that must be parsed into typed columns.
# MAGIC
# MAGIC ## 2. Technical Requirement & Problem Statement
# MAGIC Master JSON string handling: dynamically infer schema using `schema_of_json()`, parse stringified JSON into structured structs using `from_json()`, serialize DataFrames back to JSON using `to_json()`, and handle schema evolution options.
# MAGIC
# MAGIC ## 3. Spark & Databricks Architecture Notes
# MAGIC `from_json()` parses JSON strings inside JVM tasks using Jackson parser, avoiding Python serialization. Specifying an explicit schema avoids costly full-table scans to infer JSON structure.

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
    .appName("UseCase_019_Parsing_Raw_JSON_Payloads_from_json,_to_json_and_schema_of_json")
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
# MAGIC ## 5. Generating Raw Kafka-like Message Strings
# MAGIC Create sample DataFrame with raw JSON payload strings.

# COMMAND ----------

kafka_stream = [
    ("K-001", '{"order_id": "ORD-1", "customer": {"id": "C-10", "email": "c10@mail.com"}, "amount": 149.99, "tags": ["sale", "prime"]}'),
    ("K-002", '{"order_id": "ORD-2", "customer": {"id": "C-20", "email": "c20@mail.com"}, "amount": 89.50, "tags": ["clearance"]}'),
    ("K-003", '{"order_id": "ORD-3", "customer": {"id": "C-30", "email": "c30@mail.com"}, "amount": 420.00, "tags": []}')
]

df_kafka = spark.createDataFrame(kafka_stream, ["kafka_offset", "payload"])
df_kafka.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inferring Schema via schema_of_json & Explicit Parsing
# MAGIC Extract schema from a sample row and parse payloads into typed structs.

# COMMAND ----------

# 1. Infer sample DDL schema using schema_of_json
sample_json = df_kafka.select("payload").first()[0]
inferred_ddl = spark.range(1).select(F.schema_of_json(F.lit(sample_json))).first()[0]
print(f"Inferred Schema DDL: {inferred_ddl}")

# 2. Parse using explicit StructType schema for production safety
order_schema = T.StructType([
    T.StructField("order_id", T.StringType(), True),
    T.StructField("customer", T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("email", T.StringType(), True)
    ]), True),
    T.StructField("amount", T.DoubleType(), True),
    T.StructField("tags", T.ArrayType(T.StringType()), True)
])

df_parsed = df_kafka.withColumn("data", F.from_json(F.col("payload"), order_schema))
df_parsed.printSchema()
df_parsed.select("kafka_offset", "data.order_id", "data.customer.email", "data.amount", "data.tags").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Serializing Structs Back to JSON with to_json
# MAGIC Serialize structured data back into JSON strings for downstream API dispatch.

# COMMAND ----------

df_re_serialized = df_parsed.select(
    "kafka_offset",
    F.to_json("data").alias("exported_json")
)

df_re_serialized.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Key Architectural Takeaways
# MAGIC - **Always supply explicit schemas to `from_json()` to avoid driver-side inference bottlenecks.**
# MAGIC - **`schema_of_json()` is a helpful utility for deriving schema DDL strings from representative payloads.**
# MAGIC - **`to_json()` serializes complex structs and maps back into compact JSON strings.**
# MAGIC
# MAGIC ## 10. Senior Data Engineering Interview Questions
# MAGIC 1. What happens when a JSON payload contains keys that are not defined in the `from_json` schema?
# MAGIC 2. How does `from_json` performance compare to parsing JSON with Python's built-in `json.loads` UDF?
# MAGIC 3. How do you handle schema evolution when incoming JSON payloads gain new fields over time?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
