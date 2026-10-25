# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 104: Project Agentic AI & RAG: Lakehouse Document Chunking and Vector Search
# MAGIC
# MAGIC **Module:** Module 10: End-to-End Enterprise Projects & Production Workflows
# MAGIC **Difficulty:** `Advanced`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Business Scenario & Real-World Context
# MAGIC An enterprise customer service platform deploys an Agentic AI chatbot powered by Databricks Genie and LLMs. The chatbot needs to answer questions about company product manuals, warranty terms, and legal policies stored across unstructured PDF and text documents in the Lakehouse.
# MAGIC
# MAGIC ### Technical Requirement & Problem Statement
# MAGIC Build an enterprise RAG (Retrieval-Augmented Generation) data preparation pipeline: (1) Ingest raw documentation from Unity Catalog Volumes, (2) Split long text into overlapping chunks using sliding windows, (3) Generate dense vector embeddings simulation, and (4) Prepare Gold vector index tables for Vector Search.
# MAGIC
# MAGIC ### Spark & Databricks Architecture Notes
# MAGIC RAG architectures require chunking large documents into semantically coherent segments (e.g. 500 characters with 100 character overlap) and embedding them into high-dimensional vectors. Databricks Vector Search indexes Delta tables natively, enabling hybrid semantic and keyword search.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Setup & SparkSession Initialization
# MAGIC If running inside a Databricks notebook, the `spark` session is pre-initialized by the cluster.
# MAGIC If executing outside Databricks (e.g. VS Code, local CLI, CI/CD runner), we initialize a local SparkSession with Delta Lake support.

# COMMAND ----------

import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_104_Project_Agentic_AI_and_RAG_Lakehouse_Document_Chunking_and_Vector_Search")
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
# MAGIC ### Step 1: Ingesting Unstructured Product Documentation
# MAGIC Create sample product warranty and documentation text.

# COMMAND ----------

docs = [
    ("DOC-101", "Smart TV Manual", "The Smart TV comes with a 2-year manufacturer warranty covering panel defects and audio failure. To request service, submit your receipt online within 30 days of the incident. Accidental drops and water damage are not covered under the warranty terms."),
    ("DOC-102", "Return Policy", "Customers may return unopened items within 14 days of purchase for a full refund to the original payment method. Opened items are subject to a 15% restocking fee unless defective. Software downloads are strictly non-refundable.")
]

schema = "doc_id STRING, title STRING, body_text STRING"
df_docs = spark.createDataFrame(docs, schema)
df_docs.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Text Chunking via Sliding Windows in PySpark
# MAGIC Split text into overlapping chunks using higher-order functions or UDFs.

# COMMAND ----------

def chunk_text(text, chunk_size=120, overlap=30):
    words = text.split(" ")
    chunks = []
    i = 0
    step = max(1, (chunk_size - overlap) // 6)
    window_words = chunk_size // 6
    while i < len(words):
        chunk = " ".join(words[i : i + window_words])
        if chunk.strip():
            chunks.append(chunk)
        i += step
    return chunks

chunk_udf = F.udf(chunk_text, T.ArrayType(T.StringType()))

df_chunked = (
    df_docs
    .withColumn("chunks", chunk_udf("body_text"))
    .select("doc_id", "title", F.explode("chunks").alias("chunk_text"))
    .withColumn("chunk_id", F.concat(F.col("doc_id"), F.lit("_CHK_"), F.monotonically_increasing_id()))
)

print("Document Chunks Prepared for Embedding Generation:")
df_chunked.select("chunk_id", "title", "chunk_text").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Vector Embedding Representation & Gold Vector Index Schema
# MAGIC Represent dense embedding vectors and prepare schema for Databricks Vector Search.

# COMMAND ----------

# Simulate 8-dimensional dense vector embeddings for demonstration
df_vector_ready = (
    df_chunked
    .withColumn(
        "embedding_vector",
        F.array([F.round(F.rand(seed=42 + i) * 2 - 1, 4) for i in range(8)])
    )
    .withColumn("_embedded_at", F.current_timestamp())
)

print("Vector Search Index Table Schema:")
df_vector_ready.printSchema()
df_vector_ready.select("chunk_id", "title", "embedding_vector").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Architectural Takeaways
# MAGIC - **RAG pipelines bridge unstructured enterprise documents with LLM reasoning via Vector Search.**
# MAGIC - **Chunking with overlap preserves context across sentence boundaries.**
# MAGIC - **Delta Lake serves as the unified storage layer for both raw text documents and high-dimensional vector embeddings.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Senior Data Engineering Interview Questions
# MAGIC 1. What is Chunk Overlap in RAG pipelines and why is it important for semantic retrieval?
# MAGIC 2. How does Databricks Vector Search synchronize automatically with underlying Delta tables?
# MAGIC 3. What are the trade-offs between dense semantic vector search and sparse keyword search (BM25)?
# MAGIC
# MAGIC ---
# MAGIC *Created for the **Spark & Databricks Coding Use Cases Masterclass**.*
