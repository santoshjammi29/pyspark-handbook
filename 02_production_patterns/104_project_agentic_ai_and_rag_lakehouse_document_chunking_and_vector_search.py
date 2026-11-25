"""Project Agentic AI & RAG: Lakehouse Document Chunking & Vector Index Prep.

Scenario & Scale:
    An enterprise RAG assistant indexes 5M+ product manuals and policy docs.
    Pipelines must split text into overlapping token/word windows deterministically
    at high throughput and prepare Delta Gold tables for Vector Search sync.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Chunking documents with a row-by-row Python `@F.udf` and
# generating non-deterministic chunk IDs with `F.monotonically_increasing_id()`.
# Otherwise: Row-wise Py4J serialization throttles ingestion of millions of docs,
# and `monotonically_increasing_id()` churns primary keys on every Delta sync,
# forcing full Vector Search re-embedding instead of incremental CDC upserts.
@F.udf("array<string>")
def chunk_text_row_udf_bad(text: str) -> list[str]:
    words = (text or "").split(" ")
    return [" ".join(words[i : i + 20]) for i in range(0, len(words), 15)]


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Vectorize sliding-window chunking in native Catalyst higher-order
# functions (`split` + `sequence` + `transform` + `slice`) and assign deterministic
# SHA-256 `chunk_id` + `content_hash` for incremental Delta CDF Vector Search sync.
def transform_chunk_documents_native(
    df_docs: DataFrame,
    chunk_words: int = 20,
    overlap_words: int = 5,
) -> DataFrame:
    """Split document text into overlapping windows purely in Catalyst SQL."""
    step = max(1, chunk_words - overlap_words)

    df_arrays = (
        df_docs.filter(F.col("body_text").isNotNull() & (F.trim(F.col("body_text")) != ""))
        .withColumn("words", F.split(F.trim(F.col("body_text")), r"\s+"))
        .withColumn("word_count", F.size("words"))
        .withColumn(
            "start_positions",
            F.sequence(F.lit(1), F.greatest(F.col("word_count"), F.lit(1)), F.lit(step)),
        )
        .withColumn(
            "chunk_structs",
            F.transform(
                F.col("start_positions"),
                lambda pos, idx: F.struct(
                    idx.alias("chunk_index"),
                    F.array_join(F.slice(F.col("words"), pos, F.lit(chunk_words)), " ").alias(
                        "chunk_text"
                    ),
                ),
            ),
        )
    )

    return (
        df_arrays.select("doc_id", "title", F.explode("chunk_structs").alias("chk"))
        .select(
            F.sha2(F.concat_ws("||", F.col("doc_id"), F.col("chk.chunk_index").cast("string")), 256).alias(
                "chunk_id"
            ),
            F.col("doc_id"),
            F.col("title"),
            F.col("chk.chunk_index").alias("chunk_index"),
            F.col("chk.chunk_text").alias("chunk_text"),
            F.sha2(F.col("chk.chunk_text"), 256).alias("chunk_content_hash"),
            F.current_timestamp().alias("_chunked_at"),
        )
    )


def transform_attach_vector_index_metadata(
    df_chunked: DataFrame, embedding_dim: int = 8
) -> DataFrame:
    """Attach placeholder embedding vector schema ready for endpoint batch inference."""
    return df_chunked.withColumn(
        "embedding_vector",
        F.array([F.round(F.rand(seed=42 + i) * 2 - 1, 4) for i in range(embedding_dim)]),
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows native Catalyst `StringSplit`, `Sequence`,
#   `ArrayTransform`, and `Explode` (`Generate`) inside `WholeStageCodegen`—zero
#   Python worker serialization (`BatchEvalPython`) or shuffle overhead.
# - Remember `F.slice(arr, start, length)` in Spark SQL is **1-based indexed**!


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Deterministic `chunk_id` + `chunk_content_hash` lets Delta Change Data Feed
#    (CDF) skip re-embedding unchanged chunks when only 5% of docs are edited.
# 2. Keep chunk sizes tuned to your embedding model's sweet spot (typically
#    256-512 tokens with 10-20% overlap) so sentences don't lose semantic context.
# 3. For production embedding inference at scale, replace the mock vector array
#    with `ai_query()` / Model Serving REST batching via `mapInPandas` (`053`).


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("104_rag_chunking_vector_prep")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    docs = [
        (
            "DOC-101",
            "Smart TV Manual",
            "The Smart TV comes with a 2-year manufacturer warranty covering panel defects and audio failure. "
            "Submit your receipt online within 30 days. Accidental drops and water damage are excluded.",
        ),
        (
            "DOC-102",
            "Return Policy",
            "Customers may return unopened items within 14 days of purchase for a full refund to the original "
            "payment method. Opened items incur a restocking fee unless defective.",
        ),
    ]
    df_docs = spark.createDataFrame(docs, "doc_id STRING, title STRING, body_text STRING")
    df_chunks = transform_chunk_documents_native(df_docs, chunk_words=12, overlap_words=3)
    transform_attach_vector_index_metadata(df_chunks).select(
        "chunk_id", "doc_id", "chunk_index", "chunk_text"
    ).show(truncate=False)
