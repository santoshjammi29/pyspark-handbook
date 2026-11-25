"""
Module 04 / 001: Spark Connect Decoupled Client-Server Architecture (PySpark 4.0+)

Scenario & Scale:
    A multi-tenant analytics platform serves 200+ notebooks, FastAPI microservices,
    and CI runners executing Spark 4.0+ jobs against shared remote clusters
    processing 50+ TB/day. Embedding heavy JVM drivers inside each client
    container causes dependency conflicts, slow cold starts, and frequent
    client-side OOMs.

Goal:
    Connect thin Python clients over gRPC (`sc://`) using Spark Connect,
    eliminating Py4J/JVM coupling, `sparkContext`, and `.rdd` dependencies while
    maintaining full DataFrame/SQL parity and tag-based query observability.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_connect_session(endpoint: str | None = None) -> SparkSession:
    """Create a decoupled Spark Connect session without local JVM startup."""
    target = endpoint or os.getenv("SPARK_REMOTE", "sc://localhost:15002")
    return (
        SparkSession.builder.appName("001_Spark_Connect_Client")
        .remote(target)
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: Reaching into JVM/Py4J internals or RDD/SparkContext APIs
# ---------------------------------------------------------------------------
def anti_pattern_py4j_coupling(spark: SparkSession) -> None:
    # ❌ Bad Practice: Accessing JVM internals, SparkContext, or RDDs on a Spark Connect client:
    #     _ = SparkContext.getOrCreate().applicationId
    #     _ = spark._jvm.java.lang.System.currentTimeMillis()
    #     _ = spark.range(100).rdd.map(lambda row: (row.id, row.id * 2)).collect()
    # Otherwise: Spark Connect raises PySparkNotImplementedError / SparkConnectException
    # immediately because no JVM SparkContext or Py4J bridge exists in the thin client,
    # breaking microservice deployments and preventing serverless/shared-cluster execution.
    raise NotImplementedError(
        f"Py4J/RDD APIs are unavailable on Spark Connect session {spark.session_id}"
    )


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Pure DataFrame/SQL + Tag-Based Control
# ---------------------------------------------------------------------------
def good_practice_connect_pipeline(spark: SparkSession) -> DataFrame:
    # ✅ Good Practice: Express transforms via DataFrame/Column APIs serialized as
    # Spark Connect LogicalPlan protobuf messages over gRPC, tagged for server triage.
    schema = T.StructType(
        [
            T.StructField("tenant_id", T.StringType(), nullable=False),
            T.StructField("raw_amount", T.StringType(), nullable=True),
            T.StructField("event_ts", T.StringType(), nullable=False),
        ]
    )
    sample = [
        ("t_emea", "1250.50", "2026-01-15T08:30:00Z"),
        ("t_apac", "invalid_num", "2026-01-15T08:31:00Z"),
        ("t_emea", "300.00", "2026-01-15T08:32:00Z"),
    ]
    df = spark.createDataFrame(sample, schema=schema)

    spark.addTag("svc=billing-api")
    spark.addTag("env=prod")
    try:
        result = (
            df.withColumn("amount", F.try_cast(F.col("raw_amount"), T.DecimalType(12, 2)))
            .withColumn("event_time", F.to_timestamp(F.col("event_ts")))
            .filter(F.col("amount").isNotNull())
            .groupBy("tenant_id")
            .agg(
                F.count("*").alias("valid_txns"),
                F.sum("amount").alias("total_amount"),
            )
        )
        return result
    finally:
        spark.removeTag("svc=billing-api")
        spark.removeTag("env=prod")


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - Calling `result.explain("formatted")` on a Spark Connect client sends an AnalyzePlan
#   gRPC request to the remote Spark Connect server, runs Catalyst optimization on the
#   driver, and streams only the formatted physical plan string back to the thin client.
# - Look for `HashAggregate` -> `Exchange hashpartitioning(tenant_id)` -> `LocalTableScan`
#   (or `FileScan parquet` in production) and verify `try_cast` appears in `Project`
#   instead of unsafe `cast` under Spark 4.0 ANSI defaults.
# - In the Spark UI -> Connect tab, filter active and completed gRPC executions by
#   `session_id`, `operation_id`, and `user_tags` (`svc=billing-api`) to pinpoint slow
#   plans or cancel runaway client queries via `spark.interruptTag("svc=billing-api")`.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Lazy Evaluation Over gRPC: Transformations build a client-side LogicalPlan AST in
#    memory with zero network calls until an action (`show`, `collect`, `write`, `explain`)
#    dispatches the serialized protobuf to the remote server.
# 2. Python Environment Parity: Thin clients only need `pyspark-connect` (pure Python +
#    grpcio + pyarrow), whereas Python UDFs/UDTFs execute on remote server workers that
#    must have matching Python minor versions and third-party wheel packages.
# 3. gRPC Payload Caps: `createDataFrame` on large local collections or `toPandas()` on
#    unbounded results can hit `spark.connect.grpc.maxInboundMessageSize` (default 128 MB);
#    always stage large inputs in object storage and aggregate/limit before `toPandas()`.
# 4. Shared Session Tag Hygiene: Multiple async requests sharing one Spark Connect session
#    must always clear tags in `try ... finally` blocks so `interruptTag()` never kills
#    unrelated workloads.


if __name__ == "__main__":
    session = build_connect_session()
    try:
        good_practice_connect_pipeline(session).show(truncate=False)
    finally:
        session.stop()
