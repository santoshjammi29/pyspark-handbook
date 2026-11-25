"""
Module 04 / 005: Polymorphic Python UDTFs & TABLE Arguments (Spark 4.0+)

Scenario & Scale:
    A financial fraud team runs stateful sessionization and dynamic key-value
    pivoting across 4B clickstream events/day. Different product verticals pass
    arbitrary metric columns into the same Python table function, requiring
    partitioned, time-ordered stateful processing without Scala UDTFs.

Goal:
    Build a polymorphic Spark 4.0 Python UDTF using `@udtf`, `analyze()` for
    dynamic output schemas, `AnalyzeResult(partitionBy=..., orderBy=...)`
    for distributed TABLE argument ordering, and Arrow-vectorized execution.
"""

from __future__ import annotations

from collections.abc import Iterator

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.udtf import AnalyzeArgument, AnalyzeResult, OrderingColumn, PartitioningColumn, udtf


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: Row-by-row Array<Struct> UDF + explode() without partitioning
# ---------------------------------------------------------------------------
def anti_pattern_udf_explode(df: DataFrame) -> DataFrame:
    # ❌ Bad Practice: Returning `ArrayType(StructType(...))` from a scalar UDF and
    # calling `F.explode()`, or computing sessionization via `collect_list()` per account.
    # Otherwise: `collect_list` buffers entire user histories in executor heap (OOM on
    # power-law bot accounts), scalar UDFs cannot emit dynamic output schemas per call
    # site, and row-by-row pickling destroys throughput.
    grouped = df.groupBy("account_id").agg(F.collect_list("event_ts").alias("events"))
    return grouped.withColumn("event_ts", F.explode(F.col("events"))).drop("events")


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Polymorphic @udtf with analyze() & TABLE args
# ---------------------------------------------------------------------------
@udtf
class SessionizeEventsUDTF:
    """Stateful sessionizer emitting dynamic output schema driven by TABLE arg."""

    @staticmethod
    def analyze(table_arg: AnalyzeArgument, gap_seconds: AnalyzeArgument) -> AnalyzeResult:
        # Derive output schema dynamically from whatever columns the caller passes.
        input_fields = [f for f in table_arg.dataType.fields if f.name != "event_ts"]
        out_schema = T.StructType(
            input_fields
            + [
                T.StructField("event_ts", T.LongType(), nullable=False),
                T.StructField("session_seq", T.IntegerType(), nullable=False),
            ]
        )
        return AnalyzeResult(
            schema=out_schema,
            partitionBy=[PartitioningColumn("account_id")],
            orderBy=[OrderingColumn("event_ts", ascending=True)],
        )

    def __init__(self) -> None:
        self.current_account: str | None = None
        self.last_ts: int | None = None
        self.session_seq: int = 0

    def eval(self, row: T.Row, gap_seconds: int) -> Iterator[tuple]:
        if row.account_id is None or row.event_ts is None:
            return
        if row.account_id != self.current_account:
            self.current_account = row.account_id
            self.last_ts = int(row.event_ts)
            self.session_seq = 1
        elif (int(row.event_ts) - (self.last_ts or 0)) > int(gap_seconds):
            self.session_seq += 1
            self.last_ts = int(row.event_ts)
        else:
            self.last_ts = int(row.event_ts)

        passthru = tuple(row[f] for f in row.__fields__ if f != "event_ts")
        yield passthru + (int(row.event_ts), self.session_seq)

    def terminate(self) -> Iterator[tuple]:
        # Called once per partition at end-of-stream; flush buffered state here if needed.
        return iter(())


def good_practice_polymorphic_udtf(spark: SparkSession, events_df: DataFrame) -> DataFrame:
    # ✅ Good Practice: Register the polymorphic UDTF and pass a TABLE argument so
    # Catalyst automatically injects hash partitioning + sort order requested by analyze().
    spark.udtf.register("sessionize_events", SessionizeEventsUDTF)
    events_df.createOrReplaceTempView("raw_events_005")

    return spark.sql(
        """
        SELECT *
        FROM sessionize_events(
            TABLE(raw_events_005),
            1800
        )
        """
    )


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - In `explain("formatted")`, Catalyst wraps `EvalPythonUDTF` / `ArrowEvalPythonUDTF`
#   with `Sort [account_id ASC, event_ts ASC]` <- `Exchange hashpartitioning(account_id)`
#   automatically generated from `AnalyzeResult(partitionBy=..., orderBy=...)`.
# - In Spark UI -> SQL tab, inspect the `Exchange` shuffle bytes and Python UDTF
#   `number of output rows` vs `input rows` to verify streaming O(1) memory per key
#   instead of `ObjectHashAggregate` / `collect_list` buffer spills.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Avoid `withSinglePartition=True` on Big Tables: `AnalyzeResult(withSinglePartition=True)`
#    forces all rows into a single Spark task (guaranteed OOM on GB+ inputs); always
#    prefer `partitionBy=[PartitioningColumn(...)]` for parallel keyed execution.
# 2. `analyze()` Runs on the Driver at Plan Time: Keep `analyze()` pure metadata logic—
#    never make network calls or read external files inside `analyze()`.
# 3. `eval()` + `terminate()` Lifecycle: One Python UDTF instance processes rows in a
#    partition sequentially; `terminate()` runs once when the partition finishes, making it
#    ideal for emitting final window summaries without holding all partitions in RAM.
# 4. Arrow Vectorized UDTFs: Decorate batch-oriented UDTFs with `@udtf(useArrow=True)`
#    (when schema is fixed or arrow-compatible) to eliminate per-row pickle serialization.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("005_Polymorphic_Python_UDTFs")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    try:
        data = [
            ("acct_1", "checkout", 1000),
            ("acct_1", "confirm", 1200),
            ("acct_1", "login", 5000),
            ("acct_2", "view", 2000),
        ]
        df_in = spark.createDataFrame(data, "account_id STRING, action STRING, event_ts BIGINT")
        good_practice_polymorphic_udtf(spark, df_in).show(truncate=False)
    finally:
        spark.stop()
