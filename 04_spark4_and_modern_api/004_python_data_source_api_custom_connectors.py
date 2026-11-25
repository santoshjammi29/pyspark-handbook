"""
Module 04 / 004: Spark 4.0 Python Data Source API (Custom Connectors)

Scenario & Scale:
    A data platform ingests 500M records/day from an internal gRPC/REST shard
    service and proprietary telemetry feeds that lack a native Spark JVM JAR
    connector. Teams previously pulled pages onto the Spark driver before
    calling `spark.createDataFrame()`.

Goal:
    Implement a distributed, Arrow-backed Spark 4.0 Python Data Source
    (`DataSource`, `DataSourceReader`, `InputPartition`), register it via
    `spark.dataSource.register()`, and execute parallel partition reads directly
    across executors without driver bottlenecks or custom Scala JARs.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# (2) Anti-Pattern: Collecting paginated API results on the Spark driver
# ---------------------------------------------------------------------------
def anti_pattern_driver_ingestion(spark: SparkSession, total_shards: int) -> DataFrame:
    # ❌ Bad Practice: Fetching all shards sequentially on the Python driver into a
    # local list and calling `spark.createDataFrame(rows)`.
    # Otherwise: Every page traverses single-threaded driver memory (OOM crash beyond
    # a few million rows), executors sit idle during network I/O, and Py4J/Arrow must
    # re-serialize the entire dataset from the driver to executors before processing.
    rows = []
    for shard_id in range(total_shards):
        rows.append((shard_id, f"record_{shard_id}", float(shard_id * 10)))
    return spark.createDataFrame(rows, "shard_id INT, payload STRING, metric DOUBLE")


# ---------------------------------------------------------------------------
# (3) ANSI-safe PySpark 4.0+ Fix: Distributed Python Data Source + Arrow Batches
# ---------------------------------------------------------------------------
class ShardInputPartition(InputPartition):
    """Serializable partition descriptor shipped from driver to executors."""

    def __init__(self, shard_id: int, start_seq: int, end_seq: int) -> None:
        super().__init__(shard_id)
        self.shard_id = shard_id
        self.start_seq = start_seq
        self.end_seq = end_seq


class TelemetryShardReader(DataSourceReader):
    """Executor-parallel reader yielding zero-copy PyArrow RecordBatches."""

    def __init__(self, schema: T.StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.num_shards = int(options.get("num_shards", "4"))
        self.rows_per_shard = int(options.get("rows_per_shard", "100"))

    def partitions(self) -> list[InputPartition]:
        return [
            ShardInputPartition(
                shard_id=shard,
                start_seq=shard * self.rows_per_shard,
                end_seq=(shard + 1) * self.rows_per_shard,
            )
            for shard in range(self.num_shards)
        ]

    def read(self, partition: ShardInputPartition) -> Iterator[pa.RecordBatch]:
        # Instantiate HTTP/gRPC clients lazily inside `read()` on the executor—never on driver.
        seq_ids = list(range(partition.start_seq, partition.end_seq))
        shard_ids = [partition.shard_id] * len(seq_ids)
        payloads = [f"evt_{i}" for i in seq_ids]
        metrics = [round(i * 1.5, 2) for i in seq_ids]

        arrow_schema = pa.schema(
            [
                pa.field("shard_id", pa.int32(), nullable=False),
                pa.field("seq_id", pa.int64(), nullable=False),
                pa.field("payload", pa.string(), nullable=False),
                pa.field("metric", pa.float64(), nullable=False),
            ]
        )
        yield pa.RecordBatch.from_arrays(
            [
                pa.array(shard_ids, type=pa.int32()),
                pa.array(seq_ids, type=pa.int64()),
                pa.array(payloads, type=pa.string()),
                pa.array(metrics, type=pa.float64()),
            ],
            schema=arrow_schema,
        )


class TelemetryShardDataSource(DataSource):
    """Spark 4.0 custom Python Data Source registered under format `telemetry_shards`."""

    @classmethod
    def name(cls) -> str:
        return "telemetry_shards"

    def schema(self) -> str:
        return "shard_id INT NOT NULL, seq_id BIGINT NOT NULL, payload STRING NOT NULL, metric DOUBLE NOT NULL"

    def reader(self, schema: T.StructType) -> DataSourceReader:
        return TelemetryShardReader(schema=schema, options=self.options)


def good_practice_read_custom_source(spark: SparkSession) -> DataFrame:
    # ✅ Good Practice: Register the Python DataSource and read in parallel across
    # executors via standard DataFrameReader options.
    spark.dataSource.register(TelemetryShardDataSource)
    return (
        spark.read.format("telemetry_shards")
        .option("num_shards", "6")
        .option("rows_per_shard", "50")
        .load()
    )


# ---------------------------------------------------------------------------
# (4) explain("formatted") & Spark UI Diagnostic Notes (Comments Only)
# ---------------------------------------------------------------------------
# - In `explain("formatted")`, Spark 4.0 renders `BatchScan` / `PythonDataSourceScan`
#   with `Output` columns matching `schema()` and partition parallelism equal to
#   `len(reader.partitions())`.
# - In the Spark UI -> Stages tab, verify `numTasks == num_shards` (6 concurrent tasks
#   here) and inspect Python worker Arrow batch throughput instead of a single massive
#   `LocalTableScan` originating from the driver.


# ---------------------------------------------------------------------------
# (5) Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Pickling Boundary: `DataSource`, `DataSourceReader`, and `InputPartition` objects
#    are cloudpickled to executors. Never store open `requests.Session`, sockets, or DB
#    connections on `self` in `__init__`; open them lazily inside `read(partition)`.
# 2. Arrow Batch vs Row Tuples: Yielding `pa.RecordBatch` from `read()` leverages Apache
#    Arrow columnar transfer, avoiding row-by-row Python pickling overhead and achieving
#    near-JVM scan throughput.
# 3. Deterministic Schema Discovery: Keep `schema()` fast and metadata-only (or inspect
#    a single sample page) because Catalyst invokes it on the driver during query planning.
# 4. Streaming & Write Extensions: Implement `DataSourceStreamReader` (`initialOffset`,
#    `latestOffset`, `partitions`) for Structured Streaming micro-batches, and
#    `DataSourceWriter` (`write`, `commit`, `abort`) for transactional custom sinks.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("004_Python_DataSource_API")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    try:
        df = good_practice_read_custom_source(spark)
        df.groupBy("shard_id").count().orderBy("shard_id").show()
    finally:
        spark.stop()
