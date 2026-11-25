"""Pytest Unit Testing with assertDataFrameEqual & assertSchemaEqual (PySpark 4.0+).

Scenario & Scale:
    An enterprise platform enforces automated CI/CD unit tests across 200+
    PySpark pipelines. Tests must complete in under 30 seconds locally without
    external storage, heavy test harnesses, or fragile `.collect()` comparisons.
"""

from collections.abc import Callable

import pytest
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Comparing sorted `.collect()` lists manually or writing to
# live storage during unit tests without schema/nullability assertions.
# Otherwise: Driver OOM on larger fixtures, flaky row-order failures, hidden
# schema drift (e.g. INT vs BIGINT), and slow, non-hermetic CI runs.
def assert_orders_bad(actual_df: DataFrame, expected_rows: list) -> None:
    assert actual_df.collect() == expected_rows  # Fragile order & no schema check


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Pure transformation functions tested via pytest fixtures,
# `assertDataFrameEqual`, `assertSchemaEqual`, and dependency injection/mocking.
ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType(), nullable=False),
    T.StructField("amount", T.DecimalType(12, 2), nullable=True),
    T.StructField("order_ts", T.TimestampType(), nullable=True),
])


def transform_cleanse_orders(df: DataFrame) -> DataFrame:
    """Normalize order records safely under PySpark 4.0 ANSI mode."""
    return (
        df.filter(F.col("order_id").isNotNull())
        .withColumn("amount", F.try_cast(F.col("raw_amount"), T.DecimalType(12, 2)))
        .withColumn("order_ts", F.try_to_timestamp(F.col("raw_ts"), F.lit("yyyy-MM-dd HH:mm:ss")))
        .select("order_id", "amount", "order_ts")
    )


def run_order_pipeline(
    reader_fn: Callable[[], DataFrame],
) -> DataFrame:
    """Decoupled pipeline entrypoint accepting an injectable reader boundary."""
    return transform_cleanse_orders(reader_fn())


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Lightweight session-scoped SparkSession with ANSI mode enabled."""
    session = (
        SparkSession.builder.appName("pytest_pyspark_unit_suite")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_transform_cleanse_orders_ansi_safe(spark: SparkSession) -> None:
    """Verify ANSI-safe casting turns malformed inputs to NULL and drops null keys."""
    input_df = spark.createDataFrame(
        [
            ("ORD-1", "125.50", "2025-01-15 10:30:00"),
            ("ORD-2", "BAD_NUM", "not-a-timestamp"),
            (None, "50.00", "2025-01-15 11:00:00"),
        ],
        "order_id STRING, raw_amount STRING, raw_ts STRING",
    )

    actual_df = transform_cleanse_orders(input_df)

    expected_df = spark.createDataFrame(
        [
            ("ORD-1", "125.50", "2025-01-15 10:30:00"),
            ("ORD-2", None, None),
        ],
        "order_id STRING, raw_amount STRING, raw_ts STRING",
    ).select(
        F.col("order_id").cast(T.StringType()).alias("order_id"),
        F.try_cast(F.col("raw_amount"), T.DecimalType(12, 2)).alias("amount"),
        F.try_to_timestamp(F.col("raw_ts"), F.lit("yyyy-MM-dd HH:mm:ss")).alias("order_ts"),
    )

    assertSchemaEqual(actual_df.schema, expected_df.schema, ignoreNullable=True)
    assertDataFrameEqual(actual_df, expected_df, checkRowOrder=False)


def test_run_order_pipeline_with_mocked_reader(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mock external I/O without touching cloud storage or Delta tables."""
    sample_df = spark.createDataFrame(
        [("ORD-9", "42.00", "2025-02-01 00:00:00")],
        "order_id STRING, raw_amount STRING, raw_ts STRING",
    )
    mock_source = {"loader": lambda: sample_df}
    monkeypatch.setitem(mock_source, "loader", lambda: sample_df)

    result_df = run_order_pipeline(mock_source["loader"])
    assert result_df.count() == 1
    assertDataFrameEqual(
        result_df.select("order_id"),
        spark.createDataFrame([("ORD-9",)], "order_id STRING"),
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - Disable Spark UI (`spark.ui.enabled=false`) and set `shuffle.partitions=1`
#   in test fixtures to cut unit test startup overhead by ~60%.
# - `assertDataFrameEqual` prints unified row-diff tables on mismatch without
#   forcing manual `.collect()` sorting logic in user test code.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. Always pin `spark.sql.session.timeZone = UTC` and `spark.sql.ansi.enabled = true`
#    in test fixtures so CI matches production Spark 4.0 runtime behavior.
# 2. `assertSchemaEqual(..., ignoreNullable=True)` avoids false negatives because
#    `createDataFrame` infers `nullable=True` while UDFs/filters may alter it.
# 3. Pass `checkRowOrder=False` unless testing an explicit `orderBy()` contract;
#    distributed shuffles make row order non-deterministic across partitions.
