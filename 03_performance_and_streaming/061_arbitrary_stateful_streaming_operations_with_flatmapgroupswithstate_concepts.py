"""Scenario & Scale: Telecom SIM-swap & impossible-travel fraud detector.

Tracks per-user tower transitions across millions of mobile subscribers using
PySpark 4.0 transformWithState (StatefulProcessor, ValueState & EventTime Timers).
"""

import math
from typing import Iterator, Tuple

from pyspark.sql import DataFrame, Row, SparkSession, functions as F, types as T
from pyspark.sql.streaming import (
    StatefulProcessor,
    StatefulProcessorHandle,
    TimerValues,
)


# -----------------------------------------------------------------------------
# 2. Anti-Pattern
# -----------------------------------------------------------------------------
# ❌ Bad Practice: Legacy Spark 3.x flatMapGroupsWithState / mapGroupsWithState
# using untyped Pandas blobs and GroupStateTimeout.NoTimeout.
# Otherwise: No native TTL/multiple state variables, forced Arrow/Pandas
# serialization overhead per group, incompatible with structured state schema
# evolution, and unbounded RocksDB memory leaks without explicit state timeouts.
def bad_legacy_flatmap_groups(df_stream: DataFrame) -> DataFrame:
    def _legacy_fn(key, pdf_iter, state):
        # Spark 3.x legacy pattern: single untyped tuple state, manual timeout
        for pdf in pdf_iter:
            yield pdf

    return df_stream.groupBy("user_id").flatMapGroupsWithState(
        _legacy_fn,
        outputStructType=df_stream.schema,
        stateStructType=T.StructType([T.StructField("last_ts", T.LongType())]),
        outputMode="append",
        timeoutConf="NoTimeout",  # Leaks state memory permanently
    )


# -----------------------------------------------------------------------------
# 3. ANSI-safe PySpark 4.0+ Fix
# -----------------------------------------------------------------------------
# ✅ Good Practice: PySpark 4.0 transformWithState with StatefulProcessor,
# typed ValueState, and EventTime timers for deterministic state eviction.
# Note: For timeMode="ProcessingTime", use ttlDurationMs=1_800_000 instead.
STATE_SCHEMA = T.StructType([
    T.StructField("tower_id", T.StringType(), nullable=False),
    T.StructField("lat", T.DoubleType(), nullable=False),
    T.StructField("lon", T.DoubleType(), nullable=False),
    T.StructField("event_ts_ms", T.LongType(), nullable=False),
])

OUTPUT_SCHEMA = T.StructType([
    T.StructField("user_id", T.StringType(), nullable=False),
    T.StructField("tower_id", T.StringType(), nullable=False),
    T.StructField("prev_tower", T.StringType(), nullable=True),
    T.StructField("mph", T.DoubleType(), nullable=False),
    T.StructField("is_fraud_velocity", T.BooleanType(), nullable=False),
])


class TowerVelocityProcessor(StatefulProcessor):
    """PySpark 4.0 StatefulProcessor detecting impossible-travel velocities."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle
        # TTL (ttlDurationMs=...) is supported ONLY in ProcessingTime mode;
        # EventTime mode uses registerTimer() + handleExpiredTimer() below.
        self.last_tower = handle.getValueState(
            stateName="lastTower",
            schema=STATE_SCHEMA,
        )

    def handleInputRows(
        self,
        key: Tuple[str],
        rows: Iterator[Row],
        timerValues: TimerValues,
    ) -> Iterator[Row]:
        user_id = key[0]
        ordered = sorted(rows, key=lambda r: r.event_ts_ms)
        prev = self.last_tower.get()

        for row in ordered:
            mph = 0.0
            prev_tower = None
            if prev is not None and row.event_ts_ms > prev.event_ts_ms:
                prev_tower = prev.tower_id
                hours = (row.event_ts_ms - prev.event_ts_ms) / 3_600_000.0
                miles = math.sqrt(
                    ((row.lat - prev.lat) * 69.0) ** 2
                    + ((row.lon - prev.lon) * 55.0) ** 2
                )
                mph = round(miles / hours, 1)

            is_fraud = mph > 500.0
            self.last_tower.update((
                row.tower_id,
                row.lat,
                row.lon,
                row.event_ts_ms,
            ))
            prev = row
            # Register event-time inactivity timer 30m beyond latest event
            self.handle.registerTimer(row.event_ts_ms + 1_800_000)

            yield Row(
                user_id=user_id,
                tower_id=row.tower_id,
                prev_tower=prev_tower,
                mph=mph,
                is_fraud_velocity=is_fraud,
            )

    def handleExpiredTimer(
        self,
        key: Tuple[str],
        timerValues: TimerValues,
        expiredTimerInfo,
    ) -> Iterator[Row]:
        # Reclaim RocksDB key state cleanly upon session inactivity
        self.last_tower.clear()
        return iter([])


def apply_velocity_state_machine(
    df_tower_stream: DataFrame,
    watermark_delay: str = "15 minutes",
) -> DataFrame:
    """Apply PySpark 4.0 transformWithState with RocksDB state store."""
    df_prep = (
        df_tower_stream.withColumn(
            "ts", F.to_timestamp(F.col("event_time"))
        )
        .withColumn("event_ts_ms", (F.col("ts").cast("long") * F.lit(1000)))
        .withWatermark("ts", watermark_delay)
    )
    return df_prep.groupBy("user_id").transformWithState(
        statefulProcessor=TowerVelocityProcessor(),
        outputStructType=OUTPUT_SCHEMA,
        outputMode="Update",
        timeMode="EventTime",
    )


# -----------------------------------------------------------------------------
# 4. explain("formatted") / Spark UI Diagnostic Notes
# -----------------------------------------------------------------------------
# df_out.explain("formatted") shows:
#   Exchange hashpartitioning(user_id)
#   -> TransformWithStateExec (RocksDBStateStoreProvider, EventTime)
# Spark UI -> Structured Streaming tab -> State Operators:
#   - `transformWithState` uses RocksDB column families per state variable
#     (`lastTower`), keeping state off-heap and bounded by EventTime timers.
#   - Monitor `numRegisteredTimers` vs `numDeletedTimers` and RocksDB
#     `totalBytesUsed` to confirm expired keys are evicted promptly.


# -----------------------------------------------------------------------------
# 5. Production / Interview Gotchas
# -----------------------------------------------------------------------------
# 1. `transformWithState` (Spark 4.0+) requires RocksDBStateStoreProvider
#    (`spark.sql.streaming.stateStore.providerClass`); the HDFS-backed provider
#    does NOT support multiple state variables, TTLs, or column families.
# 2. Prefer EventTime timers over ProcessingTime timers when deterministic
#    checkpoint replay after outages is required.
# 3. PySpark forbids combining `ttlDurationMs` with `timeMode="EventTime"`
#    (STATEFUL_PROCESSOR_INCORRECT_TIME_MODE_TO_ASSIGN_TTL); use
#    `registerTimer` + `state.clear()` in `handleExpiredTimer()` for EventTime,
#    or `ttlDurationMs` (milliseconds) when running in `ProcessingTime` mode.


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("061_TransformWithState")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state."
            "RocksDBStateStoreProvider",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    print("TowerVelocityProcessor configured for Spark 4.0 transformWithState.")
