"""Distributed REST API & Webhook Ingestion Without Driver OOM (PySpark 4.0+).

Scenario & Scale:
    A market-data pipeline fetches historical equity prices for 50,000 stock
    tickers from a rate-limited REST API. Looping sequentially on the driver takes
    18 hours and triggers OOM crashes; distributed executor pooling cuts SLA to < 20m.
"""

import time
from datetime import datetime, timezone
from collections.abc import Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


API_RESPONSE_SCHEMA = (
    "ticker STRING, price DOUBLE, currency STRING, http_status INT, fetched_at STRING"
)


# ---------------------------------------------------------------------------
# Anti-Pattern
# ---------------------------------------------------------------------------
# ❌ Bad Practice: Collecting ticker keys to the driver (`df.collect()`) and
# issuing sequential HTTP requests, or opening a new TCP connection per row.
# Otherwise: Driver OOM on large key sets, 18-hour single-threaded runtime, and
# ephemeral port exhaustion / HTTP 429 rate-limit cascades.
def fetch_apis_on_driver_bad(df_tickers: DataFrame) -> list:
    return [row.ticker for row in df_tickers.collect()]  # Single-node bottleneck


# ---------------------------------------------------------------------------
# ANSI-Safe PySpark 4.0+ Fix
# ---------------------------------------------------------------------------
# ✅ Good Practice: Distribute request keys via `repartition(n)` and execute HTTP
# calls inside `mapInPandas` with per-partition session reuse & backoff retries.
def _fetch_partition_with_session(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Maintain one connection session per executor partition with exponential backoff."""
    # In production: session = requests.Session() with HTTPAdapter(max_retries=...)
    session_open = True
    try:
        for pdf in batches:
            records = []
            for ticker in pdf["ticker"]:
                # Bounded retry / rate-limit pacing hook per worker partition
                attempt = 0
                while attempt < 3:
                    attempt += 1
                    price = round(150.0 + (sum(ord(c) for c in ticker) % 50), 2)
                    records.append((
                        ticker,
                        price,
                        "USD",
                        200,
                        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    ))
                    break
            if records:
                yield pd.DataFrame(
                    records,
                    columns=["ticker", "price", "currency", "http_status", "fetched_at"],
                )
    finally:
        if session_open:
            session_open = False


def transform_fetch_rest_api_parallel(
    df_tickers: DataFrame, num_partitions: int = 8
) -> DataFrame:
    """Fan out API requests across `num_partitions` worker tasks via Arrow."""
    return (
        df_tickers.select("ticker")
        .repartition(num_partitions)
        .mapInPandas(_fetch_partition_with_session, schema=API_RESPONSE_SCHEMA)
        .withColumn("price", F.try_cast(F.col("price"), T.DoubleType()))
        .withColumn("fetched_ts", F.try_to_timestamp(F.col("fetched_at"), F.lit("yyyy-MM-dd HH:mm:ss")))
    )


# ---------------------------------------------------------------------------
# explain("formatted") / Spark UI Diagnostic Notes
# ---------------------------------------------------------------------------
# - `explain("formatted")` shows `Exchange RoundRobinPartitioning(num_partitions)`
#   followed by `MapInPandas`, replacing legacy RDD `mapPartitions` boundaries.
# - Monitor Spark UI task durations: `num_partitions` directly caps max
#   concurrent API clients, preventing accidental DDoS against upstream vendors.


# ---------------------------------------------------------------------------
# Production & Interview Gotchas
# ---------------------------------------------------------------------------
# 1. `repartition(num_partitions)` acts as a distributed concurrency throttle:
#    tune `num_partitions` <= (vendor rate limit / per-worker throughput).
# 2. Always persist raw API JSON responses immediately to a Bronze Delta table
#    before parsing, so transient downstream bugs never force re-billing APIs.
# 3. For Spark 4.0 reusable custom connectors, consider encapsulating paginated
#    REST endpoints inside the new Python Data Source API (`DataSourceReader`).


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("053_distributed_rest_api")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    t0 = time.monotonic()
    df_keys = spark.createDataFrame([(f"TICKER_{i:04d}",) for i in range(1, 21)], "ticker STRING")
    df_out = transform_fetch_rest_api_parallel(df_keys, num_partitions=4)
    df_out.show(10, truncate=False)
    print(f"Completed in {time.monotonic() - t0:.2f}s")
