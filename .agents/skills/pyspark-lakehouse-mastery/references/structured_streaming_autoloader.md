# PySpark Structured Streaming & Auto Loader Reference

This guide covers production real-time streaming architectures, event-time watermarking, stateful stream-stream joins, Auto Loader (`cloudFiles`), and Dead Letter Queue (DLQ) error isolation.

---

## 1. Databricks Auto Loader (`cloudFiles`) Ingestion Engine

Auto Loader provides incremental, scalable ingestion of millions of landing files from cloud object storage (ADLS Gen2 / AWS S3 / GCS) into Delta Lake:

```python
from pyspark.sql import functions as F

# Auto Loader Ingestion with Schema Evolution & Rescued Data
stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/prod_catalog/meta/schemas/orders_landing")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    # File discovery mode: "directoryListing" or "fileNotification" (SNS/SQS or Event Grid)
    .option("cloudFiles.useNotifications", "true")
    .load("/Volumes/prod_catalog/raw_landing/orders/")
)

# Filter corrupt data into Dead Letter Queue (DLQ)
clean_df = stream_df.filter(F.col("_rescued_data").isNull())
dlq_df = stream_df.filter(F.col("_rescued_data").isNotNull())
```

---

## 2. Event-Time Watermarking & Deduplication

In distributed networks, events arrive out-of-order. A watermark defines how long Spark keeps state before dropping late data:

```python
from pyspark.sql import functions as F

# Define watermark and deduplicate over composite key
deduped_stream = (
    stream_df
    # Watermark: allow up to 15 minutes of late arrival
    .withWatermark("event_timestamp", "15 minutes")
    # Stateful deduplication: drops duplicate transactions within watermark boundary
    .dropDuplicates(["transaction_id", "event_timestamp"])
)
```

---

## 3. Stateful Stream-Stream Joins

Joining two unbounded streams (e.g., Orders and Payments) requires watermarks on both sides and an event-time range constraint:

```python
# Stream 1: Orders
orders = orders_stream.withWatermark("order_time", "1 hour")

# Stream 2: Payments
payments = payments_stream.withWatermark("payment_time", "2 hours")

# Symmetric Inner Join with Time Boundary
joined_stream = orders.join(
    payments,
    expr="""
        orders.order_id = payments.order_id AND
        payments.payment_time >= orders.order_time AND
        payments.payment_time <= orders.order_time + interval 1 hour
    """,
    how="inner"
)
```

---

## 4. Trigger Strategies

| Trigger Mode | Syntax | When to Use |
| :--- | :--- | :--- |
| **AvailableNow** | `.trigger(availableNow=True)` | **Cost-effective incremental batching**: spins up cluster, processes all pending files in microbatches, commits state, and safely shuts down cluster. |
| **MicroBatch Processing** | `.trigger(processingTime="30 seconds")` | Standard near-real-time streaming with bounded latency. |
| **Continuous Processing** | `.trigger(continuous="1 second")` | Low latency (<10ms) without microbatch overhead (limited operator support). |
| **Once (Legacy)** | `.trigger(once=True)` | Deprecated in Spark 3.3+ in favor of `availableNow=True`. |

---

## 5. Streaming Output with `foreachBatch` & Idempotent Delta Upsert

```python
def upsert_to_delta(microbatch_df, batch_id):
    """
    Idempotent microbatch sink to prevent duplicate rows on cluster restarts.
    """
    if microbatch_df.isEmpty():
        return
        
    from delta.tables import DeltaTable
    target = DeltaTable.forName(spark, "silver.orders")
    
    (
        target.alias("t")
        .merge(
            microbatch_df.alias("s"),
            "t.order_id = s.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

query = (
    deduped_stream.writeStream
    .format("delta")
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/Volumes/prod_catalog/meta/checkpoints/orders_silver")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
```
