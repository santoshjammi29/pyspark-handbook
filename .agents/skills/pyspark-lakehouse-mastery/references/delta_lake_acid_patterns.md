# Delta Lake 3.2+ Architecture & ACID Operations Reference

This reference covers physical storage mechanics, transaction log protocols, multi-condition merges, time travel, deletion vectors, and Liquid Clustering for enterprise lakehouses.

---

## 1. Delta Transaction Log (`_delta_log/`) Internals

Every Delta table is a directory of immutable Apache Parquet data files accompanied by an ACID write-ahead log in `_delta_log/`:

```text
/lakehouse/silver/orders/
├── _delta_log/
│   ├── 00000000000000000000.json      # Commit 0: Schema definition, table creation
│   ├── 00000000000000000001.json      # Commit 1: Append 5 Parquet files
│   ├── 00000000000000000002.json      # Commit 2: Merge / update tombstoning 1 file, adding 1 file
│   ├── ...
│   └── 00000000000000000010.checkpoint.parquet # State checkpoint every 10 commits
├── part-00000-a1b2c3d4.c000.snappy.parquet
└── part-00001-e5f6g7h8.c000.snappy.parquet
```

### Protocol Mechanics:
1. **Optimistic Concurrency Control (OCC)**: Writers attempt commits; if another writer committed overlapping files first, the transaction retries automatically based on conflict isolation level (WriteSerializable vs Serializable).
2. **Checkpointing**: Every 10 commits, Spark writes a `.checkpoint.parquet` file consolidating the table state, avoiding replaying thousands of JSON log files.
3. **Snapshot Isolation**: Readers query a specific commit version snapshot without locking writers.

---

## 2. Multi-Condition Delta `MERGE INTO` (Upserts, Deletes & Conditional Updates)

Delta Lake supports complex CDC reconciliation in a single atomic transaction:

```python
from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "silver.orders")

(
    target_table.alias("target")
    .merge(
        source=updates_df.alias("source"),
        condition="target.order_id = source.order_id"
    )
    # Case 1: Hard delete discontinued or cancelled orders
    .whenMatchedDelete(
        condition="source.status = 'CANCELLED' AND target.created_date < DATE_SUB(CURRENT_DATE(), 30)"
    )
    # Case 2: Update existing orders only if newer timestamp
    .whenMatchedUpdate(
        condition="source.updated_at > target.updated_at",
        set={
            "customer_id": "source.customer_id",
            "order_amount": "source.order_amount",
            "order_status": "source.order_status",
            "updated_at": "source.updated_at"
        }
    )
    # Case 3: Insert new arriving records
    .whenNotMatchedInsert(
        values={
            "order_id": "source.order_id",
            "customer_id": "source.customer_id",
            "order_amount": "source.order_amount",
            "order_status": "source.order_status",
            "created_date": "source.created_date",
            "updated_at": "source.updated_at"
        }
    )
    .execute()
)
```

---

## 3. Liquid Clustering (`CLUSTER BY`) vs. Z-Ordering & Partitioning

| Metric / Dimension | Legacy Hive Partitioning | Z-Ordering (`OPTIMIZE ... ZORDER BY`) | Liquid Clustering (`CLUSTER BY`) |
| :--- | :--- | :--- | :--- |
| **Physical Layout** | Rigid directories (`date=2026/`) | 1D Hilbert curve across Parquet files | Dynamic multidimensional clustering curve |
| **Small File Tax** | High on high-cardinality keys | Moderate; requires manual rewrite | None; handled dynamically |
| **Key Mutability** | Impossible without full table rewrite | Must re-run Z-Order every batch | Clustering keys can change instantly with `ALTER TABLE` |
| **Concurrency** | Frequent partition lock conflicts | High lock contention during rewrite | Row-level deletion vector compatibility |

### Liquid Clustering Syntax:
```sql
-- Create table with Liquid Clustering
CREATE TABLE silver.iot_readings (
    device_id STRING,
    event_timestamp TIMESTAMP,
    sensor_type STRING,
    metric_value DOUBLE
) USING DELTA
CLUSTER BY (device_id, event_timestamp);

-- Alter clustering keys dynamically without data rewrites
ALTER TABLE silver.iot_readings CLUSTER BY (device_id, sensor_type);

-- Trigger incremental clustering
OPTIMIZE silver.iot_readings;
```

---

## 4. Deletion Vectors & High-Throughput Point Mutations

Deletion vectors eliminate the rewrite penalty of Parquet files:
- When a row is deleted or updated, instead of rewriting the entire 500MB Parquet file, Delta writes a tiny bitmap vector recording deleted row indices.
- **Enable on Delta Table**:
```sql
ALTER TABLE silver.orders SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = true,
    'delta.columnMapping.mode' = 'name'
);
```

---

## 5. Time Travel & Snapshot Rollback

```python
# Query historical version by Version Number
df_v5 = spark.read.format("delta").option("versionAsOf", 5).load("/path/to/delta")

# Query historical version by Timestamp
df_ts = spark.read.format("delta").option("timestampAsOf", "2026-11-01 12:00:00").load("/path/to/delta")

# Rollback entire table to prior clean version
spark.sql("RESTORE TABLE silver.orders TO VERSION AS OF 12;")
```

---

## 6. Safe Compaction & Tombstone Management (`OPTIMIZE` & `VACUUM`)

```sql
-- Compact small files into ~1GB optimal files
OPTIMIZE silver.orders;

-- Purge deleted files older than retention threshold (default: 168 hours / 7 days)
VACUUM silver.orders RETAIN 168 HOURS;

-- Dry run check before running VACUUM
VACUUM silver.orders DRY RUN;
```
