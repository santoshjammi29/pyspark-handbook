# Enterprise Data Engineering Production Incident Runbooks & Triage Playbooks

This guide provides deep-dive root cause analysis (RCA), diagnostic SQL/CLI queries, and surgical remediation steps for the most common mission-critical data engineering production incidents.

---

## Triage Runbook 1: Spark Driver Out-Of-Memory (`Driver OOM`)

### Symptoms
- Spark job fails with `java.lang.OutOfMemoryError: Java heap space` or `GC overhead limit exceeded` on driver node.
- Driver heartbeat lost, cluster terminates unexpectedly.
- Web UI shows driver memory flatlining at 100%.

### Root Causes
1. Unbounded actions collecting distributed data to the single driver process: `.collect()`, `.toPandas()`, `.take(1000000)`.
2. Broadcasting tables exceeding `spark.sql.autoBroadcastJoinThreshold` (e.g., driver downloads 4GB table, serializes to executors, exhausting heap).
3. Writing millions of small unpartitioned files via single-threaded driver file committers.

### Surgical Remediation
```python
# 1. Eliminate Driver Collection — write direct to storage
# Anti-pattern:
# records = df.collect()

# Production pattern:
df.write.format("delta").mode("append").save("/Volumes/prod_catalog/gold/metrics")

# 2. Tune Driver Broadcast Threshold & Memory Allocations
# In cluster config or SparkSession:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "67108864")  # 64MB limit
spark.conf.set("spark.driver.maxResultSize", "8g")
spark.conf.set("spark.driver.memory", "16g")

# 3. Use take() with bounded limits for telemetry logging
sample_records = df.limit(50).toPandas()
```

---

## Triage Runbook 2: Data Skew Elimination via Two-Phase Key Salting

### Symptoms
- Spark Stage shows 199/200 tasks finished in 30 seconds; 1 task hangs for 2 hours.
- Spark UI Task Duration histogram is severely skewed to the right.
- Single executor runs out of memory (OOM Exit 137).

### Root Causes
- Join or GroupBy on low-cardinality keys with skewed distributions (e.g., `NULL`, `UNKNOWN`, `retailer_id=99999` accounting for 80% of volume).

### Surgical Remediation
```python
from pyspark.sql import functions as F

# Two-Phase Key Salting:
# Step 1: Add salt key (0 to N-1) to skewed left table
SALT_FACTOR = 16
df_skewed_salted = df_large.withColumn("salt", F.floor(F.rand() * SALT_FACTOR))

# Step 2: Explode dimension / right lookup table with all salt values
salt_df = spark.range(SALT_FACTOR).withColumnRenamed("id", "salt")
df_dim_exploded = df_small.crossJoin(salt_df)

# Step 3: Join on original key AND salt key to distribute evenly
joined_df = df_skewed_salted.join(
    df_dim_exploded,
    on=["join_key", "salt"],
    how="inner"
).drop("salt")
```

---

## Triage Runbook 3: Eliminating Tungsten Disk Spill

### Symptoms
- Spark UI Stage details show **Spill (Memory): 120 GB, Spill (Disk): 45 GB**.
- Severe performance degradation due to executor local disk I/O serialization.

### Root Causes
- Sizing of `spark.sql.shuffle.partitions` (default 200) is too low for input volume.
- Memory allocation fraction (`spark.memory.fraction`, default 0.6) saturated by aggressive sorting or Window aggregations.

### Surgical Remediation
```python
# 1. Calculate Target Shuffle Partition Count
# Target partition size = 128 MB to 200 MB
# Shuffle Partition Count = Total Shuffle Stage Input Size (GB) / 0.15 GB
# Example: 150 GB shuffle input / 0.15 GB = 1000 partitions
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# 2. Enable Adaptive Query Execution (AQE) Partition Coalescing
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728") # 128MB

# 3. Off-Heap Memory Allocation
# spark.memory.offHeap.enabled = true
# spark.memory.offHeap.size = 4g
```

---

## Triage Runbook 4: Delta Lake Concurrent Append & Metadata Conflicts

### Symptoms
- Pipeline fails with `ConcurrentAppendException` or `ConcurrentTransactionException`.
- Multiple concurrent pipelines writing to same Delta table partition simultaneously.

### Root Causes
- Concurrent writers mutating the same partition or metadata files without row-level concurrency isolation.

### Surgical Remediation
1. **Enable Deletion Vectors**: Allows concurrent updates/deletes to mark rows deleted without rewriting entire Parquet files.
   ```sql
   ALTER TABLE catalog.schema.target_table 
   SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
   ```
2. **Migrate from Hive Partitioning to Liquid Clustering**:
   ```sql
   ALTER TABLE catalog.schema.target_table 
   CLUSTER BY (customer_id, transaction_date);
   ```
3. **Partition-Pruned Conditional Merges**:
   ```sql
   MERGE INTO target t USING source s
   ON t.date_key = s.date_key AND t.id = s.id
   WHEN MATCHED THEN UPDATE SET *
   WHEN NOT MATCHED THEN INSERT *;
   ```

---

## Triage Runbook 5: Cross-System Retry Storms (ADF Activity × Databricks Task)

### Symptoms
- Transient network blip or lock causes 16 duplicate job executions.
- Cluster pool exhausted; downstream tables contain duplicate records or deadlocks.

### Root Causes
- ADF Activity `retryCount: 3` × Databricks Task `max_retries: 3` = $(1 + 3) \times (1 + 3) = 16$ invocations.

### Surgical Remediation
1. **Centralize Retry Ownership**: Set Databricks task `max_retries: 0`. Let ADF orchestrate backoff.
2. **Pass Pipeline RunId & Enforce Idempotency**:
   ```python
   # In Databricks Notebook:
   dbutils.widgets.text("pipeline_run_id", "")
   run_id = dbutils.widgets.get("pipeline_run_id")

   # Check if this batch was already committed
   existing_run = spark.sql(f"SELECT 1 FROM audit.pipeline_runs WHERE run_id = '{run_id}'").count()
   if existing_run > 0:
       print(f"Run {run_id} already committed. Skipping idempotent re-execution.")
       dbutils.notebook.exit("ALREADY_COMMITTED")
   ```

---

## Triage Runbook 6: Structured Streaming Checkpoint Invalidation

### Symptoms
- Pipeline fails after upstream schema change. An engineer deletes the `_checkpoint/` directory.
- Billions of duplicate events processed; downstream Gold tables corrupted.

### Root Causes
- Deleting checkpoints erases the persistent Kafka/Auto Loader offset progress record.

### Surgical Remediation
1. **Never Delete Checkpoint Directories**.
2. **Inspect Commit vs Offset JSON**:
   Compare files in `_checkpoint/offsets/` and `_checkpoint/commits/`. Find the highest uncommitted batch number and replay cleanly.
3. **Enable Schema Evolution on Auto Loader**:
   ```python
   df = (
       spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", "/Volumes/prod/meta/schema_tracking")
       .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
       .load("/Volumes/prod/raw/events")
   )
   ```
4. **Idempotent Streaming Output**:
   Always write streaming sinks using `.foreachBatch()` with `MERGE INTO` or deterministic deduplication (`dropDuplicates` on event ID and timestamp).

---

## Triage Runbook 7: ADF Watermark Desync & Transaction Boundaries

### Symptoms
- Downstream dashboard reports missing 24 hours of transactions after a failed pipeline restart.
- Watermark in Control Table was incremented even though Databricks job failed midway.

### Root Causes
- Updating the Control Table high watermark BEFORE or independent of verifying that the target Delta commit completed.

### Surgical Remediation
1. **Two-Phase Watermark Commit Protocol**:
   - Step 1: Extract candidate batch: `WHERE update_ts > last_watermark AND update_ts <= new_candidate_watermark`.
   - Step 2: Write batch to Delta with transaction log commit.
   - Step 3: Databricks returns exact committed max timestamp to ADF pipeline return value.
   - Step 4: ADF Control Table updates `last_watermark` ONLY on explicit Success status.
2. **Automated Audit Check**:
   ```sql
   SELECT 
       c.table_name,
       c.last_watermark,
       MAX(t.event_timestamp) as actual_target_max,
       DATEDIFF(hour, c.last_watermark, MAX(t.event_timestamp)) as lag_hours
   FROM control.watermark_tracking c
   JOIN gold.fact_transactions t ON c.table_name = 'fact_transactions'
   GROUP BY c.table_name, c.last_watermark
   HAVING c.last_watermark > MAX(t.event_timestamp);
   -- If any rows returned, watermark is desynchronized!
   ```

---

## Triage Runbook 8: Kafka Consumer Group Rebalance Storm

### Symptoms
- Real-time Spark Streaming or Flink consumer stalls; processing throughput drops to 0.
- Kafka logs show continuous `PreparingRebalance` and `Member has joined the group` messages.

### Root Causes
- Microbatch processing duration exceeds `max.poll.interval.ms` (default 300,000 ms / 5 min).
- Kafka broker assumes consumer died, kicks it out, triggers partition reassignment across cluster.

### Surgical Remediation
```python
# 1. Cap maximum records polled per microbatch
spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "telemetry_topic") \
    .option("maxOffsetsPerTrigger", "50000") \
    .option("kafka.max.poll.interval.ms", "600000") \
    .load()
```
