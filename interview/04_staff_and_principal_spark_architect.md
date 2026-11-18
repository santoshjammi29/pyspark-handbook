# 🎓 PySpark Interview Guide — Tier 4: Staff & Principal Spark Architect (System Design & Governance)

> **Focus Areas**: Cluster Sizing & Memory Math, Multi-Tenant Fair Scheduler Queues, Cross-Region Geo-DR, Zero-Copy Lakehouse Federation, FinOps Right-Sizing, Unity Catalog Governance.
> **Target Audience**: Principal Data Architects, Staff Infrastructure Engineers, Platform Leads

---

### Q4.1: GDPR Compliance & Dynamic Data Masking (Data Governance)

**Question & Concept Overview:**  
Enforcing security configurations dynamically on PII columns and complying with the GDPR 'Right to be Forgotten' by purging targeted personal details across historical snapshots without compromising table ACID logs.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    from pyspark.sql.functions import when, expr, col, sha2, lit
    
    # Dynamic column masking for IBAN
    # is_member evaluates the active caller's Microsoft Entra ID groups in Unity Catalog
    secured_customer_df = customer_raw_df.withColumn(
        "iban_secured",
        when(expr("is_member('Compliance_Auditors')"), col("iban"))
        .when(expr("is_member('Standard_Developers')"), sha2(col("iban"), 256))
        .otherwise(lit("ACCESS_DENIED"))
    )
    
    # 2. Right to be Forgotten (GDPR Article 17): Purge records based on customer requests
    # Perform deletion in the Delta table and clean up history using VACUUM
    spark.sql("DELETE FROM eu_bfsi_prod.silver.customers WHERE customer_id = 'CUST-EU-8809'")
    
    # Run VACUUM to permanently purge deleted data files from physical storage
    # Set retention to 0 hours for immediate physical delete (requires bypass flag)
    spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
    spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
    spark.sql("VACUUM eu_bfsi_prod.silver.customers RETAIN 0 HOURS")
except Exception as e:
    logger.error(f"Pipeline stage failed in GDPR Compliance & Dynamic Data Masking: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Masking customer IBAN fields dynamically based on user identity roles, and executing audit-safe customer data purges to satisfy EU GDPR requirements.

**Architectural Invariants & Gotchas:**
- Dynamic data masking leverages Unity Catalog integration to evaluate groups dynamically (is_member).
- Standard SQL DELETE operations in Delta Lake rewrite file segments, marking old files as tombstoned.
- VACUUM permanently deletes files that have been logically deleted longer than the retention period.
- Always restrict the use of retentionDurationCheck.enabled = false; running it incorrectly can corrupt active transactions.

---

### Q4.2: Idempotent Medallion Pipeline Orchestration (Architecture Design)

**Question & Concept Overview:**  
Designing end-to-end batch and micro-batch data pipelines that guarantee exactly-once delivery, avoiding data duplication during job reruns or mid-execution failures.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    from pyspark.sql import functions as F
    
    # Batch Ingestion pattern with explicit batch tracking
    def ingest_daily_transactions(batch_date: str, source_path: str, target_table: str):
        # 1. Load batch files for the specific execution date
        batch_df = spark.read.format("delta").load(source_path) \
            .filter(F.col("txn_date") == batch_date)
        
        # 2. Add pipeline metadata for audit tracing
        enriched_batch_df = batch_df \
            .withColumn("_pipeline_batch_id", F.lit(batch_date)) \
            .withColumn("_ingested_at", F.current_timestamp())
    
        # 3. Idempotently write to target table using INSERT OVERWRITE on the target partition
        # Rerunning this function for the same date will safely overwrite without doubling rows
        enriched_batch_df.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"txn_date = '{batch_date}'") \
            .save(target_table)
except Exception as e:
    logger.error(f"Pipeline stage failed in Idempotent Medallion Pipeline Orchestration: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Orchestrating daily transaction ingestion schedules from payment landing zones to the Silver layer that are resilient to pipeline failures.

**Architectural Invariants & Gotchas:**
- Idempotency ensures that executing a pipeline multiple times with the same input produces the exact same output.
- Using option('replaceWhere', ...) ensures Spark only overwrites files within matching partitions.
- Avoid append mode without duplicate validation since running twice will create duplicates.
- For streaming workloads, exactly-once is enforced using checkpoints and state databases (RocksDB).

---

### Q4.3: FinOps: Cost-Optimized Cluster Configurations (Cloud FinOps)

**Question & Concept Overview:**  
Structuring resource policies, configuring instance pools, setting scaling thresholds, and tweaking execution profiles to reduce cloud waste during massive data transformation tasks.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # Configure Spark execution settings to optimize cloud cluster costs (FinOps)
    spark.conf.set("spark.sql.adaptive.enabled", "true")                  # Auto optimizes execution paths
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")         # Mitigates data skew tasks dynamically
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")# Automatically merges small output files
    
    # Specify cluster autoscaling logic programmatically for Synapse/Fabric workloads
    # Example: Limit max drivers, configure dynamic allocation based on thread limits
    spark.conf.set("spark.dynamicAllocation.enabled", "true")
    spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
    spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
    spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
except Exception as e:
    logger.error(f"Pipeline stage failed in FinOps: Cost-Optimized Cluster Configurations: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Configuring serverless autoscaling rules on monthly payment reconciliation jobs to balance performance with strict budget limits.

**Architectural Invariants & Gotchas:**
- Set aggressive cluster auto-termination limits (e.g. 15-20 minutes of inactivity) on interactive workspaces.
- Use spot instance configurations for worker nodes to save up to 80% of virtual machine computing costs.
- Enable Photon Engine in Databricks clusters for vectorized execution on large query volumes.
- Set CPU resources dynamically with dynamicAllocation to release workers back to the pool during idle times.

---

### Q4.4: Enterprise Metadata Catalog Integration (Data Integration)

**Question & Concept Overview:**  
Designing Unified namespaces across business domains. Leverages Microsoft Fabric OneLake shortcuts and Unity Catalog schemas to bridge multi-cloud workloads without replicating files.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # 1. Unity Catalog three-level namespace execution
    # Querying directly across catalog structures (dev to prod)
    gold_report_df = spark.read.table("eu_bfsi_prod.gold.aml_alerts") \
        .join(spark.read.table("eu_bfsi_prod.silver.customers"), on="customer_id")
    
    # 2. Accessing cross-cloud datasets using Microsoft Fabric OneLake Shortcuts
    # OneLake presents a single ADLS repository across workspaces
    fabric_shortcut_path = "abfss://finance_workspace@onelake.dfs.fabric.microsoft.com/invoices.Lakehouse/Tables/raw_invoices"
    external_invoices_df = spark.read.format("delta").load(fabric_shortcut_path)
    
    # 3. Create a shortcut reference catalog pointer
    spark.sql(f"CREATE TABLE IF NOT EXISTS eu_bfsi_prod.bronze.shortcut_invoices USING DELTA LOCATION '{fabric_shortcut_path}'")
except Exception as e:
    logger.error(f"Pipeline stage failed in Enterprise Metadata Catalog Integration: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Cross-subsidiary query resolution between UK and German banking divisions using unified catalog schemas.

**Architectural Invariants & Gotchas:**
- Unity Catalog enforces a three-level namespace: catalog_name.schema_name.table_name.
- Fabric shortcuts let you link external storage accounts (S3, ADLS Gen2) into OneLake as virtual tables.
- Using metadata shortcuts prevents duplication of raw files, reducing data egress charges and sync delays.
- Configure row-level policies directly inside Unity Catalog to restrict data visibility based on subsidiary locations.

---

### Q4.5: ACID Transactions and Concurrent Writes (Advanced Delta Lake)

**Question & Concept Overview:**  
Managing concurrent transactions and resolving conflicts. Delta Lake uses Optimistic Concurrency Control (OCC) to handle parallel write operations, failing transactions only if they try to modify the same partitions.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # Scenario: Simultaneously streaming alerts into gold table and running background maintenance delete
    from delta.tables import DeltaTable
    
    # Background process deleting expired transactions
    def run_purging_operation():
        try:
            # Perform logical delete on target
            spark.sql("DELETE FROM eu_bfsi_prod.gold.compliance_ledger WHERE txn_date < '2019-01-01'")
        except Exception as e:
            print(f"Conflict detected: {e}")
            # OCC conflict throws 'ConcurrentAppendException' or 'WriteConflictException'
    
    # Stream write configuration with concurrency limits
    # Enable schema evolution and configure write limits
    spark.conf.set("spark.databricks.delta.write.allowConcurrentWrites", "true")
    spark.conf.set("spark.databricks.delta.properties.defaults.isolationLevel", "WriteSerializable")
except Exception as e:
    logger.error(f"Pipeline stage failed in ACID Transactions and Concurrent Writes: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Orchestrating concurrent processes: real-time alerts are appended while GDPR maintenance scripts delete historical rows in the same table.

**Architectural Invariants & Gotchas:**
- Optimistic Concurrency Control assumes writes rarely conflict; it validates before commit and retries if a conflict occurs.
- Isolation levels: 'Serializable' guarantees full transaction isolation; 'WriteSerializable' allows parallel updates to different partitions.
- Enable Change Data Feed (CDF) to avoid write conflicts by only reading changes instead of full tables.
- Design target partitions carefully; writing to different date-partitions (e.g. 2026-05-31 vs 2026-05-30) prevents concurrent conflicts.

---

### Q4.6: Multi-tenant Architecture with Catalog Isolation (Data Architecture)

**Question & Concept Overview:**  
Structuring workspaces and schemas to enforce strict data separation across EU subsidiaries. Uses catalog-level governance to restrict access boundaries and comply with national regulations.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # Create tenant catalogs with isolated storage locations (managed locations)
    spark.sql("""
        CREATE CATALOG IF NOT EXISTS tenant_france
        MANAGED LOCATION 'abfss://france@euadlsfr.dfs.core.windows.net/managed/'
        COMMENT 'Isolated catalog for French Banking Subsidiary'
    """)
    
    spark.sql("""
        CREATE CATALOG IF NOT EXISTS tenant_germany
        MANAGED LOCATION 'abfss://germany@euadlsde.dfs.core.windows.net/managed/'
        COMMENT 'Isolated catalog for German Banking Subsidiary'
    """)
    
    # Grant access permissions strictly to regional groups
    spark.sql("GRANT USAGE, CREATE SCHEMA ON CATALOG tenant_france TO `group-fr-data-engineers`")
    spark.sql("GRANT USAGE, CREATE SCHEMA ON CATALOG tenant_germany TO `group-de-data-engineers`")
    
    # Active tenant selection logic
    spark.sql("USE CATALOG tenant_france")
except Exception as e:
    logger.error(f"Pipeline stage failed in Multi-tenant Architecture with Catalog Isolation: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Designing compliance environments to separate French and German retail banking customer data, preventing cross-border leaks.

**Architectural Invariants & Gotchas:**
- Managed locations ensure that data created inside the catalog is automatically stored in tenant-owned cloud buckets.
- Catalogs provide the strongest governance boundary in Unity Catalog, isolating data, storage, and access permissions.
- Multi-tenant architectures should use separate Entra ID groups to manage regional roles.
- Cross-catalog joins are supported for administrative accounts with read permissions on both catalogs.

---

### Q4.7: Medallion Silver-to-Gold Schema Evolution (Schema Management)

**Question & Concept Overview:**  
Managing schema changes in medallion pipelines. Demonstrates how to support schema evolution, prevent pipeline failures from schema drift, and route invalid rows to quarantine.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # Load raw ingestion schema with drift allowance enabled
    raw_ingest_df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", "abfss://meta@euadlsprod.dfs.core.windows.net/schemas/drift_txns") \
        .load("abfss://landing@euadlsprod.dfs.core.windows.net/incoming_txns")
    
    # Configure target write with mergeSchema enabled to automatically adapt to column additions
    drift_query = raw_ingest_df.writeStream.format("delta") \
        .outputMode("append") \
        .option("mergeSchema", "true") \
        .option("checkpointLocation", "abfss://meta@euadlsprod.dfs.core.windows.net/checkpoints/drift_txns") \
        .start("abfss://silver@euadlsprod.dfs.core.windows.net/drift_silver_transactions")
except Exception as e:
    logger.error(f"Pipeline stage failed in Medallion Silver-to-Gold Schema Evolution: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Designing a robust payment gateway ingestion pipeline that automatically adds new compliance fields to Silver tables without manual intervention.

**Architectural Invariants & Gotchas:**
- Schema Evolution allows Spark to adapt to new columns, automatically merging them into the Delta table schema.
- Schema Enforcement prevents writes from executing if they contain columns not matching the target table, unless mergeSchema is explicitly enabled.
- Use cloudFiles.inferColumnTypes carefully, as type changes (e.g. integer to string) can trigger write failures.
- Quarantine pattern: route rows containing unexpected fields to a separate dead-letter-table for manual review.

---

### Q4.8: Spark UI Analysis & Query Plan Debugging (Debugging & Performance)

**Question & Concept Overview:**  
Reading Spark execution query plans and diagnosing performance bottlenecks. Explains how to identify physical plan operators, find data spills, and check task partition details.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # Load target tables for performance diagnosis
    txns = spark.read.table("eu_bfsi_prod.silver.transactions")
    customers = spark.read.table("eu_bfsi_prod.silver.customers")
    
    # Combine and aggregate to analyze query plans
    query_df = txns.join(customers, on="customer_id", how="inner") \
        .groupBy("customers.country_code").count()
    
    # 1. Print formatted execution query plan to console
    # Displays: Parsed Logical Plan -> Analyzed Logical Plan -> Optimized Logical Plan -> Physical Plan
    query_df.explain(extended=True)
    
    # 2. Force explain output with cost and code generation details
    query_df.explain(mode="formatted")
except Exception as e:
    logger.error(f"Pipeline stage failed in Spark UI Analysis & Query Plan Debugging: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Diagnosing a slow monthly aggregation query by reviewing its physical plan to locate disk spill bottlenecks and optimize shuffle partitioning.

**Architectural Invariants & Gotchas:**
- Physical plans show operators like SortMergeJoin, HashAggregate, and FileScan, exposing the query's execution path.
- Look for '*' in physical plan operators, which denotes codegen support (Whole-Stage Code Generation).
- Disk Spill occurs when partition data exceeds the JVM memory limit, forcing Spark to spill data to local executor storage.
- Check the Spark UI Storage tab to verify caching efficiency, and the Executor tab to identify skewed tasks.

---

### Q4.9: Distributed Join Salting Pattern for Skewed Keys (Performance Tuning & Optimization)

**Question & Concept Overview:**  
Appends random numerical salt keys to skewed DataFrames to eliminate straggler tasks and balance partition distribution across executors.

**Production Implementation:**
```python
from pyspark.sql import functions as F

# Assume df_large is heavily skewed on 'country_code' (e.g. 80% 'US')
SALT_FACTOR = 4

# Step 1: Append random salt [0..3] to skewed table
df_large_salted = df_large.withColumn(
    "salt",
    F.concat(F.col("country_code"), F.lit("_"), (F.rand() * SALT_FACTOR).cast("int"))
)

# Step 2: Explode the lookup table across matching salt range [0..3]
salt_df = spark.range(SALT_FACTOR).withColumnRenamed("id", "salt_id")
df_lookup_salted = df_lookup.crossJoin(salt_df) \
    .withColumn("salt", F.concat(F.col("country_code"), F.lit("_"), F.col("salt_id")))

# Step 3: Execute join on balanced composite salt key
df_joined = df_large_salted.join(df_lookup_salted, on="salt", how="inner") \
    .drop("salt", "salt_id")
```

**Enterprise Use Case:**  
High-volume joins against tables with severe key skew (e.g., default IDs, popular countries, anonymous users).

**Architectural Invariants & Gotchas:**
- Eliminates the classic '199 tasks finish in seconds, 1 task hangs for hours' bottleneck in large shuffles.
- Spark 3.x+ Adaptive Query Execution (AQE) handles moderate skew automatically via spark.sql.adaptive.skewJoin.enabled.

---

### Q4.10: Delta Lake SCD Type 2 Dimension Upsert via MERGE INTO (Delta Lake & Lakehouse Storage)

**Question & Concept Overview:**  
Performs an atomic SCD Type 2 dimension merge, expiring outdated records and inserting active versions with effective dates in a single transaction.

**Production Implementation:**
```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_table = DeltaTable.forName(spark, "dim_customer")

# Prepare staged updates with dummy null keys to trigger both update & insert
staged_updates = df_updates.select(
    F.col("customer_id").alias("merge_key"),
    F.col("*")
).unionByName(
    df_updates.select(F.lit(None).alias("merge_key"), F.col("*"))
)

# Atomic Merge execution
target_table.alias("t").merge(
    source=staged_updates.alias("s"),
    condition="t.customer_id = s.merge_key AND t.is_current = true"
).whenMatchedUpdate(
    condition="t.city <> s.city OR t.segment <> s.segment",
    set={"is_current": "false", "end_date": "current_timestamp()"}
).whenNotMatchedInsert(
    values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "city": "s.city",
        "segment": "s.segment",
        "is_current": "true",
        "start_date": "current_timestamp()",
        "end_date": "null"
    }
).execute()
```

**Enterprise Use Case:**  
Historical dimension tracking in enterprise data warehouses and lakehouses.

**Architectural Invariants & Gotchas:**
- The union with a null merge_key forces Spark to match active records for expiration while also triggering the insert branch for new records.
- Guarantees ACID transactional consistency with zero duplicate active records.

---

### Q4.11: Two-Phase Key Salting for Severe Join Skew Remediation (Performance Tuning & Optimization)

**Question & Concept Overview:**  
Resolving catastrophic join skew where a single key (e.g., country_code = 'US' or store_id = 'ONLINE') contains 80%+ of total records, stalling worker nodes on straggler tasks. Uses randomized two-phase salting.

**Production Implementation:**
```python
from pyspark.sql import functions as F

SALT_FACTOR = 16  # Distribute skewed key across 16 parallel tasks

fact_transactions = spark.table("prod_catalog.silver.transactions")
dim_stores = spark.table("prod_catalog.silver.dim_stores")

# Step 1: Add randomized salt [0..15] to large fact table
salted_fact = fact_transactions.withColumn(
    "salt_id",
    F.floor(F.rand() * SALT_FACTOR)
).withColumn(
    "salted_join_key",
    F.concat(F.col("store_id"), F.lit("_"), F.col("salt_id"))
)

# Step 2: Replicate dimension table records across all 16 salt IDs [0..15]
salt_table = spark.range(0, SALT_FACTOR).withColumnRenamed("id", "salt_id")
replicated_dim = dim_stores.crossJoin(salt_table).withColumn(
    "salted_join_key",
    F.concat(F.col("store_id"), F.lit("_"), F.col("salt_id"))
)

# Step 3: Perform balanced inner join on the salted key
balanced_joined_df = (
    salted_fact.join(replicated_dim, on="salted_join_key", how="inner")
    .drop("salted_join_key", "salt_id")
)

# Step 4: Validate execution plan: tasks finish uniformly without stragglers
balanced_joined_df.write.format("delta").mode("overwrite").saveAsTable("prod_catalog.gold.balanced_sales")
```

**Enterprise Use Case:**  
Joining a 2-billion-row retail transactions dataset with store metadata where the e-commerce flagship store represents 85% of volume, preventing executor OOM and task stragglers.

**Architectural Invariants & Gotchas:**
- Salting breaks monolithic partitions into multiple smaller, evenly distributed shuffle buckets.
- Only apply salting to the specific skewed key; salting already uniform keys introduces unnecessary cross-join overhead.
- Ensure Adaptive Query Execution (AQE) skew join is enabled (spark.sql.adaptive.skewJoin.enabled = true) as first line of defense.
- Replicating small dimension tables by 16x is trivial in memory compared to the cost of a 4-hour stuck shuffle stage.

---

### Q4.12: Stateful Streaming Deduplication with Watermarking (Structured Streaming)

**Question & Concept Overview:**  
Managing streaming state stores with event-time watermarking to execute stateful streaming deduplication without unbounded memory bloat. Configured with RocksDB state store for mission-critical reliability.

**Production Implementation:**
```python
from pyspark.sql import functions as F

# 1. Enable RocksDB state store to avoid Java heap memory limits on high-state streams
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
)

# 2. Read real-time telemetry stream from Azure Event Hubs / Kafka
telemetry_stream = (
    spark.readStream
    .format("delta")
    .table("prod_catalog.bronze.iot_telemetry_raw")
)

# 3. Define 2-hour watermark and deduplicate on device_id + event_time
deduplicated_stream = (
    telemetry_stream
    .withWatermark("event_timestamp", "2 hours")
    .dropDuplicates(["device_id", "event_timestamp"])
)

# 4. Windowed stateful aggregation: 10-minute tumbling windows
windowed_metrics = (
    deduplicated_stream
    .groupBy(
        F.window("event_timestamp", "10 minutes"),
        "device_type"
    )
    .agg(
        F.avg("temperature").alias("avg_temp"),
        F.max("vibration").alias("max_vibration"),
        F.count("event_id").alias("reading_count")
    )
)

# 5. Output to Silver Delta table with checkpointing
query = (
    windowed_metrics
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/prod_catalog/checkpoints/iot_silver_state/")
    .trigger(processingTime="1 minute")
    .toTable("prod_catalog.silver.iot_aggregated_metrics")
)
```

**Enterprise Use Case:**  
Processing continuous streams of 50,000 IoT sensor events per second, filtering out redundant retry packets and computing 10-minute metrics with zero JVM heap exhaustion.

**Architectural Invariants & Gotchas:**
- Watermarking tells the state store when late data can be safely dropped and state purged from disk/memory.
- Default HDFS-backed state store keeps state in JVM heap; RocksDB offloads state to local SSD, supporting tens of millions of active keys.
- dropDuplicates with a watermark requires the watermark column to be present in the deduplication key set.
- Append output mode only emits rows once the window falls behind the active watermark threshold.

---

### Q4.13: Full Medallion Architecture DDL in Spark SQL (Architecture Patterns)

**Question & Concept Overview:**  
The Medallion Architecture (Bronze → Silver → Gold) organizes data into progressively refined layers. Bronze captures raw data with minimal transformation, Silver cleanses and conforms, Gold delivers business-ready aggregations. Implementing this in SQL requires careful DDL design with proper partitioning, clustering, properties, and governance from the start.

**Production Implementation:**
```python
-- =====================================================================
-- FULL MEDALLION ARCHITECTURE DDL
-- EU BFSI — Pan-European Retail Bank Transaction Platform
-- Catalog: eu_bfsi_prod | Workspace: Azure Databricks (East US 2)
-- Author: Data Engineering Team | Santosh Jammi, TCS
-- =====================================================================

-- ── CATALOG & SCHEMA SETUP ────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS eu_bfsi_prod
  COMMENT 'Production catalog — EU Banking platform on Azure Databricks';

CREATE SCHEMA IF NOT EXISTS eu_bfsi_prod.bronze
  MANAGED LOCATION 'abfss://bronze@euadlsprod.dfs.core.windows.net/'
  COMMENT 'Raw ingestion — no business transformations';

CREATE SCHEMA IF NOT EXISTS eu_bfsi_prod.silver
  MANAGED LOCATION 'abfss://silver@euadlsprod.dfs.core.windows.net/'
  COMMENT 'Cleansed, conformed, enriched';

CREATE SCHEMA IF NOT EXISTS eu_bfsi_prod.gold
  MANAGED LOCATION 'abfss://gold@euadlsprod.dfs.core.windows.net/'
  COMMENT 'Aggregated, business-ready, BI/ML ready';

-- ── BRONZE LAYER ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eu_bfsi_prod.bronze.bronze_transactions (
    _ingest_id          STRING          COMMENT 'Auto Loader file UUID',
    _source_file        STRING          COMMENT 'ADLS file path',
    _source_ts          TIMESTAMP       COMMENT 'File modification time',
    _ingest_ts          TIMESTAMP       NOT NULL COMMENT 'Databricks ingest time',
    _batch_id           STRING          COMMENT 'Pipeline run ID',
    _rescued_data       STRING          COMMENT 'Auto Loader schema rescue',
    txn_id              STRING,
    account_id          STRING,
    txn_raw_amount      STRING          COMMENT 'Raw — may contain commas/currency symbols',
    txn_currency        STRING,
    txn_type            STRING,
    txn_status          STRING,
    txn_timestamp_raw   STRING          COMMENT 'Raw timestamp string from source',
    source_system       STRING          COMMENT 'CORE_BANKING | SWIFT | SEPA | PSD2_OPEN'
)
USING DELTA
PARTITIONED BY (ingest_date DATE GENERATED ALWAYS AS (DATE(_ingest_ts)))
COMMENT 'Bronze — raw transactions as landed from source systems'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'       = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.columnMapping.mode'         = 'name',
    'delta.enableDeletionVectors'      = 'true',
    'data.layer'                       = 'bronze',
    'data.classification'              = 'RAW_CONFIDENTIAL',
    'pipeline.source'                  = 'AUTO_LOADER',
    'data.retention.years'             = '7'
);

-- ── SILVER LAYER ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eu_bfsi_prod.silver.silver_transactions (
    txn_id              STRING          NOT NULL COMMENT 'Deduplicated transaction key',
    account_id          STRING          NOT NULL,
    counterparty_id     STRING,
    txn_amount          DECIMAL(18, 4)  NOT NULL,
    txn_currency        STRING(3)       NOT NULL,
    txn_amount_eur      DECIMAL(18, 4)  COMMENT 'Normalized EUR amount via ECB rate',
    fx_rate             DECIMAL(12, 8),
    txn_type            STRING,
    txn_status          STRING,
    txn_timestamp       TIMESTAMP       NOT NULL,
    txn_date            DATE            GENERATED ALWAYS AS (DATE(txn_timestamp)),
    value_date          DATE,
    source_system       STRING,
    is_high_value       BOOLEAN         GENERATED ALWAYS AS (txn_amount >= 15000),
    amount_band         STRING          GENERATED ALWAYS AS (
                        CASE
                            WHEN txn_amount < 1000    THEN 'MICRO'
                            WHEN txn_amount < 10000   THEN 'SMALL'
                            WHEN txn_amount < 100000  THEN 'MEDIUM'
                            WHEN txn_amount < 1000000 THEN 'LARGE'
                            ELSE 'WHALE'
                        END),
    dq_score            DECIMAL(5, 2)   COMMENT 'Data quality score 0-100',
    dq_flags            ARRAY<STRING>   COMMENT 'DQ rule violations',
    _bronze_ingest_ts   TIMESTAMP       COMMENT 'Source bronze ingest time',
    _silver_ts          TIMESTAMP       NOT NULL COMMENT 'Silver promotion timestamp'
)
USING DELTA
PARTITIONED BY (txn_date)
CLUSTER BY (account_id, txn_currency)
COMMENT 'Silver — cleansed, deduplicated, FX-normalized transactions'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'            = 'true',
    'delta.autoOptimize.optimizeWrite'      = 'true',
    'delta.autoOptimize.autoCompact'        = 'true',
    'delta.columnMapping.mode'              = 'name',
    'delta.enableDeletionVectors'           = 'true',
    'delta.enableRowTracking'               = 'true',
    'data.layer'                            = 'silver',
    'data.classification'                   = 'CONFIDENTIAL',
    'data.pii.columns'                      = 'account_id,counterparty_id',
    'data.gdpr'                             = 'true',
    'data.retention.years'                  = '7'
);

-- ── GOLD LAYER ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eu_bfsi_prod.gold.gold_daily_account_summary (
    snapshot_date       DATE            NOT NULL,
    account_id          STRING          NOT NULL,
    risk_tier           STRING,
    product_code        STRING,
    country_code        STRING,
    txn_count           BIGINT,
    debit_count         BIGINT,
    credit_count        BIGINT,
    total_debit_eur     DECIMAL(18, 2),
    total_credit_eur    DECIMAL(18, 2),
    net_flow_eur        DECIMAL(18, 2),
    max_single_txn_eur  DECIMAL(18, 2),
    avg_txn_eur         DECIMAL(18, 2),
    unique_currencies   BIGINT,
    unique_counterparties BIGINT,
    high_value_count    BIGINT          COMMENT 'Transactions >= 15,000 EUR (CTR threshold)',
    cross_border_count  BIGINT,
    dq_pass_rate        DECIMAL(5, 2)   COMMENT 'Data quality pass % for the day',
    _gold_ts            TIMESTAMP       NOT NULL COMMENT 'Gold layer computation timestamp'
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMAND 'Gold — daily account-level summary for regulatory & BI reporting'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'data.layer'                        = 'gold',
    'data.classification'               = 'INTERNAL',
    'data.retention.years'              = '10',
    'business.domain'                   = 'PAYMENTS_ANALYTICS',
    'business.consumers'                = 'RISK,COMPLIANCE,FINANCE,BI'
);
```

**Enterprise Use Case:**  
Greenfield Medallion Architecture for a pan-European retail bank's Azure Databricks platform — supporting 15M daily transactions across 12 EU countries, with full lineage from Core Banking raw events through to COREP/FINREP regulatory reports and Power BI dashboards, all governed by Unity Catalog.

**Architectural Invariants & Gotchas:**
- Bronze = raw as-is with ingestion metadata; Silver = validated, typed, enriched; Gold = aggregated, business-ready — each layer has a distinct contract
- Generated columns (txn_date, is_high_value, amount_band) eliminate application-layer logic errors and guarantee consistency
- CLUSTER BY (Liquid Clustering) in Silver enables incremental re-clustering as data grows — no need to repartition
- CDF enabled on Bronze and Silver enables efficient incremental propagation — avoid full scans on multi-billion row tables
- TBLPROPERTIES as a data catalog: store owner, classification, PII columns, GDPR flag, retention, and business domain — queryable via SHOW TBLPROPERTIES

---

### Q4.14: Delta Live Tables (DLT) SQL Syntax — Multi-hop Pipeline (Delta Live Tables)

**Question & Concept Overview:**  
Delta Live Tables (DLT) is Databricks' declarative ETL framework. SQL-based DLT pipelines define streaming and batch tables with expectations (data quality rules) and automatic dependency resolution. DLT manages infrastructure, retries, monitoring, and schema evolution, making it ideal for production Medallion pipelines.

**Production Implementation:**
```python
-- =============================================
-- Delta Live Tables (DLT) SQL Pipeline
-- EU BFSI — Bronze→Silver→Gold Multi-hop
-- =============================================
-- File: /pipeline/eu_bfsi_dlt_pipeline.sql
-- Pipeline name: EU_BFSI_TXN_PIPELINE
-- =============================================

-- ─── BRONZE: Raw Ingestion via Auto Loader ────
CREATE OR REFRESH STREAMING TABLE bronze_transactions
COMMENT 'Bronze — raw transactions from ADLS via Auto Loader'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality.layer'              = 'bronze'
)
AS SELECT
    _metadata.file_path         AS _source_file,
    _metadata.file_modification_time AS _source_ts,
    current_timestamp()         AS _ingest_ts,
    monotonically_increasing_id() AS _row_id,
    *
FROM cloud_files(
    'abfss://raw@euadlsprod.dfs.core.windows.net/transactions/',
    'json',
    map(
        'cloudFiles.schemaLocation',
        'abfss://checkpoints@euadlsprod.dfs.core.windows.net/bronze_txn_schema/',
        'cloudFiles.inferColumnTypes',   'true',
        'cloudFiles.schemaEvolutionMode', 'rescue'
    )
);

-- ─── SILVER: Cleansed with DQ Expectations ────
CREATE OR REFRESH STREAMING TABLE silver_transactions (
  CONSTRAINT txn_id_not_null
      EXPECT (txn_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount
      EXPECT (txn_amount > 0 AND txn_amount < 100000000) ON VIOLATION DROP ROW,
  CONSTRAINT valid_currency
      EXPECT (txn_currency RLIKE '^[A-Z]{3}$') ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp
      EXPECT (txn_timestamp IS NOT NULL AND txn_timestamp <= current_timestamp())
      ON VIOLATION DROP ROW,
  CONSTRAINT account_id_not_null
      EXPECT (account_id IS NOT NULL) ON VIOLATION WARN  -- Warn, don't drop
)
COMMENT 'Silver — validated, typed, FX-normalized transactions'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality.layer'              = 'silver'
)
AS SELECT
    txn_id,
    account_id,
    CAST(txn_amount AS DECIMAL(18,4))           AS txn_amount,
    UPPER(TRIM(txn_currency))                   AS txn_currency,
    CAST(txn_timestamp AS TIMESTAMP)            AS txn_timestamp,
    DATE(CAST(txn_timestamp AS TIMESTAMP))      AS txn_date,
    COALESCE(txn_type, 'UNKNOWN')               AS txn_type,
    COALESCE(txn_status, 'PENDING')             AS txn_status,
    COALESCE(source_system, 'UNKNOWN')          AS source_system,
    _ingest_ts                                  AS _bronze_ingest_ts,
    current_timestamp()                         AS _silver_ts
FROM STREAM(LIVE.bronze_transactions)
WHERE _rescued_data IS NULL;  -- Exclude rescued/malformed rows

-- ─── SILVER: FX-Enriched via LIVE table join ──
CREATE OR REFRESH STREAMING TABLE silver_transactions_enriched
COMMENT 'Silver enriched — with EUR normalization'
AS SELECT
    t.*,
    COALESCE(fx.exchange_rate, 1.0)             AS fx_rate,
    ROUND(t.txn_amount * COALESCE(fx.exchange_rate, 1.0), 4) AS txn_amount_eur
FROM STREAM(LIVE.silver_transactions) t
LEFT JOIN LIVE.dim_fx_rates fx   -- Static table (non-streaming JOIN)
    ON t.txn_currency = fx.from_currency
    AND t.txn_date    = fx.rate_date
    AND fx.to_currency = 'EUR';

-- ─── GOLD: Daily Account Summary ──────────────
CREATE OR REFRESH LIVE TABLE gold_daily_account_summary
COMMENT 'Gold — daily account-level aggregation for BI & regulatory reporting'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
AS SELECT
    txn_date                            AS snapshot_date,
    account_id,
    COUNT(*)                            AS txn_count,
    SUM(CASE WHEN txn_type='DEBIT'  THEN txn_amount_eur ELSE 0 END) AS total_debit_eur,
    SUM(CASE WHEN txn_type='CREDIT' THEN txn_amount_eur ELSE 0 END) AS total_credit_eur,
    SUM(CASE WHEN txn_type='DEBIT'  THEN txn_amount_eur
             WHEN txn_type='CREDIT' THEN -txn_amount_eur
             ELSE 0 END)                AS net_flow_eur,
    MAX(txn_amount_eur)                 AS max_single_txn_eur,
    COUNT(DISTINCT txn_currency)        AS unique_currencies,
    SUM(CASE WHEN txn_amount_eur >= 15000 THEN 1 ELSE 0 END) AS high_value_count,
    current_timestamp()                 AS _gold_ts
FROM LIVE.silver_transactions_enriched
GROUP BY txn_date, account_id;
```

**Enterprise Use Case:**  
Production payment processing pipeline at a European fintech — DLT's declarative model with built-in expectations replaces 3,000 lines of imperative PySpark with 200 lines of SQL, while automated retries and monitoring reduce on-call incidents by 70%. The pipeline processes 50M daily transactions with < 5-minute latency from source to gold.

**Architectural Invariants & Gotchas:**
- EXPECT ... ON VIOLATION DROP ROW: removes bad rows; WARN: logs violation but keeps row; FAIL: halts pipeline — choose based on data quality tolerance
- STREAM(LIVE.table) reads incrementally from an upstream DLT table — enables multi-hop streaming without duplicates
- DLT resolves dependencies automatically from LIVE.* references — no need to manage execution order manually
- cloud_files() is the DLT-native Auto Loader function — handles schema inference, evolution, and checkpoint management
- DLT expectations are tracked in the Pipeline Events UI — violation rates feed the Data Quality dashboard for SLA monitoring

---

### Q4.15: Unity Catalog Governance: GRANT, REVOKE, Row Filters, Column Masks (Data Governance)

**Question & Concept Overview:**  
Unity Catalog provides fine-grained access control with GRANT/REVOKE at catalog/schema/table/column levels, dynamic row filters for per-user data visibility, and column masks for PII redaction. These replace legacy IP-based ACLs with policy-as-code governance that scales across multi-workspace, multi-regulation BFSI environments.

**Production Implementation:**
```python
-- =============================================
-- Unity Catalog Governance
-- EU BFSI — GDPR-Compliant Access Control
-- =============================================

USE CATALOG eu_bfsi_prod;

-- ─── 1. GRANT — Catalog/Schema/Table level ────

-- Catalog-level: USE privilege
GRANT USE CATALOG ON CATALOG eu_bfsi_prod
    TO `data-analysts`, `compliance-officers`, `bi-team`;

-- Schema-level: USE privilege
GRANT USE SCHEMA ON SCHEMA eu_bfsi_prod.gold
    TO `data-analysts`, `bi-team`, `compliance-officers`;

-- Table-level: SELECT only (read-only analysts)
GRANT SELECT ON TABLE eu_bfsi_prod.gold.gold_daily_account_summary
    TO `data-analysts`;

-- Silver: Only compliance and engineering can access
GRANT SELECT ON TABLE eu_bfsi_prod.silver.silver_transactions
    TO `compliance-officers`, `data-engineers`;

-- Full DML for data engineers
GRANT SELECT, INSERT, UPDATE, DELETE, MODIFY
    ON TABLE eu_bfsi_prod.silver.silver_transactions
    TO `data-engineers`;

-- Schema-wide GRANT for gold read
GRANT SELECT ON SCHEMA eu_bfsi_prod.gold
    TO `bi-team`;

-- Function execution
GRANT EXECUTE ON FUNCTION eu_bfsi_prod.security.mask_iban
    TO `data-analysts`;

-- ─── 2. REVOKE ─────────────────────────────────
REVOKE SELECT ON TABLE eu_bfsi_prod.silver.silver_transactions
    FROM `data-analysts`;

-- ─── 3. ROW FILTER — per-user data visibility ──
-- Analysts see only their region's data
CREATE FUNCTION IF NOT EXISTS eu_bfsi_prod.security.filter_by_region (
    row_country_code STRING
)
RETURNS BOOLEAN
RETURN
    is_member('data-engineers')         -- Engineers see all
    OR is_member('compliance-officers') -- Compliance sees all
    OR (
        is_member('uk-analysts')  AND row_country_code = 'GB'
    ) OR (
        is_member('de-analysts')  AND row_country_code = 'DE'
    ) OR (
        is_member('fr-analysts')  AND row_country_code = 'FR'
    );

-- Apply row filter to table
ALTER TABLE eu_bfsi_prod.gold.gold_daily_account_summary
    SET ROW FILTER eu_bfsi_prod.security.filter_by_region
    ON (country_code);

-- ─── 4. COLUMN MASK — PII redaction ────────────
-- Mask account_id for non-privileged users
CREATE FUNCTION IF NOT EXISTS eu_bfsi_prod.security.mask_account_id (
    account_id STRING
)
RETURNS STRING
RETURN CASE
    WHEN is_member('data-engineers')    THEN account_id
    WHEN is_member('compliance-officers') THEN
        CONCAT(LEFT(account_id, 3), '****', RIGHT(account_id, 4))
    ELSE
        SHA2(account_id, 256)  -- Pseudonymize for analysts
END;

ALTER TABLE eu_bfsi_prod.silver.silver_transactions
    ALTER COLUMN account_id
    SET MASK eu_bfsi_prod.security.mask_account_id;

-- ─── 5. Show grants / effective permissions ────
SHOW GRANTS ON TABLE eu_bfsi_prod.silver.silver_transactions;
SHOW GRANTS ON SCHEMA eu_bfsi_prod.silver;
SHOW GRANTS ON CATALOG eu_bfsi_prod;

-- ─── 6. Information schema queries ────────────
SELECT
    table_catalog, table_schema, table_name,
    privilege_type, grantee, is_grantable
FROM eu_bfsi_prod.information_schema.table_privileges
WHERE table_schema = 'silver'
ORDER BY table_name, grantee;

-- ─── 7. Audit access via system tables ─────────
SELECT
    event_time, user_name, action_name,
    request_params.table_full_name,
    response.status_code
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 7 DAYS
  AND action_name IN ('SELECT', 'INSERT', 'DELETE', 'MERGE')
  AND request_params.table_full_name
      LIKE 'eu_bfsi_prod.silver%'
ORDER BY event_time DESC
LIMIT 500;
```

**Enterprise Use Case:**  
Implementing GDPR Article 25 (data protection by design) at a pan-European insurance company — row filters restrict UK, DE, FR analysts to their jurisdiction's customer data, column masks pseudonymize account IDs for the analytics team, and audit logs feed the quarterly GDPR compliance report automatically from system.access.audit.

**Architectural Invariants & Gotchas:**
- Row filters are transparent — users don't see the filter; they simply get fewer rows with no error message, which is the intended behavior for need-to-know data
- Column masks evaluate per-row at query time — use is_member() / is_account_group_member() for group-based logic
- GRANT on SCHEMA applies to all current tables but NOT future tables — use GRANT ON FUTURE TABLES for forward-covering grants
- system.access.audit provides immutable audit log of all data access events — required for GDPR Article 30 records of processing activities
- Unity Catalog GRANTs are workspace-independent — grants made in one workspace apply across all workspaces sharing the metastore

---

### Q4.16: Data Vault 2.0 SQL Patterns: Hub, Link, Satellite (Architecture Patterns)

**Question & Concept Overview:**  
Data Vault 2.0 provides a highly scalable, auditable, and agile data modeling approach for enterprise data warehouses. The three core entity types — Hubs (unique business keys), Links (relationships), and Satellites (context/attributes) — are implemented in Spark SQL with Delta tables and MERGE INTO for parallel, idempotent loads. DV2 is ideal for BFSI environments requiring full auditability.

**Production Implementation:**
```python
-- =====================================================================
-- DATA VAULT 2.0 SQL PATTERNS
-- EU BFSI — Payment Transaction Data Vault
-- =====================================================================

USE CATALOG eu_bfsi_prod;
USE SCHEMA dv_raw;  -- Raw Vault schema

-- ─── HUB: Hub_Transaction ─────────────────────────────────────────────
-- Stores unique business keys for transactions
CREATE TABLE IF NOT EXISTS hub_transaction (
    txn_hash_key    STRING  NOT NULL  COMMENT 'MD5(txn_id || source_system)',
    load_date       TIMESTAMP NOT NULL COMMENT 'First seen timestamp',
    record_source   STRING  NOT NULL  COMMENT 'System that first provided this key',
    txn_id          STRING  NOT NULL  COMMENT 'Business key from source'
)
USING DELTA
COMMENT 'Hub — unique transaction business keys'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true');

-- Load Hub (idempotent — only new hash keys inserted)
MERGE INTO hub_transaction AS tgt
USING (
    SELECT DISTINCT
        MD5(CONCAT_WS('||', txn_id, source_system))  AS txn_hash_key,
        MIN(_ingest_ts) OVER (PARTITION BY txn_id)   AS load_date,
        source_system                                 AS record_source,
        txn_id
    FROM eu_bfsi_prod.bronze.bronze_transactions
    WHERE ingest_date = current_date()
) AS src
ON tgt.txn_hash_key = src.txn_hash_key
WHEN NOT MATCHED THEN INSERT *;

-- ─── HUB: Hub_Account ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hub_account (
    acct_hash_key   STRING  NOT NULL,
    load_date       TIMESTAMP NOT NULL,
    record_source   STRING  NOT NULL,
    account_id      STRING  NOT NULL
)
USING DELTA COMMENT 'Hub — unique account business keys';

-- ─── LINK: Link_Transaction_Account ───────────────────────────────────
-- Represents the relationship: Transaction involves Account
CREATE TABLE IF NOT EXISTS link_txn_account (
    link_hash_key   STRING  NOT NULL  COMMENT 'MD5(txn_hash_key || acct_hash_key)',
    load_date       TIMESTAMP NOT NULL,
    record_source   STRING  NOT NULL,
    txn_hash_key    STRING  NOT NULL  COMMENT 'FK → hub_transaction',
    acct_hash_key   STRING  NOT NULL  COMMENT 'FK → hub_account'
)
USING DELTA COMMENT 'Link — transaction-to-account relationship';

MERGE INTO link_txn_account AS tgt
USING (
    SELECT
        MD5(CONCAT_WS('||',
            MD5(CONCAT_WS('||', b.txn_id, b.source_system)),
            MD5(b.account_id)
        ))                                          AS link_hash_key,
        b._ingest_ts                                AS load_date,
        b.source_system                             AS record_source,
        MD5(CONCAT_WS('||', b.txn_id, b.source_system)) AS txn_hash_key,
        MD5(b.account_id)                           AS acct_hash_key
    FROM eu_bfsi_prod.bronze.bronze_transactions b
    WHERE ingest_date = current_date()
) AS src
ON tgt.link_hash_key = src.link_hash_key
WHEN NOT MATCHED THEN INSERT *;

-- ─── SATELLITE: Sat_Transaction_Details ───────────────────────────────
-- Stores context/attributes for transactions (versioned)
CREATE TABLE IF NOT EXISTS sat_transaction_details (
    txn_hash_key    STRING  NOT NULL  COMMENT 'FK → hub_transaction',
    load_date       TIMESTAMP NOT NULL COMMENT 'Record effective from',
    load_end_date   TIMESTAMP         COMMENT 'NULL = current; populated when superseded',
    record_source   STRING  NOT NULL,
    hash_diff       STRING  NOT NULL  COMMENT 'MD5 of all satellite attributes for change detection',
    -- Descriptive attributes
    txn_amount      DECIMAL(18,4),
    txn_currency    STRING(3),
    txn_type        STRING,
    txn_status      STRING,
    txn_timestamp   TIMESTAMP,
    source_system   STRING
)
USING DELTA
COMMENT 'Satellite — transaction detail attributes with full history'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true');

-- Load Satellite (insert-only, no updates)
INSERT INTO sat_transaction_details
SELECT
    MD5(CONCAT_WS('||', b.txn_id, b.source_system))  AS txn_hash_key,
    b._ingest_ts                                       AS load_date,
    NULL                                               AS load_end_date,
    b.source_system                                    AS record_source,
    MD5(CONCAT_WS('||',
        COALESCE(CAST(b.txn_raw_amount AS STRING), ''),
        COALESCE(b.txn_currency, ''),
        COALESCE(b.txn_type, ''),
        COALESCE(b.txn_status, ''),
        COALESCE(CAST(b.txn_timestamp_raw AS STRING), '')
    ))                                                 AS hash_diff,
    CAST(REGEXP_REPLACE(b.txn_raw_amount, '[^0-9.]', '') AS DECIMAL(18,4)) AS txn_amount,
    UPPER(TRIM(b.txn_currency))                        AS txn_currency,
    b.txn_type,
    b.txn_status,
    CAST(b.txn_timestamp_raw AS TIMESTAMP)             AS txn_timestamp,
    b.source_system
FROM eu_bfsi_prod.bronze.bronze_transactions b
WHERE NOT EXISTS (
    -- Only insert if hash_diff changed (new attributes)
    SELECT 1 FROM sat_transaction_details s
    WHERE s.txn_hash_key = MD5(CONCAT_WS('||', b.txn_id, b.source_system))
      AND s.hash_diff = MD5(CONCAT_WS('||',
            COALESCE(CAST(b.txn_raw_amount AS STRING), ''),
            COALESCE(b.txn_currency, ''),
            COALESCE(b.txn_type, ''),
            COALESCE(b.txn_status, ''),
            COALESCE(CAST(b.txn_timestamp_raw AS STRING), '')
        ))
      AND s.load_end_date IS NULL
);
```

**Enterprise Use Case:**  
Building a Data Vault 2.0 raw vault for a pan-European bank's enterprise data warehouse — Hub/Link/Sat pattern absorbs schema changes from Core Banking, SWIFT, and PSD2 Open Banking feeds without breaking downstream consumers, while the hash-key approach enables parallel loading of 200M daily records across 50 Databricks worker nodes.

**Architectural Invariants & Gotchas:**
- Hub stores ONLY the business key and hash key — all attributes belong in satellites; this enforces separation of concerns
- Hash keys (MD5/SHA1 of business key + source) enable parallel loading of all vault entities without sequence generators
- Satellites are insert-only append tables — hash_diff change detection avoids re-inserting unchanged records
- Links require all related hub hash keys — they store relationships with timestamps, enabling full temporal analysis of relationship history
- DV2's parallel load pattern (Hub/Link/Sat load simultaneously) scales linearly — a 1-hour sequential load becomes 15 minutes with parallel Databricks jobs

---

### Q4.17: GDPR Erasure with DELETE and VACUUM (Data Governance)

**Question & Concept Overview:**  
GDPR Article 17 (Right to Erasure) requires physically removing personal data across all layers of the data lake within 30 days of a verified erasure request. Delta's DELETE combined with column mapping and VACUUM provides a compliant erasure workflow. The challenge is cascading deletions across Bronze, Silver, and Gold layers while maintaining referential integrity.

**Production Implementation:**
```python
-- =============================================
-- GDPR Right to Erasure — Full Pipeline
-- EU BFSI — Customer Data Deletion Workflow
-- Author: Santosh Jammi, TCS Data Engineering
-- =============================================

USE CATALOG eu_bfsi_prod;

-- ─── STEP 0: Create erasure request tracking table ────
CREATE TABLE IF NOT EXISTS eu_bfsi_prod.governance.gdpr_erasure_requests (
    request_id          STRING NOT NULL,
    customer_id         STRING NOT NULL,
    requested_by        STRING,
    request_date        DATE NOT NULL,
    approved_date       DATE,
    approval_status     STRING COMMENT 'PENDING | APPROVED | REJECTED | COMPLETED',
    erasure_scope       ARRAY<STRING> COMMENT 'List of tables to erase from',
    completed_ts        TIMESTAMP,
    completion_notes    STRING
) USING DELTA;

-- ─── STEP 1: Approve and scope the erasure request ────
INSERT INTO eu_bfsi_prod.governance.gdpr_erasure_requests
VALUES (
    'GDPR-2024-001247',
    'CUST-EU-83847291',
    'dpo@eurobank.eu',
    '2024-12-15',
    '2024-12-16',
    'APPROVED',
    ARRAY(
        'silver.silver_customers',
        'silver.silver_accounts',
        'silver.silver_transactions',
        'bronze.bronze_transactions',
        'gold.gold_daily_account_summary'
    ),
    NULL,
    NULL
);

-- ─── STEP 2: Collect all affected identifiers ─────────
CREATE OR REPLACE TEMPORARY VIEW v_erasure_scope AS
SELECT
    r.customer_id,
    a.account_id,
    r.request_id
FROM eu_bfsi_prod.governance.gdpr_erasure_requests r
JOIN eu_bfsi_prod.silver.silver_accounts a
    ON r.customer_id = a.customer_id
WHERE r.request_id = 'GDPR-2024-001247'
  AND r.approval_status = 'APPROVED';

-- ─── STEP 3: Delete PII from Silver customers ────────
DELETE FROM eu_bfsi_prod.silver.silver_customers
WHERE customer_id IN (
    SELECT customer_id FROM v_erasure_scope
);

-- ─── STEP 4: Pseudonymize transaction references ──────
-- (Cannot delete transactions — needed for financial audit)
-- Replace PII fields with pseudonymous tokens
UPDATE eu_bfsi_prod.silver.silver_transactions
SET
    account_id      = SHA2(CONCAT(account_id, 'GDPR_SALT_2024'), 256),
    counterparty_id = SHA2(CONCAT(counterparty_id, 'GDPR_SALT_2024'), 256)
WHERE account_id IN (
    SELECT account_id FROM v_erasure_scope
);

-- ─── STEP 5: Delete from bronze (raw data) ───────────
DELETE FROM eu_bfsi_prod.bronze.bronze_transactions
WHERE account_id IN (
    SELECT account_id FROM v_erasure_scope
);

-- ─── STEP 6: Delete from gold aggregations ───────────
DELETE FROM eu_bfsi_prod.gold.gold_daily_account_summary
WHERE account_id IN (
    SELECT account_id FROM v_erasure_scope
);

-- ─── STEP 7: Physical file deletion with VACUUM ───────
-- CRITICAL: Set retention to 0 ONLY for GDPR compliance
-- First, disable safety check:
SET spark.databricks.delta.retentionDurationCheck.enabled = false;

VACUUM eu_bfsi_prod.silver.silver_customers RETAIN 0 HOURS;
VACUUM eu_bfsi_prod.bronze.bronze_transactions RETAIN 0 HOURS;
VACUUM eu_bfsi_prod.silver.silver_transactions RETAIN 0 HOURS;
VACUUM eu_bfsi_prod.gold.gold_daily_account_summary RETAIN 0 HOURS;

-- Re-enable safety check
SET spark.databricks.delta.retentionDurationCheck.enabled = true;

-- ─── STEP 8: Mark request as completed ───────────────
UPDATE eu_bfsi_prod.governance.gdpr_erasure_requests
SET
    approval_status = 'COMPLETED',
    completed_ts    = current_timestamp(),
    completion_notes = 'PII deleted from 4 tables; transactions pseudonymized; VACUUM completed'
WHERE request_id = 'GDPR-2024-001247';

-- ─── STEP 9: Verify erasure ───────────────────────────
SELECT COUNT(*) AS remaining_records
FROM eu_bfsi_prod.silver.silver_customers
WHERE customer_id = 'CUST-EU-83847291';
-- Expected: 0
```

**Enterprise Use Case:**  
GDPR Article 17 erasure automation at a European digital bank — a former customer exercises their right to erasure; the workflow deletes PII from Silver/Bronze/Gold layers, pseudonymizes transaction references (preserving financial audit trail), runs VACUUM to purge old file versions, and generates a compliance certificate within 72 hours of the request.

**Architectural Invariants & Gotchas:**
- VACUUM RETAIN 0 HOURS physically removes Parquet files from storage — do this ONLY for GDPR; normal operations should keep 7+ days
- Disable retentionDurationCheck only temporarily and in the scope of GDPR deletion — never as a permanent setting
- Financial transactions often cannot be deleted (money laundering regulations require 5-7 year retention) — pseudonymization with SHA256 is the compliant alternative
- Time travel to versions before deletion becomes impossible after VACUUM — this is intentional for GDPR compliance
- Maintain the erasure_requests audit table permanently — it's your proof of compliance without retaining the erased PII

---

### Q4.18: FinOps Table Partitioning and Cost Optimization Strategies (FinOps & Performance)

**Question & Concept Overview:**  
FinOps in Azure Databricks requires minimizing DBU consumption and ADLS storage costs while maintaining query performance SLAs. Strategic partitioning, Liquid Clustering, Photon engine optimization, and compute auto-scaling are the primary levers. Poor partitioning (too many small files or over-partitioned tables) is the #1 cause of runaway Databricks costs in BFSI platforms.

**Production Implementation:**
```python
-- =============================================
-- FinOps Partitioning & Cost Optimization
-- EU BFSI — Azure Databricks Cost Engineering
-- =============================================

USE CATALOG eu_bfsi_prod;
USE SCHEMA silver;

-- ─── 1. Diagnose partition health ─────────────
-- Check number of files and sizes per partition
SELECT
    partition.txn_date,
    COUNT(*)                AS num_files,
    SUM(size) / 1e9         AS total_size_gb,
    AVG(size) / 1e6         AS avg_file_size_mb,
    MIN(size) / 1e3         AS min_file_kb,
    MAX(size) / 1e6         AS max_file_mb,
    SUM(CASE WHEN size < 1048576 THEN 1 ELSE 0 END) AS tiny_files_under_1mb
FROM (
    DESCRIBE DETAIL eu_bfsi_prod.silver.silver_transactions
)
CROSS JOIN (
    SHOW PARTITIONS eu_bfsi_prod.silver.silver_transactions
) partition
GROUP BY partition.txn_date
ORDER BY tiny_files_under_1mb DESC
LIMIT 30;

-- ─── 2. Optimal partitioning strategies ───────

-- Strategy A: Date partitioning (most common for time-series BFSI data)
-- Good for: daily batch queries, time-range scans
-- Target: 128MB - 1GB per partition file
CREATE TABLE IF NOT EXISTS silver_transactions_v2
USING DELTA
PARTITIONED BY (txn_date)  -- ~1 partition per day, sized for daily volume
COMMENT 'Date-partitioned silver transactions';

-- Strategy B: Liquid Clustering (DBR 13.3+ — preferred over static partitioning)
-- Allows incremental re-clustering without full rewrites
CREATE TABLE IF NOT EXISTS silver_transactions_lc
USING DELTA
CLUSTER BY (txn_date, account_id, txn_currency)
COMMENT 'Liquid-clustered silver transactions — auto-optimized';

-- ─── 3. Auto-optimize settings for cost control ─
ALTER TABLE silver_transactions
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'   = 'true',  -- Bin-pack on write
    'delta.autoOptimize.autoCompact'     = 'true',  -- Compact in background
    'delta.targetFileSize'               = '134217728',  -- 128MB target
    'delta.tuneFileSizesForRewrites'     = 'true'   -- Optimize for MERGE
);

-- ─── 4. Statistics collection for better plans ──
ANALYZE TABLE silver_transactions
COMPUTE STATISTICS FOR ALL COLUMNS;

-- ─── 5. Partition pruning verification ──────────
-- EXPLAIN shows partition skipping
EXPLAIN COST
SELECT COUNT(*)
FROM silver_transactions
WHERE txn_date = '2024-12-31';  -- Should show: PartitionFilters: [txn_date = 2024-12-31]

-- ─── 6. Archive cold partitions to cheaper storage
-- Move partitions older than 2 years to archive tier
CREATE TABLE IF NOT EXISTS eu_bfsi_prod.archive.silver_transactions_archive
USING DELTA
LOCATION 'abfss://archive@euadlsarchive.dfs.core.windows.net/transactions/'
COMMENT 'Archive — transactions > 2 years (Cool tier ADLS)';

-- Insert old data into archive then delete from hot
INSERT INTO eu_bfsi_prod.archive.silver_transactions_archive
SELECT * FROM eu_bfsi_prod.silver.silver_transactions
WHERE txn_date < DATE_SUB(current_date(), 730);

-- Then delete from hot tier (after verifying archive)
DELETE FROM eu_bfsi_prod.silver.silver_transactions
WHERE txn_date < DATE_SUB(current_date(), 730);
VACUUM eu_bfsi_prod.silver.silver_transactions RETAIN 168 HOURS;

-- ─── 7. Query optimization via SET configs ──────
-- Reduce shuffle partitions for small datasets (cost saving)
SET spark.sql.shuffle.partitions = 200;  -- Default 200; reduce for < 10GB queries

-- Enable AQE (default in Spark 3.x)
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;

-- ─── 8. Bloom filter for high-cardinality lookups
CREATE BLOOMFILTER INDEX ON TABLE silver_transactions
FOR COLUMNS (
    txn_id OPTIONS (fpp=0.001, numItems=5000000000),
    account_id OPTIONS (fpp=0.01, numItems=10000000)
);
```

**Enterprise Use Case:**  
FinOps optimization at a European digital bank where uncontrolled Databricks spend had grown 340% in 18 months — audit reveals 2.3 billion tiny files from 3 years of unoptimized streaming writes. Liquid Clustering + OPTIMIZE + VACUUM + ADLS tiering reduces monthly Azure spend by 58% while improving P95 query latency from 4 minutes to 35 seconds.

**Architectural Invariants & Gotchas:**
- Target 128MB-1GB file sizes in Delta — tiny files (<1MB) cause excessive file listing overhead and slow queries significantly
- Liquid Clustering outperforms static partitioning for high-cardinality dimensions — it clusters on multiple columns without exploding partition count
- ADLS Gen2 tiering: Hot (frequently accessed), Cool (infrequently accessed, cheaper), Archive (rare access, much cheaper) — move >2-year data to Cool tier for 50% storage cost saving
- Adaptive Query Execution (AQE) dynamically coalesces shuffle partitions, handles skew joins, and switches join strategies at runtime — keep enabled for all production clusters
- Bloom filters dramatically speed up equality lookups on high-cardinality columns (txn_id, account_id) — each file's bloom filter prevents unnecessary file reads for point lookups

---

### Q4.19: Microsoft Fabric Lakehouse SQL Endpoint and Cross-Workspace Queries (Microsoft Fabric)

**Question & Concept Overview:**  
Microsoft Fabric's Lakehouse exposes a SQL Analytics Endpoint (formerly SQL Server endpoint) that supports T-SQL queries directly against Delta tables in OneLake. The unified OneLake storage model enables cross-workspace queries and shortcut-based data sharing without data movement. Fabric's Direct Lake mode powers Power BI with columnar in-memory performance over Delta files.

**Production Implementation:**
```python
-- =============================================
-- Microsoft Fabric Lakehouse SQL
-- EU BFSI — Cross-Workspace Analytics
-- =============================================

-- ─── 1. Fabric SQL Analytics Endpoint (T-SQL compatible) ─
-- Access via: SQL connection string in Fabric workspace
-- jdbc:sqlserver://<workspace>.datawarehouse.fabric.microsoft.com

-- Query Delta table via SQL Analytics Endpoint
SELECT TOP 100
    txn_id,
    account_id,
    txn_amount,
    txn_currency,
    txn_date
FROM [eu_bfsi_lakehouse].[dbo].[silver_transactions]
WHERE txn_date = CAST(GETDATE() AS DATE)
  AND txn_amount > 10000
ORDER BY txn_amount DESC;

-- ─── 2. OneLake Shortcut — cross-workspace data sharing
-- (Configured via Fabric portal; queries are transparent)
-- Shortcut from workspace B to workspace A's Delta table:
SELECT COUNT(*), SUM(txn_amount_eur)
FROM [eu_analytics_lakehouse].[dbo].[silver_transactions]  -- Shortcut to prod lakehouse
WHERE txn_date >= DATEADD(DAY, -30, GETDATE());

-- ─── 3. Fabric Warehouse — cross-item queries ─────────────
-- Query across Lakehouse and Warehouse in same workspace
SELECT
    lh.account_id,
    lh.txn_amount_eur,
    wh.customer_segment,
    wh.relationship_manager
FROM [eu_bfsi_lakehouse].[dbo].[silver_transactions] lh  -- Lakehouse (Delta)
JOIN [eu_bfsi_warehouse].[dbo].[dim_customers] wh         -- Fabric Warehouse table
    ON lh.account_id = wh.account_id
WHERE lh.txn_date = CAST(GETDATE()-1 AS DATE);

-- ─── 4. Azure Databricks ↔ Fabric interop ────────────────
-- Option A: Fabric shortcut to ADLS Gen2 (where Databricks Delta tables live)
-- Create shortcut in Fabric to:
-- abfss://silver@euadlsprod.dfs.core.windows.net/silver_transactions/
-- → Available as: [eu_bfsi_lakehouse].[dbo].[silver_transactions]

-- Option B: From Databricks, write to Fabric OneLake via ABFSS connector
-- Databricks SQL writing to Fabric OneLake:
CREATE TABLE IF NOT EXISTS fabric.eu_bfsi_lakehouse.silver_transactions_export
USING DELTA
LOCATION 'abfss://eu_bfsi_lakehouse@onelake.dfs.fabric.microsoft.com/silver_transactions/'
AS SELECT * FROM eu_bfsi_prod.silver.silver_transactions
WHERE txn_date = current_date() - INTERVAL 1 DAY;

-- ─── 5. Direct Lake — Fabric Power BI optimization ────────
-- Direct Lake reads Delta Parquet files directly into memory
-- No import, no DirectQuery latency — native columnar performance
-- Trigger Direct Lake framing refresh via SQL:
-- EXEC sp_refreshsqlmodule '[dbo].[silver_transactions]';

-- ─── 6. Fabric SQL — GDPR-compliant view ─────────────────
CREATE OR ALTER VIEW [dbo].[v_silver_transactions_masked]
AS
SELECT
    txn_id,
    -- Mask account_id using SQL dynamic data masking
    CASE
        WHEN IS_MEMBER('DataEngineers') = 1 THEN account_id
        ELSE CONCAT('****', RIGHT(account_id, 6))
    END AS account_id,
    txn_amount,
    txn_currency,
    txn_date,
    txn_type
FROM [dbo].[silver_transactions];

-- Grant access to view (not underlying table)
GRANT SELECT ON [dbo].[v_silver_transactions_masked]
    TO [eu-analysts-sg@bank.eu];
```

**Enterprise Use Case:**  
A European universal bank's analytics modernization — Azure Databricks runs the heavy ETL (Bronze→Silver→Gold via DLT), writes Delta tables to ADLS Gen2, and Fabric shortcuts expose the same tables to Power BI via Direct Lake, eliminating the legacy SQL Server data warehouse while reducing time-to-insight from daily batch to real-time.

**Architectural Invariants & Gotchas:**
- Fabric SQL Analytics Endpoint is T-SQL compatible — existing SQL Server queries work with minimal changes, lowering migration effort
- OneLake Shortcuts eliminate data duplication — the same Delta file is queryable from Databricks, Fabric Lakehouse, and Fabric Warehouse simultaneously
- Direct Lake mode achieves Power BI report render times under 2 seconds on billion-row Delta tables — it frames data into columnar memory cache on demand
- ABFSS connector allows Databricks to write directly to Fabric OneLake — enabling hybrid architectures where Databricks processes and Fabric serves
- Cross-workspace queries in Fabric use the workspace identity for authorization — ensure OneLake permissions are configured at both workspace and item levels

---

### Q4.20: Databricks SQL Functions and Procedures (CREATE FUNCTION, SQL UDF) (Reusable SQL Components)

**Question & Concept Overview:**  
Databricks supports SQL User Defined Functions (UDFs), Python UDFs registered in Unity Catalog, and stored procedures-like patterns using EXECUTE IMMEDIATE. SQL UDFs enable reusable, versionable business logic — critical for regulatory calculations (PD, LGD, risk scoring) that must be consistent across all pipelines and consumers.

**Production Implementation:**
```python
-- =============================================
-- SQL Functions and Reusable Logic
-- EU BFSI — Regulatory Calculation Library
-- =============================================

USE CATALOG eu_bfsi_prod;

-- ─── 1. Scalar SQL UDF ────────────────────────
CREATE OR REPLACE FUNCTION eu_bfsi_prod.utils.calculate_ead (
    outstanding_balance DECIMAL(18, 4),
    undrawn_commitment  DECIMAL(18, 4),
    credit_conversion_factor DECIMAL(5, 4)  -- CCF: 0.0 to 1.0
)
RETURNS DECIMAL(18, 4)
COMMENT 'Exposure at Default (EAD) = Outstanding + CCF * Undrawn. Used for COREP credit risk.'
RETURN outstanding_balance + (credit_conversion_factor * undrawn_commitment);

-- Usage:
SELECT
    account_id,
    eu_bfsi_prod.utils.calculate_ead(
        outstanding_balance,
        undrawn_commitment,
        0.75  -- 75% CCF for revolving facilities
    ) AS ead_exposure
FROM eu_bfsi_prod.silver.silver_credit_facilities;

-- ─── 2. Table-valued SQL UDF ──────────────────
CREATE OR REPLACE FUNCTION eu_bfsi_prod.utils.get_account_transactions (
    p_account_id STRING,
    p_from_date  DATE,
    p_to_date    DATE
)
RETURNS TABLE (
    txn_id          STRING,
    txn_amount      DECIMAL(18,4),
    txn_currency    STRING,
    txn_date        DATE,
    txn_type        STRING
)
COMMENT 'Returns transactions for an account in a date range'
RETURN
SELECT txn_id, txn_amount, txn_currency, txn_date, txn_type
FROM eu_bfsi_prod.silver.silver_transactions
WHERE account_id = p_account_id
  AND txn_date BETWEEN p_from_date AND p_to_date;

-- Usage:
SELECT * FROM eu_bfsi_prod.utils.get_account_transactions(
    'ACC-EU-12345678', '2024-01-01', '2024-12-31'
);

-- ─── 3. Risk scoring UDF with CASE logic ──────
CREATE OR REPLACE FUNCTION eu_bfsi_prod.utils.calculate_risk_score (
    days_past_due       INT,
    debt_to_income      DECIMAL(5,4),
    credit_utilization  DECIMAL(5,4),
    num_recent_inquiries INT
)
RETURNS INT
COMMENT 'Simplified credit risk score 0-100. Higher = higher risk.'
RETURN
    -- Days past due component (40 pts max)
    CASE
        WHEN days_past_due = 0          THEN 0
        WHEN days_past_due <= 30        THEN 15
        WHEN days_past_due <= 60        THEN 25
        WHEN days_past_due <= 90        THEN 35
        ELSE                                 40
    END
    -- DTI component (30 pts max)
    + CASE
        WHEN debt_to_income < 0.35      THEN 0
        WHEN debt_to_income < 0.50      THEN 10
        WHEN debt_to_income < 0.65      THEN 20
        ELSE                                 30
    END
    -- Utilization component (20 pts max)
    + CASE
        WHEN credit_utilization < 0.30  THEN 0
        WHEN credit_utilization < 0.60  THEN 7
        WHEN credit_utilization < 0.80  THEN 14
        ELSE                                 20
    END
    -- Inquiries component (10 pts max)
    + LEAST(num_recent_inquiries * 2, 10);

-- ─── 4. EXECUTE IMMEDIATE — dynamic SQL ───────
-- Stored procedure-like pattern in Databricks
DECLARE table_name STRING DEFAULT 'silver_transactions';
DECLARE schema_name STRING DEFAULT 'silver';
DECLARE row_count BIGINT;

EXECUTE IMMEDIATE
    CONCAT('SELECT COUNT(*) FROM eu_bfsi_prod.', schema_name, '.', table_name,
           ' WHERE txn_date = current_date()')
    INTO row_count;

SELECT row_count AS todays_record_count;

-- ─── 5. Grant function access ─────────────────
GRANT EXECUTE ON FUNCTION eu_bfsi_prod.utils.calculate_ead
    TO `risk-analysts`, `compliance-officers`, `data-engineers`;

GRANT EXECUTE ON FUNCTION eu_bfsi_prod.utils.get_account_transactions
    TO `data-analysts`;

-- ─── 6. List all functions in schema ──────────
SHOW FUNCTIONS IN eu_bfsi_prod.utils;
```

**Enterprise Use Case:**  
Regulatory calculation library at a European bank's risk platform — SQL UDFs encapsulate EAD, LGD, and credit scoring formulas reviewed and approved by the Model Risk Management team. All 23 pipelines (credit risk, stress testing, COREP) call the same UDF, ensuring a single change to the CCF parameter updates all downstream calculations simultaneously.

**Architectural Invariants & Gotchas:**
- SQL UDFs in Unity Catalog are first-class securables — GRANT EXECUTE controls who can call them, enabling function-level access control
- Table-valued functions (RETURNS TABLE) are composable — use them in FROM clause like a table, enabling modular pipeline building
- SQL UDFs are inlined into the query plan when possible — no serialization overhead unlike Python UDFs which require inter-process communication
- EXECUTE IMMEDIATE enables parameterized dynamic SQL — use for metadata-driven pipeline frameworks where table/column names vary
- Register business-critical formulas (EAD, LGD, PD) as versioned SQL UDFs — ensures calculation consistency across 50+ pipelines accessing the same regulatory logic

---

### Q4.21: Data Quality Checks in SQL (Expectations, DQ Framework) (Data Quality)

**Question & Concept Overview:**  
Enterprise-grade data quality requires declarative rules, automated monitoring, and integration with pipeline execution. In Databricks, DQ is implemented via DLT expectations (declarative), SQL-based assertion tables, and Great Expectations-style check patterns. Financial regulatory data must meet SLA-bound quality thresholds — failed DQ can trigger pipeline halts and alert the data engineering team.

**Production Implementation:**
```python
-- =============================================
-- SQL Data Quality Framework
-- EU BFSI — Payment Data Quality Checks
-- =============================================

USE CATALOG eu_bfsi_prod;
USE SCHEMA governance;

-- ─── 1. DQ Rules Catalog Table ────────────────
CREATE TABLE IF NOT EXISTS dq_rules (
    rule_id         STRING NOT NULL,
    table_fqn       STRING NOT NULL COMMENT 'catalog.schema.table',
    rule_name       STRING NOT NULL,
    rule_type       STRING COMMENT 'NOT_NULL | RANGE | FORMAT | REFERENTIAL | CUSTOM_SQL',
    column_name     STRING,
    rule_sql        STRING NOT NULL COMMENT 'SQL expression that returns TRUE for VALID rows',
    severity        STRING COMMENT 'CRITICAL | HIGH | MEDIUM | LOW',
    threshold_pct   DECIMAL(5,2) COMMENT 'Acceptable fail % (e.g. 0.0 = zero tolerance)',
    owner           STRING,
    active          BOOLEAN DEFAULT TRUE
) USING DELTA;

-- Load rules
INSERT INTO dq_rules VALUES
('DQ-001', 'eu_bfsi_prod.silver.silver_transactions', 'txn_id_not_null',
 'NOT_NULL', 'txn_id', 'txn_id IS NOT NULL', 'CRITICAL', 0.0, 'data-eng@bank.eu', true),
('DQ-002', 'eu_bfsi_prod.silver.silver_transactions', 'positive_amount',
 'RANGE', 'txn_amount', 'txn_amount > 0', 'CRITICAL', 0.0, 'data-eng@bank.eu', true),
('DQ-003', 'eu_bfsi_prod.silver.silver_transactions', 'valid_currency',
 'FORMAT', 'txn_currency', "txn_currency RLIKE '^[A-Z]{3}$'", 'HIGH', 0.01, 'data-eng@bank.eu', true),
('DQ-004', 'eu_bfsi_prod.silver.silver_transactions', 'future_timestamp',
 'RANGE', 'txn_timestamp', 'txn_timestamp <= current_timestamp()', 'HIGH', 0.0, 'data-eng@bank.eu', true),
('DQ-005', 'eu_bfsi_prod.silver.silver_transactions', 'valid_txn_type',
 'FORMAT', 'txn_type', "txn_type IN ('DEBIT','CREDIT','FEE','REVERSAL','INTERNAL')", 'MEDIUM', 0.1, 'data-eng@bank.eu', true);

-- ─── 2. DQ Results Table ──────────────────────
CREATE TABLE IF NOT EXISTS dq_results (
    run_id          STRING NOT NULL,
    run_ts          TIMESTAMP NOT NULL,
    rule_id         STRING NOT NULL,
    table_fqn       STRING NOT NULL,
    total_rows      BIGINT,
    passing_rows    BIGINT,
    failing_rows    BIGINT,
    pass_rate_pct   DECIMAL(5,2),
    severity        STRING,
    threshold_pct   DECIMAL(5,2),
    dq_status       STRING COMMENT 'PASS | FAIL | WARN',
    sample_failures STRING COMMENT 'JSON sample of failed rows'
) USING DELTA;

-- ─── 3. DQ Check Execution (run per-table) ────
INSERT INTO dq_results
WITH base AS (
    SELECT
        '${run_id}'                             AS run_id,
        current_timestamp()                     AS run_ts,
        r.rule_id,
        r.table_fqn,
        r.severity,
        r.threshold_pct
    FROM dq_rules r
    WHERE r.table_fqn = 'eu_bfsi_prod.silver.silver_transactions'
      AND r.active = true
),
check_results AS (
    SELECT
        'DQ-001' AS rule_id,
        COUNT(*) AS total_rows,
        SUM(CASE WHEN txn_id IS NOT NULL THEN 1 ELSE 0 END) AS passing_rows,
        SUM(CASE WHEN txn_id IS NULL THEN 1 ELSE 0 END) AS failing_rows
    FROM eu_bfsi_prod.silver.silver_transactions
    WHERE txn_date = current_date()
    UNION ALL
    SELECT 'DQ-002', COUNT(*),
        SUM(CASE WHEN txn_amount > 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN txn_amount <= 0 THEN 1 ELSE 0 END)
    FROM eu_bfsi_prod.silver.silver_transactions WHERE txn_date = current_date()
    UNION ALL
    SELECT 'DQ-003', COUNT(*),
        SUM(CASE WHEN txn_currency RLIKE '^[A-Z]{3}$' THEN 1 ELSE 0 END),
        SUM(CASE WHEN NOT (txn_currency RLIKE '^[A-Z]{3}$') THEN 1 ELSE 0 END)
    FROM eu_bfsi_prod.silver.silver_transactions WHERE txn_date = current_date()
)
SELECT
    b.run_id,
    b.run_ts,
    b.rule_id,
    b.table_fqn,
    cr.total_rows,
    cr.passing_rows,
    cr.failing_rows,
    ROUND(100.0 * cr.passing_rows / cr.total_rows, 4) AS pass_rate_pct,
    b.severity,
    b.threshold_pct,
    CASE
        WHEN 100.0 * cr.failing_rows / cr.total_rows <= b.threshold_pct THEN 'PASS'
        WHEN b.severity = 'CRITICAL' THEN 'FAIL'
        ELSE 'WARN'
    END AS dq_status,
    NULL AS sample_failures
FROM base b
JOIN check_results cr ON b.rule_id = cr.rule_id;

-- ─── 4. DQ Dashboard Query ────────────────────
SELECT
    run_ts,
    rule_id,
    table_fqn,
    total_rows,
    failing_rows,
    pass_rate_pct,
    severity,
    dq_status
FROM dq_results
WHERE DATE(run_ts) = current_date()
ORDER BY dq_status DESC, severity, pass_rate_pct;
```

**Enterprise Use Case:**  
Regulatory data quality gate at a European bank's FINREP reporting pipeline — 47 SQL-based DQ rules run after each silver layer load; CRITICAL rule failures halt the gold layer job and alert the data engineering on-call via Teams webhook, preventing out-of-tolerance data from reaching the regulatory submission portal.

**Architectural Invariants & Gotchas:**
- DQ rules as data (not code) enables non-engineers to configure quality checks via a UI or spreadsheet import — accelerates governance adoption
- DLT EXPECT clauses are the most elegant option for streaming pipelines; SQL assertion tables work better for batch-oriented governance frameworks
- Threshold-based pass/fail (e.g., 0.01% fail rate allowed for optional fields) prevents false alarms from genuinely nullable columns
- Store sample failing rows as JSON in dq_results — enables root-cause analysis without re-running the check
- Integrate DQ results with Databricks Asset Bundles monitoring and PagerDuty/Teams alerts to close the incident response loop

---

### Q4.22: APPLY CHANGES INTO — DLT CDC Pattern (Delta Live Tables)

**Question & Concept Overview:**  
APPLY CHANGES INTO is a DLT-exclusive SQL command that implements CDC (Change Data Capture) processing. It consumes ordered change events from a source (insert/update/delete) and applies them to a target table with automatic ordering, deduplication, and SCD Type 1 or Type 2 handling. This is the most sophisticated streaming CDC pattern available in Databricks.

**Production Implementation:**
```python
-- =============================================
-- APPLY CHANGES INTO — DLT CDC Pipeline
-- EU BFSI — Core Banking Account CDC
-- =============================================
-- File: /pipeline/cdc_account_pipeline.sql
-- Pipeline mode: Enhanced Autoscaling, Triggered
-- =============================================

-- ─── SOURCE: Streaming CDC events from Debezium/Kafka ─
CREATE OR REFRESH STREAMING TABLE bronze_account_cdc
COMMENT 'Bronze — raw CDC events from Core Banking Debezium connector'
AS SELECT
    CAST(key AS STRING)                     AS account_id,
    get_json_object(CAST(value AS STRING), '$.op')          AS operation,
    get_json_object(CAST(value AS STRING), '$.ts_ms')       AS event_ts_ms,
    get_json_object(CAST(value AS STRING), '$.after.account_id')       AS after_account_id,
    get_json_object(CAST(value AS STRING), '$.after.account_type')     AS after_account_type,
    get_json_object(CAST(value AS STRING), '$.after.risk_tier')        AS after_risk_tier,
    get_json_object(CAST(value AS STRING), '$.after.kyc_status')       AS after_kyc_status,
    get_json_object(CAST(value AS STRING), '$.after.current_balance')  AS after_current_balance,
    get_json_object(CAST(value AS STRING), '$.after.account_status')   AS after_account_status,
    _metadata.timestamp                     AS kafka_ingested_ts
FROM cloud_files(
    'abfss://kafka-dump@euadlsprod.dfs.core.windows.net/core-banking-cdc/',
    'json'
);

-- ─── APPLY CHANGES INTO — SCD Type 1 (latest wins) ───
APPLY CHANGES INTO LIVE.silver_accounts_scd1
FROM STREAM(LIVE.bronze_account_cdc)
KEYS (account_id)
APPLY AS DELETE WHEN operation = 'd'
APPLY AS TRUNCATE WHEN operation = 't'
SEQUENCE BY CAST(event_ts_ms AS BIGINT)
COLUMNS * EXCEPT (operation, event_ts_ms, kafka_ingested_ts)
STORED AS SCD TYPE 1;
-- Result: One row per account — always reflects latest state

-- ─── APPLY CHANGES INTO — SCD Type 2 (full history) ──
APPLY CHANGES INTO LIVE.silver_accounts_scd2
FROM STREAM(LIVE.bronze_account_cdc)
KEYS (account_id)
APPLY AS DELETE WHEN operation = 'd'
SEQUENCE BY CAST(event_ts_ms AS BIGINT)
COLUMNS after_account_id, after_account_type, after_risk_tier,
        after_kyc_status, after_current_balance, after_account_status
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (after_current_balance);
-- after_current_balance excluded from history tracking (too frequent, not needed)
-- Result: Full history with __START_AT and __END_AT columns auto-managed

-- ─── Query SCD2 historical table ──────────────
-- (Auto-generated columns by DLT)
SELECT
    after_account_id    AS account_id,
    after_risk_tier     AS risk_tier,
    after_kyc_status    AS kyc_status,
    __START_AT          AS effective_from,
    __END_AT            AS effective_to,
    CASE WHEN __END_AT IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END AS record_status
FROM LIVE.silver_accounts_scd2
WHERE after_account_id = 'ACC-EU-98765431'
ORDER BY __START_AT;

-- ─── Gold: Point-in-time risk tier from SCD2 ──
SELECT
    txn_id,
    txn_amount,
    t.account_id,
    -- Get risk tier AS OF the transaction date
    a.after_risk_tier               AS risk_tier_at_txn_time
FROM eu_bfsi_prod.silver.silver_transactions t
JOIN eu_bfsi_prod.silver.silver_accounts_scd2 a
    ON t.account_id = a.after_account_id
    AND t.txn_timestamp BETWEEN a.__START_AT
        AND COALESCE(a.__END_AT, TIMESTAMP '9999-12-31 23:59:59')
WHERE t.txn_date = '2024-06-30';
```

**Enterprise Use Case:**  
Real-time Core Banking CDC pipeline at a European challenger bank — Debezium captures every account attribute change from PostgreSQL Core Banking, APPLY CHANGES INTO processes 500K daily CDC events into SCD2, enabling the risk team to query a customer's exact risk classification at any point in time for MiFID II suitability back-testing.

**Architectural Invariants & Gotchas:**
- APPLY CHANGES INTO is exclusive to DLT — it handles out-of-order events, deduplication, and SCD management automatically
- SEQUENCE BY column must be monotonically increasing per key — use Kafka offset, Debezium ts_ms, or database LSN (Log Sequence Number)
- SCD TYPE 2 auto-generates __START_AT and __END_AT columns — no manual effective date management required
- TRACK HISTORY ON * EXCEPT excludes frequently changing columns (like balances) from SCD2 versioning — prevents version explosion for operational metrics
- APPLY CHANGES INTO requires the target table to be a LIVE table (DLT-managed) — it cannot be used with regular Delta tables outside DLT

---

### Q4.23: TrueTime Drift Uncertainty Bounds and GPS Lock Failure during Leap Second Smearing (Lakehouse Architecture)

**Question & Concept Overview:**  
In a globally distributed multi-region transactional engine spanning Virginia (us-east-1), Frankfurt (eu-central-1), and Tokyo (ap-northeast-1), your cluster relies on an atomic TrueTime-style API providing time intervals [earliest, latest] where uncertainty is bounded by 2*epsilon. During a planned solar geomagnetic disturbance, redundant GPS receivers across Frankfurt lose satellite lock, falling back to local oven-controlled crystal oscillators (OCXO) with a drift rate of 1 microsecond per second, driving epsilon from 200 microseconds to 12 milliseconds over a 3-hour period. Because strict external consistency (linearizability) requires transactions to commit-wait for 2*epsilon before releasing write locks, p99 commit latency explodes from 800 microseconds to 24 milliseconds, causing severe lock contention on primary account balance rows and cascading connection pool exhaustion across all regional ingress proxies. How do you re-architect the commit-wait epoch and lease allocation mechanism to maintain linearizable snapshot isolation without degrading write throughput to zero, given the physical speed-of-light round-trip time between Frankfurt and Tokyo is 112ms?

**Enterprise Use Case:**  
Enterprise Lakehouse Scale

**Architectural Invariants & Gotchas:**
- 

---

### Q4.24: Asymmetric Half-Open TCP Partition Cascades and Raft Leader Lease Flapping (Lakehouse Architecture)

**Question & Concept Overview:**  
A 5-node geo-distributed metadata cluster deployed across North America, Europe, and Asia uses Raft with leader leases (lease duration = 2,000ms, heartbeat interval = 300ms) to serve 150,000 metadata lookups per second. A malfunctioning border gateway router in Ashburn begins silently dropping egress TCP packets while continuing to forward ingress TCP ACK packets, creating an asymmetric network partition: Leader Node A can receive heartbeats and append-entries responses from Followers B and C, but B and C cannot receive Leader A's heartbeat updates. Followers B and C experience heartbeat election timeouts (1,500ms) and increment their term, broadcasting `RequestVote` RPCs. Because ingress still functions, Leader A receives these higher-term votes, steps down to follower, and aborts in-flight client writes. However, neither B nor C can establish a majority quorum due to transatlantic cross-partition packet drops, causing continuous term escalation up to Term 45,000. When the router temporarily clears its route table, the hyper-accelerated term flood invalidates valid leases across all surviving nodes. How do you architect a consensus transport and lease renewal protocol that prevents term inflation cascades and preserves read availability during asymmetric routing failures?

**Enterprise Use Case:**  
Enterprise Lakehouse Scale

**Architectural Invariants & Gotchas:**
- 

---

### Q4.25: Direct I/O Zero-Copy WAL Ring-Buffer Alignment over Gen5 NVMe Microsecond Jitter (Lakehouse Architecture)

**Question & Concept Overview:**  
You are optimizing a low-latency distributed state machine engine processing 2,000,000 write operations per second across an array of enterprise PCIe Gen5 x4 NVMe SSDs (U.2 form factor, raw sequential write throughput: 14 GB/s, 4KB random write IOPS: 3.2M). The consensus engine writes to an append-only Write-Ahead Log (WAL) using Direct I/O (`O_DIRECT | O_DSYNC`). Profiling reveals that while the p50 write latency is 18 microseconds, the p99.99 latency spikes violently to 14.8 milliseconds. Kernel tracing with eBPF reveals two root causes: (1) memory buffers passed to `pwrite64` occasionally cross 64-byte L3 cache lines and unaligned 4096-byte memory page boundaries, forcing NVMe driver split-DMA transfers; and (2) internal SSD controller NAND flash garbage collection (GC) block-erase cycles stall controller write queues when write amplification factor (WAF) reaches 3.2. How do you architect the user-space WAL ring buffer allocator and NVMe I/O submission path to guarantee strict sub-50 microsecond p99.99 write latency without losing synchronous durability?

**Enterprise Use Case:**  
Enterprise Lakehouse Scale

**Architectural Invariants & Gotchas:**
- 

---
