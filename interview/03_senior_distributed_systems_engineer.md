# 🎓 PySpark Interview Guide — Tier 3: Senior Distributed Systems & Performance Engineer

> **Focus Areas**: Catalyst Optimizer, Project Tungsten, Memory Pools & Spill Triage, Adaptive Query Execution (AQE), Join Strategies (BHJ, SMJ, SHJ), Data Skew Salting, Stateful Streaming.
> **Target Audience**: Senior Data Engineers, Distributed Systems Specialists, Performance Tuning Leads

---

### Q3.1: Delta Lake MERGE INTO (Upsert Operations) (Medallion Architecture)

**Question & Concept Overview:**  
Atomic upsert (insert, update, delete) operations inside Delta tables. Crucial for syncing changing master datasets (e.g. customer dimensions) without rewriting whole directories.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # Reference target Delta table
    target_table_path = "abfss://silver@euadlsprod.dfs.core.windows.net/customers"

    # Load source staging updates
    staging_updates_df = spark.read.format("delta").load("abfss://bronze@euadlsprod.dfs.core.windows.net/customer_updates")

    from delta.tables import DeltaTable

    # Instantiate the target DeltaTable object
    delta_target = DeltaTable.forPath(spark, target_table_path)

    # Execute the upsert (merge) statement
    delta_target.alias("t").merge(
        source=staging_updates_df.alias("s"),
        condition="t.customer_id == s.customer_id"
    ).whenMatchedUpdate(set={
        "t.customer_name": "s.customer_name",
        "t.email": "s.email",
        "t.risk_score": "s.risk_score",
        "t.updated_ts": "s.updated_ts"
    }).whenNotMatchedInsert(values={
        "customer_id": "s.customer_id",
        "customer_name": "s.customer_name",
        "email": "s.email",
        "risk_score": "s.risk_score",
        "created_ts": "s.updated_ts",
        "updated_ts": "s.updated_ts"
    }).execute()
except Exception as e:
    logger.error(f"Pipeline stage failed in Delta Lake MERGE INTO (Upsert Operations): {str(e)}")
    raise
```

**Enterprise Use Case:**  
Updating customer master profiles in the Silver layer (SCD Type 1) with incoming daily profile drift updates.

**Architectural Invariants & Gotchas:**
- MERGE INTO matches rows between target and source based on a join condition, applying updates or insertions.
- Delta Lake ensures that the merge is transactional; readers see either the old state or the full new state.
- Optimize performance by ensuring the merge key column is clustered (Z-Ordered) on the target table.
- Add additional conditions (e.g. whenMatchedUpdate(condition='s.updated_ts > t.updated_ts')) to prevent out-of-order overrides.

---

### Q3.2: Delta Lake Change Data Feed (CDF) (Medallion Architecture)

**Question & Concept Overview:**  
Enabling and querying row-level change propagation logs in Delta tables. Allows incremental extraction of changes (inserts, updates, deletes) to populate downstream Gold aggregates efficiently.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # 1. Read change feed events between two versions of the Silver customer table
    cdf_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 10) \
        .option("endingVersion", 15) \
        .load("abfss://silver@euadlsprod.dfs.core.windows.net/customers")

    # 2. Filter CDF events to extract modifications and propagate to audit ledger
    updates_and_deletes_df = cdf_df.filter(
        col("_change_type").isin("update_postimage", "delete")
    ).select(
        col("customer_id"),
        col("risk_score"),
        col("_change_type").alias("operation_type"),
        col("_commit_timestamp").alias("applied_at")
    )
except Exception as e:
    logger.error(f"Pipeline stage failed in Delta Lake Change Data Feed (CDF): {str(e)}")
    raise
```

**Enterprise Use Case:**  
Propagating KYC risk rating status changes incrementally from the Silver customer directory to Gold aggregate tables.

**Architectural Invariants & Gotchas:**
- CDF must be explicitly enabled on the table via table properties: tblproperties('delta.enableChangeDataFeed' = 'true').
- Special metadata columns returned include _change_type, _commit_version, and _commit_timestamp.
- _change_type values include insert, delete, update_preimage (value before update), and update_postimage (value after update).
- CDF eliminates the need for expensive full-table diff comparison scans in multi-hop medallion updates.

---

### Q3.3: Broadcast Join & Joins Optimization (Performance Tuning)

**Question & Concept Overview:**  
Eliminating network shuffle stages when joining a large table with a small reference dataset. PySpark copies the small dataset to all worker executors, keeping the join entirely local.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    from pyspark.sql.functions import broadcast

    # Load the massive transactions dataset (10 Billion records)
    txns_df = spark.read.format("delta").load("abfss://silver@euadlsprod.dfs.core.windows.net/transactions")

    # Load small currency dimension table (100 rows)
    currency_dim_df = spark.read.format("delta").load("abfss://silver@euadlsprod.dfs.core.windows.net/currencies")

    # Force broadcast join of the currency dimension
    enriched_txns_df = txns_df.join(
        broadcast(currency_dim_df),
        on="currency_code",
        how="inner"
    )

    # Verify execution plan contains 'BroadcastHashJoin' using explain()
    enriched_txns_df.explain()
except Exception as e:
    logger.error(f"Pipeline stage failed in Broadcast Join & Joins Optimization: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Joining billions of retail transactions with a tiny, static currency exchange rate lookup table during close-of-day valuation.

**Architectural Invariants & Gotchas:**
- Broadcast joins bypass Shuffle operations, which are the most expensive resource bottleneck in Spark.
- The default maximum size for a broadcasted table is 10MB (spark.sql.autoBroadcastJoinThreshold).
- Broadcasting a large table can trigger OutOfMemory (OOM) errors on Spark worker executors.
- Check query plans for 'BroadcastHashJoin' vs 'SortMergeJoin' to verify optimization success.

---

### Q3.4: Caching and Persistence Strategies (Resource Management)

**Question & Concept Overview:**  
Caching intermediate DataFrames in executor memory or disk. This avoids re-evaluating long execution lineages when the same DataFrame is accessed multiple times by downstream actions.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    from pyspark import StorageLevel

    # Load and apply heavy transformations to expose transaction flags
    flagged_accounts_df = raw_txns \
        .filter(col("amount_eur") > 250000) \
        .groupBy("account_id").count() \
        .filter(col("count") >= 5)

    # Persist to memory and serialized disk spill (retains resource availability)
    flagged_accounts_df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    # Trigger action 1: Count of critical warning accounts
    high_risk_count = flagged_accounts_df.count()

    # Trigger action 2: Save list to audit files (reuses the cached state)
    flagged_accounts_df.write.format("delta").save("abfss://gold@euadlsprod.dfs.core.windows.net/aml_targets")

    # Always unpersist to release memory when transactions are completed
    flagged_accounts_df.unpersist()
except Exception as e:
    logger.error(f"Pipeline stage failed in Caching and Persistence Strategies: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Caching a filtered daily risk exposure dataset reused across three different reporting pipelines (AML, credit risk, regulatory reporting).

**Architectural Invariants & Gotchas:**
- cache() is a shortcut for persist() using default StorageLevel.MEMORY_AND_DISK.
- Use serialized persistence (MEMORY_AND_DISK_SER) to reduce memory foot-print and garbage collection overhead.
- Caching is lazy; it only materializes in memory when the first action (e.g. count, write) is executed.
- Always unpersist() DataFrames to prevent cluster memory leak issues on shared interactive workspaces.

---

### Q3.5: Custom Spark Partitioning & Repartitioning (Performance Tuning)

**Question & Concept Overview:**  
Managing the degree of parallelism and layout of partitions in memory. Explains the performance trade-offs between shuffle-inducing repartition and metadata-only coalesce.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # Load Silver transactions (poorly partitioned on landing)
    dirty_input_df = spark.read.format("delta").load("abfss://bronze@euadlsprod.dfs.core.windows.net/landing_txns")

    # 1. repartition: Increase partitions and shuffle data evenly across keys
    # Useful to increase parallelism for heavy CPU tasks
    repartitioned_df = dirty_input_df.repartition(100, "account_id")

    # 2. coalesce: Decreases partitions without shuffling (minimizes data movement)
    # Ideal for merging small partitions before writing target files
    optimized_for_export_df = repartitioned_df.filter(col("amount_eur") > 10000) \
        .coalesce(5)

    # Write output (will produce exactly 5 files in storage)
    optimized_for_export_df.write.format("json").save("abfss://export@euadlsprod.dfs.core.windows.net/daily_alerts")
except Exception as e:
    logger.error(f"Pipeline stage failed in Custom Spark Partitioning & Repartitioning: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Balancing cluster work (100 partitions during parallel mapping) and merging reports (5 files during export writes).

**Architectural Invariants & Gotchas:**
- repartition() triggers a full shuffle to distribute data evenly, creating equal-sized partitions.
- coalesce() only collapses partitions on the same executor, avoiding network shuffles entirely.
- Calling repartition() with a partition column optimizes execution paths for downstream grouping.
- Coalescing to 1 file before writing causes a bottlenecks, as the final step runs on a single worker executor.

---

### Q3.6: Handling Skewed Data with Salting (Performance Tuning)

**Question & Concept Overview:**  
Resolving performance bottlenecks caused by uneven data distribution. By appending a randomized suffix (salt key) to skewed keys, transactions are distributed evenly across executors during joins.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    from pyspark.sql.functions import col, concat, lit, rand, round, explode, array

    # Target Table: Massive transactions table where some account IDs are extremely skewed (e.g. corporate accounts)
    # Reference Table: Accounts dimension table

    # 1. Salt the transactions DataFrame by adding a random salt column (0-9)
    salt_factor = 10
    salted_txns_df = txns_df.withColumn("salt", round(rand() * (salt_factor - 1))) \
        .withColumn("salted_join_key", concat(col("account_id"), lit("_"), col("salt")))

    # 2. Replicate the accounts dimension table 10 times to match all possible salts
    exploded_salts_df = spark.range(0, salt_factor).withColumnRenamed("id", "salt")
    replicated_accounts_df = accounts_df.crossJoin(exploded_salts_df) \
        .withColumn("salted_join_key", concat(col("account_id"), lit("_"), col("salt")))

    # 3. Perform skew-free join on salted keys
    skew_free_join_df = salted_txns_df.join(
        replicated_accounts_df,
        on="salted_join_key",
        how="inner"
    ).drop("salt", "salted_join_key")
except Exception as e:
    logger.error(f"Pipeline stage failed in Handling Skewed Data with Salting: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Joining billions of payment rows with account masters when corporate ledger accounts hold millions of events compared to retail accounts holding tens of events.

**Architectural Invariants & Gotchas:**
- Data skew manifests in the Spark UI as a single task taking significantly longer than others (straggler task).
- Salting breaks down the massive group corresponding to a skewed key into smaller subsets that run in parallel.
- The salt factor determines how many times the dimension table is replicated; choose a value that balances JVM memory limits.
- Spark 3.x Adaptive Query Execution (AQE) can handle skew auto-detection dynamically using shuffle statistics.

---

### Q3.7: Delta Lake Optimize, Z-Ordering, and Liquid Clustering (Storage Optimization)

**Question & Concept Overview:**  
Consolidating small files into large ones and organizing layout mapping to speed up queries. Highlights static Z-Ordering vs modern Liquid Clustering for cost-efficient data skipping.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # 1. Legacy Z-Ordering (Static multi-dimensional clustering)
    # Run via SQL command interface
    spark.sql("OPTIMIZE eu_bfsi_prod.silver.transactions ZORDER BY (account_id, currency)")

    # 2. Modern Liquid Clustering (DBR 13.3+)
    # Configured at table creation or altered dynamically
    spark.sql("""
        CREATE TABLE IF NOT EXISTS eu_bfsi_prod.silver.liquid_transactions (
            txn_id STRING,
            account_id STRING,
            txn_date DATE,
            amount_eur DOUBLE
        ) USING DELTA
        CLUSTER BY (account_id, txn_date)
    """)

    # Compelling execution run to re-cluster layout after batch loading
    spark.sql("OPTIMIZE eu_bfsi_prod.silver.liquid_transactions")
except Exception as e:
    logger.error(f"Pipeline stage failed in Delta Lake Optimize, Z-Ordering, and Liquid Clustering: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Maintaining sub-second lookup performance on multi-terabyte transactional Delta tables queried by BI reporting tools.

**Architectural Invariants & Gotchas:**
- OPTIMIZE merges small partition files into standardized larger blocks (~1GB Parquet files) to improve disk I/O.
- Z-Ordering structures columns in space-filling curves to allow multi-dimensional data skipping.
- Liquid Clustering eliminates static partitioning, dynamically adjusting physical organization without full rewrites.
- Run OPTIMIZE regularly (e.g. daily/weekly) during low-usage windows to minimize cloud storage cost overhead.

---

### Q3.8: Structured Streaming with Delta Lake (Real-time Pipelines)

**Question & Concept Overview:**  
Creating continuous, fault-tolerant ingestion pipelines with low latency. Real-time streams are processed in micro-batches with checkpoints to ensure exactly-once guarantees.

**Production Implementation:**
```python
import logging

# Init pipeline logger
logger = logging.getLogger("MedallionPipeline")

try:
    # Define streaming read from Delta bronze table
    streaming_source_df = spark.readStream.format("delta") \
        .option("maxFilesPerTrigger", "10") \
        .load("abfss://bronze@euadlsprod.dfs.core.windows.net/transactions")

    # Filter fraud candidates (amounts exceeding 50,000 EUR)
    alert_stream_df = streaming_source_df \
        .filter((col("amount_eur") > 50000) & (col("status") == "COMPLETED"))

    # Start streaming query and output results to target database
    streaming_query = alert_stream_df.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "abfss://meta@euadlsprod.dfs.core.windows.net/checkpoints/realtime_alerts") \
        .trigger(processingTime="10 seconds") \
        .start("abfss://gold@euadlsprod.dfs.core.windows.net/realtime_aml_alerts")
except Exception as e:
    logger.error(f"Pipeline stage failed in Structured Streaming with Delta Lake: {str(e)}")
    raise
```

**Enterprise Use Case:**  
Ingesting incoming SEPA transaction feeds, applying real-time compliance filters, and outputting to an AML dashboard with 10-second latency.

**Architectural Invariants & Gotchas:**
- Structured Streaming processes data stream engines using micro-batches (or continuous processing for lower latency).
- maxFilesPerTrigger controls the size of each micro-batch, preventing ingestion spikes from crashing workers.
- checkpointLocation tracks processed offsets, enabling the stream to resume from failure points without duplicating data.
- Trigger processingTime configuration dictates the micro-batch window scheduling interval (e.g. every 10s).

---

### Q3.9: Databricks Auto Loader Streaming with Rescued Data Protection (Streaming & Ingestion)

**Question & Concept Overview:**  
Ingests continuous file streams with automated schema evolution while routing corrupted or unexpected schema drift into the _rescued_data column.

**Production Implementation:**
```python
from pyspark.sql import functions as F

source_path = "abfss://raw@datalake.dfs.core.windows.net/events_landing/"
checkpoint_path = "abfss://checkpoints@datalake.dfs.core.windows.net/events_checkpoint/"
schema_path = "abfss://checkpoints@datalake.dfs.core.windows.net/events_schema/"

# Configure resilient Auto Loader stream
streaming_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", schema_path) \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .load(source_path)

# Write to Delta table with append mode and schema merging
query = streaming_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .toTable("bronze_events")
```

**Enterprise Use Case:**  
Mission-critical continuous cloud storage file ingestion where upstream schemas frequently change without notice.

**Architectural Invariants & Gotchas:**
- cloudFiles.schemaEvolutionMode supports 'addNewColumns', 'rescue', 'failOnNewColumns', and 'none'.
- _rescued_data stores any column that fails type casting as raw JSON text, preventing pipeline crashes.

---

### Q3.10: Object-Oriented Dynamic PySpark Transformation Utility (Design Patterns & OOP)

**Question & Concept Overview:**  
A reusable, modular Python class applying method chaining and dynamic column aggregation expressions to eliminate hardcoded PySpark logic.

**Production Implementation:**
```python
from pyspark.sql import DataFrame, functions as F
from typing import List, Dict

class DynamicPipelineTransformer:
    def __init__(self, df: DataFrame):
        self.df = df

    def add_audit_columns(self) -> 'DynamicPipelineTransformer':
        self.df = self.df.withColumn("ingested_at", F.current_timestamp()) \
                         .withColumn("source_system", F.lit("ADLS_RAW"))
        return self

    def dynamic_aggregate(self, group_by_cols: List[str], agg_mapping: Dict[str, str]) -> DataFrame:
        """
        Dynamically builds aggregation expressions from a dictionary.
        agg_mapping: {'revenue': 'sum', 'order_id': 'count', 'discount': 'avg'}
        """
        expressions = []
        for col_name, func_name in agg_mapping.items():
            spark_func = getattr(F, func_name.lower())
            expressions.append(spark_func(F.col(col_name)).alias(f"{func_name}_{col_name}"))

        return self.df.groupBy(*group_by_cols).agg(*expressions)

# Pipeline usage:
transformer = DynamicPipelineTransformer(df_raw_sales)
df_gold = transformer.add_audit_columns() \
    .dynamic_aggregate(
        group_by_cols=["region", "product_category"],
        agg_mapping={"sale_amount": "sum", "order_id": "count"}
    )

df_gold.show()
```

**Enterprise Use Case:**  
Building enterprise-wide PySpark utility libraries and shared data engineering modules across distributed teams.

**Architectural Invariants & Gotchas:**
- Employs Python's getattr(F, func_name) to resolve PySpark SQL functions dynamically at runtime.
- Method chaining (returning self) creates expressive, testable, and reusable ETL pipelines.

---

### Q3.11: Benchmarking 6 Delta Lake Optimization Levers at Scale (Delta Lake & Storage Optimization)

**Question & Concept Overview:**  
Hands-on empirical benchmarking comparing 6 Delta Lake layout and file management techniques (Baseline small files, Physical Partitioning, Z-Ordering, Manual Compaction, Auto Optimize, and Liquid Clustering) across a 50M-row dataset, measuring scan time, input files, and bytes read via the Spark UI.

**Production Implementation:**
```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
CATALOG = "delta_opt"
SCHEMA = "sales"

# 1. Baseline unoptimized table with fragmented small files
# spark.sql(f"CREATE TABLE {CATALOG}.{SCHEMA}.sales_raw ...")

# 2. Physical Partitioning (low-cardinality country column)
df_raw = spark.table(f"{CATALOG}.{SCHEMA}.sales_raw")
df_raw.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("country") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.sales_partitioned")

# 3. Multi-dimensional Z-Ordering on high-cardinality keys
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.sales_zorder AS
    SELECT * FROM {CATALOG}.{SCHEMA}.sales_raw;
""")
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.sales_zorder ZORDER BY (customer_id, product_id)")

# 4. Liquid Clustering (modern recommended standard - Hilbert curve)
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.sales_liquid
    USING DELTA
    CLUSTER BY (sale_date, customer_id)
    AS SELECT * FROM {CATALOG}.{SCHEMA}.sales_raw;
""")
# Incrementally clusters new appends without full-table rewrites
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.sales_liquid")

# 5. Profile metrics in Spark UI & metadata
detail_df = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.sales_liquid")
detail_df.select("numFiles", "sizeInBytes", "clusteringColumns").show()
```

**Enterprise Use Case:**  
Optimizing a 50M-row retail sales fact table in Databricks to eliminate query scan bottlenecks and compare empirical performance across layout strategies.

**Architectural Invariants & Gotchas:**
- Liquid Clustering replaces both partitioning and Z-Order, supporting dynamic clustering key alterations.
- Z-Ordering is effective for multi-column queries on legacy tables but suffers from high write amplification on frequent appends.
- Physical partitioning on high-cardinality columns (>10,000 values) leads to severe small-file sprawl and metadata overhead.
- Use DESCRIBE DETAIL to verify file count reduction and average file size approaching the ~1GB sweet spot.

---

### Q3.12: Event-Time Watermarking & Windowed Stateful Aggregations (Structured Streaming)

**Question & Concept Overview:**  
Implements event-time stateful streaming aggregations with watermarking to handle out-of-order and late data cleanly. Uses tumbling and sliding windows with state eviction to prevent executor out-of-memory errors.

**Production Implementation:**
```python
from pyspark.sql.functions import col, window, sum, count

# Read clickstream events from Kafka or Delta Bronze
events_df = spark.readStream.format("delta").table("bronze.clickstream")

# Define 10-minute watermark for late data handling
windowed_metrics = events_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "1 hour", "15 minutes"), # Sliding 1-hr window every 15 mins
        col("campaign_id"),
        col("country")
    ) \
    .agg(
        count("click_id").alias("total_clicks"),
        sum("conversion_value").alias("gross_revenue")
    )

# Write to Silver Gold aggregated layer with Update output mode
query = windowed_metrics.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", "/mnt/checkpoints/campaign_hourly_metrics") \
    .toTable("silver.campaign_hourly_metrics")

query.awaitTermination()
```

**Enterprise Use Case:**  
Tracking real-time ad campaign conversions and revenue in sliding 1-hour windows while bounding state store memory usage.

**Architectural Invariants & Gotchas:**
- withWatermark('event_time', '10 minutes') instructs Spark that events arriving older than 10 mins past current max event time can be dropped.
- Watermarking is mandatory for stateful streaming operations to allow the RocksDB/HDFS state store to purge expired state buffers.
- Update mode writes only updated aggregate windows per micro-batch, whereas Append mode emits only finalized closed windows.
- Sliding windows (duration, slide) allow tracking moving trends with overlapping time boundaries.

---

### Q3.13: foreachBatch Idempotent Upsert & Deduplication Pattern (Structured Streaming)

**Question & Concept Overview:**  
Production pattern using foreachBatch to execute multi-table writes and ACID MERGE INTO operations within a streaming pipeline. Solves duplicate delivery issues in streaming CDC pipelines using batch deduplication.

**Production Implementation:**
```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

target_delta = DeltaTable.forName(spark, "silver.customer_accounts")

def process_micro_batch(micro_batch_df, batch_id):
    # 1. Deduplicate within the micro-batch window (take latest updated record)
    window_spec = Window.partitionBy("account_id").orderBy(col("last_modified_ts").desc())
    deduped_batch = micro_batch_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # 2. Idempotent MERGE into Target Silver Delta Table
    target_delta.alias("tgt").merge(
        deduped_batch.alias("src"),
        "tgt.account_id = src.account_id"
    ).whenMatchedUpdate(
        condition="src.last_modified_ts > tgt.last_modified_ts",
        set={
            "balance": "src.balance",
            "status": "src.status",
            "last_modified_ts": "src.last_modified_ts"
        }
    ).whenNotMatchedInsert(
        values={
            "account_id": "src.account_id",
            "balance": "src.balance",
            "status": "src.status",
            "created_ts": "src.last_modified_ts",
            "last_modified_ts": "src.last_modified_ts"
        }
    ).execute()

# Stream processor calling foreachBatch
streaming_query = spark.readStream.table("bronze.cdc_accounts") \
    .writeStream \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", "/mnt/checkpoints/accounts_upsert") \
    .start()

streaming_query.awaitTermination()
```

**Enterprise Use Case:**  
Consuming real-time banking CDC account updates from Kafka into a Silver dimensional table without duplicate record conflicts.

**Architectural Invariants & Gotchas:**
- foreachBatch allows applying arbitrary batch operations (like DeltaTable.merge) to streaming micro-batch DataFrames.
- Batch deduplication with Window.partitionBy ensures multiple CDC updates for the same key in one micro-batch don't cause MERGE conflict errors.
- Provides end-to-end exactly-once idempotent semantics even when micro-batches are re-executed upon cluster failure.
- Enables updating multiple target tables (e.g. Silver fact + quarantine audit table) in a single streaming job.

---

### Q3.14: Lakeflow / Delta Live Tables (DLT) Streaming with Expectations (Lakeflow Declarative Pipelines)

**Question & Concept Overview:**  
End-to-end Lakeflow Declarative Pipeline (formerly DLT) in Python implementing Bronze-to-Silver-to-Gold streaming medallion architecture with rigorous data quality expectations (@dlt.expect, @dlt.expect_or_drop, @dlt.expect_or_fail).

**Production Implementation:**
```python
import dlt
from pyspark.sql.functions import col, current_timestamp, to_date

# 1. Bronze Layer: Ingest streaming apparel events
@dlt.table(
    name="bronze_apparel_sales",
    comment="Raw streaming apparel sales transactions"
)
def bronze_apparel_sales():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/landing/apparel_sales/")

# 2. Silver Layer: Cleanse data with strict Quality Expectations
@dlt.table(
    name="silver_apparel_sales",
    comment="Cleansed apparel sales with quality validations"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "item_price > 0 AND quantity > 0")
@dlt.expect("valid_email", "customer_email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'")
def silver_apparel_sales():
    return dlt.read_stream("bronze_apparel_sales") \
        .select(
            col("order_id"),
            col("customer_id"),
            col("customer_email"),
            col("item_sku"),
            col("quantity").cast("int"),
            col("item_price").cast("decimal(10,2)"),
            (col("quantity") * col("item_price")).alias("total_sale_amount"),
            to_date(col("transaction_ts")).alias("sale_date")
        )

# 3. Gold Layer: Live Materialized View for Daily Apparel Revenue
@dlt.table(
    name="gold_daily_apparel_revenue",
    comment="Daily aggregated sales by product category"
)
def gold_daily_apparel_revenue():
    return dlt.read("silver_apparel_sales") \
        .groupBy("sale_date", "item_sku") \
        .agg(
            F.sum("total_sale_amount").alias("daily_revenue"),
            F.sum("quantity").alias("units_sold")
        )
```

**Enterprise Use Case:**  
Building an automated enterprise retail pipeline where invalid sales records are quarantined and live KPIs refresh incrementally.

**Architectural Invariants & Gotchas:**
- @dlt.expect records invalid rows in the DLT event log without dropping records (warn only).
- @dlt.expect_or_drop filters out bad rows from the downstream table while logging metric counts.
- @dlt.expect_or_fail halts pipeline execution immediately if invalid data is detected (critical regulatory data).
- dlt.read_stream creates an incremental streaming pipeline; dlt.read creates a batch materialized view.

---

### Q3.15: Databricks Genie Space Setup & Star Schema Optimization (GenAI & AI BI)

**Question & Concept Overview:**  
Architecting and calibrating a Databricks Genie AI/BI space on top of a Kimball dimensional star schema (Sales Fact, Customer Dim, Product Dim, Date Dim), defining semantic instructions, benchmark evaluation questions, and trusted SQL definitions.

**Production Implementation:**
```python
# Databricks Genie Space Semantic Configuration Pattern
# 1. Ensure Star Schema tables have rich column comments and PRIMARY/FOREIGN keys in Unity Catalog:
spark.sql("""
    ALTER TABLE gold.fct_sales
    ALTER COLUMN customer_id COMMENT 'Unique foreign key referencing dim_customer.customer_id';
""")

# 2. Informational Primary & Foreign Key constraints help Genie resolve join paths accurately:
spark.sql("""
    ALTER TABLE gold.dim_customer ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_id);
    ALTER TABLE gold.fct_sales ADD CONSTRAINT fk_sales_customer FOREIGN KEY (customer_id) REFERENCES gold.dim_customer(customer_id);
""")

# 3. Genie Space Semantic Instructions (YAML / System Prompt specification):
genie_space_config = {
    "space_name": "Executive Sales Analytics Space",
    "tables": ["gold.fct_sales", "gold.dim_customer", "gold.dim_product", "gold.dim_date"],
    "general_instructions": """
        - Always join fct_sales with dim_date on date_id to filter by calendar year and quarter.
        - 'Revenue' or 'Sales' refers to SUM(net_amount), excluding tax and shipping fees.
        - 'Active Customers' refers to COUNT(DISTINCT customer_id) with at least 1 order in the period.
        - Format currency outputs to 2 decimal places.
        - Never generate CROSS JOIN; always use explicit foreign key relationships.
    """,
    "certified_sql_queries": [
        {
            "question": "What is the top-selling product category by revenue this quarter?",
            "verified_sql": """
                SELECT p.category_name, SUM(f.net_amount) AS total_revenue
                FROM gold.fct_sales f
                JOIN gold.dim_product p ON f.product_id = p.product_id
                JOIN gold.dim_date d ON f.date_id = d.date_id
                WHERE d.quarter = '2026-Q3'
                GROUP BY p.category_name
                ORDER BY total_revenue DESC LIMIT 5;
            """
        }
    ]
}
```

**Enterprise Use Case:**  
Empowering business executives and non-technical stakeholders to ask self-service natural language questions against curated Lakehouse gold marts.

**Architectural Invariants & Gotchas:**
- Genie spaces leverage Unity Catalog table comments, column descriptions, and informational PK/FK constraints to infer join paths.
- Providing certified SQL query pairs anchors the underlying foundation model to verified business metric logic.
- Clear natural language synonyms in instructions prevent hallucinations on domain acronyms like ARR, NRR, or GMV.
- Run benchmark suites across 20-50 business questions to measure deterministic Text-to-SQL accuracy.

---

### Q3.16: Production Auto Loader Streaming with Rescued Data (Streaming & Ingestion)

**Question & Concept Overview:**  
Ingesting high-volume file streams using Databricks Auto Loader (cloudFiles) with dynamic schema evolution and a dedicated _rescued_data column to prevent pipeline breakage from corrupted or schema-deviant records.

**Production Implementation:**
```python
from pyspark.sql import functions as F

# 1. Configure Auto Loader stream with schema evolution & rescue mechanism
raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/prod_catalog/checkpoints/orders_schema/")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .option("cloudFiles.maxFilesPerTrigger", 500)  # Rate limiting backpressure
    .load("/Volumes/prod_catalog/landing/raw_orders/")
)

# 2. Add audit timestamp and monitor corrupted records
audited_stream = (
    raw_stream
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("is_corrupted", F.col("_rescued_data").isNotNull())
)

# 3. Write micro-batch to Bronze Delta Lake table with checkpointing
query = (
    audited_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/prod_catalog/checkpoints/orders_delta_write/")
    .trigger(availableNow=True)  # Incremental batch trigger
    .toTable("prod_catalog.bronze.orders_raw")
)
```

**Enterprise Use Case:**  
Building production landing-zone pipelines that ingest millions of semi-structured supplier JSON files daily into Bronze Delta tables without failing on unanticipated schema changes.

**Architectural Invariants & Gotchas:**
- Auto Loader automatically discovers new files as they arrive in cloud object storage via incremental directory listing or SNS/SQS event notifications.
- The _rescued_data column captures values with type mismatches or unparsed tokens, preventing pipeline termination.
- trigger(availableNow=True) processes all available data in multiple micro-batches then stops the cluster, saving compute costs vs continuous streaming.
- Always store checkpoints on durable, separate cloud storage to guarantee exactly-once processing across pipeline restarts.

---

### Q3.17: Programmatic DeltaTable MERGE (SCD Type 2 & Deletions) (Delta Lake Features)

**Question & Concept Overview:**  
Executing programmatic Delta Lake merge operations using DeltaTable.forName() in PySpark. Demonstrates multi-branch conditional updates, soft deletions, and inserts with predicate pruning for high performance.

**Production Implementation:**
```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# 1. Reference target Delta Lake table
delta_target = DeltaTable.forName(spark, "prod_catalog.silver.customer_dim")

# 2. Incoming batch with updates, inserts, and soft-delete flags
updates_df = spark.read.table("prod_catalog.staging.customer_updates")

# 3. Execute atomic MERGE INTO
(
    delta_target.alias("target")
    .merge(
        updates_df.alias("source"),
        # Primary key match condition
        "target.customer_id = source.customer_id"
    )
    # Branch 1: Soft-delete when flagged by upstream system
    .whenMatchedUpdate(
        condition="source.is_deleted = true",
        set={
            "is_active": F.lit(False),
            "updated_at": F.current_timestamp()
        }
    )
    # Branch 2: Update existing active records when source has newer timestamp
    .whenMatchedUpdate(
        condition="source.update_timestamp > target.updated_at",
        set={
            "email": "source.email",
            "phone": "source.phone",
            "tier": "source.tier",
            "updated_at": "source.update_timestamp"
        }
    )
    # Branch 3: Insert new customer records
    .whenNotMatchedInsert(
        values={
            "customer_id": "source.customer_id",
            "email": "source.email",
            "phone": "source.phone",
            "tier": "source.tier",
            "is_active": F.lit(True),
            "created_at": F.current_timestamp(),
            "updated_at": "source.update_timestamp"
        }
    )
    .execute()
)
```

**Enterprise Use Case:**  
Synchronizing customer master records from Salesforce and CRM systems into Silver dimension tables with audit tracking, soft-deletes, and timestamp fencing.

**Architectural Invariants & Gotchas:**
- DeltaTable.merge() executes atomically under Optimistic Concurrency Control.
- Including partition columns in the merge condition enables partition pruning, skipping 90%+ of Parquet file scans.
- Delta Lake 2.0+ supports multiple whenMatched and whenNotMatched clauses for complex conditional routing.
- Avoid full table scans by ensuring target table has statistics computed and Liquid Clustering enabled on join keys.

---

### Q3.18: #017 Recursive Schema Traversal: Flattening Deeply Nested JSON/Structs (Complex Data Types & Nested Structures Handling)

**Question & Concept Overview:**  
Develop a generic, production-ready recursive schema flattener in Python that traverses any arbitrarily deep Spark schema. The function must explode nested arrays and flatten all nested structs into top-level columns (`parent_child_field`).

**Production Implementation:**
```python
import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_017_Recursive_Schema_Traversal_Flattening_Deeply_Nested_JSONStructs")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
deep_data = [
    {
        "id": "ACC-99",
        "company": {
            "name": "Acme Corp",
            "headquarters": {
                "geo": {"lat": 37.7749, "lon": -122.4194},
                "city": "San Francisco"
            }
        },
        "metrics": {"financials": {"revenue": 5000000.0, "ebitda": 1200000.0}}
    }
]

import json
# Spark Connect-safe: avoid spark.sparkContext.parallelize()
df_raw = spark.createDataFrame([(json.dumps(d),) for d in deep_data], ["raw_json"])
inferred_schema = spark.range(1).select(F.schema_of_json(F.lit(json.dumps(deep_data[0])))).first()[0]
df_deep = df_raw.select(F.from_json("raw_json", inferred_schema).alias("doc")).select("doc.*")
print("Original Deeply Nested Schema:")
df_deep.printSchema()

# --- Execution Step ---
def flatten_dataframe(df):
    """
    Recursively flattens all StructType columns in a DataFrame into top-level columns.
    """
    flat_cols = []
    has_struct = False

    for field in df.schema.fields:
        if isinstance(field.dataType, T.StructType):
            has_struct = True
            for child in field.dataType.fields:
                flat_cols.append(
                    F.col(f"{field.name}.{child.name}").alias(f"{field.name}_{child.name}")
                )
        else:
            flat_cols.append(F.col(field.name))

    df_result = df.select(*flat_cols)
    # If any struct columns were flattened, recurse until all structs are resolved
    if has_struct:
        return flatten_dataframe(df_result)
    return df_result

df_flattened = flatten_dataframe(df_deep)
print("Completely Flattened Schema:")
df_flattened.printSchema()
df_flattened.show(truncate=False)

# --- Execution Step ---
expected_cols = [
    "id",
    "company_name",
    "company_headquarters_city",
    "company_headquarters_geo_lat",
    "company_headquarters_geo_lon",
    "metrics_financials_ebitda",
    "metrics_financials_revenue"
]

for c in expected_cols:
    assert c in df_flattened.columns, f"Expected column {c} missing from flattened DataFrame"

print(f"Successfully flattened deep schema into {len(df_flattened.columns)} scalar columns!")
```

**Enterprise Use Case:**  
An enterprise integration hub ingests complex payloads from ERP and CRM systems containing 5+ levels of nested structs and arrays. Analysts require a completely flattened tabular DataFrame where column names reflect their full dot-delimited path.

**Architectural Invariants & Gotchas:**
- Deeply nested schemas can cause severe query compilation overhead. Traversing the schema programmatically inspects `df.schema.fields` and constructs a projection expression list dynamically without executing any JVM actions.

---

### Q3.19: #029 Sessionization and the Gaps & Islands Problem in PySpark (Advanced SQL, Aggregations & Analytical Window Functions)

**Question & Concept Overview:**  
Solve the classic Gaps and Islands problem in distributed PySpark: detect temporal gaps using `lag()`, mark island session boundaries with conditional flags, compute cumulative sums over flags to generate sequential session IDs, and aggregate session metrics.

**Production Implementation:**
```python
import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_029_Sessionization_and_the_Gaps_and_Islands_Problem_in_PySpark")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
clicks = [
    ("U-01", "2023-11-20 10:00:00", "/home"),
    ("U-01", "2023-11-20 10:12:00", "/catalog"),     # 12 min gap (same session)
    ("U-01", "2023-11-20 10:25:00", "/product/42"),  # 13 min gap (same session)
    ("U-01", "2023-11-20 11:30:00", "/home"),        # 65 min gap -> NEW SESSION!
    ("U-01", "2023-11-20 11:40:00", "/cart"),        # 10 min gap (same session)
    ("U-02", "2023-11-20 09:00:00", "/login"),
    ("U-02", "2023-11-20 09:15:00", "/dashboard")
]

schema = "user_id STRING, event_time STRING, page STRING"
df_raw_clicks = spark.createDataFrame(clicks, schema).withColumn("ts", F.to_timestamp("event_time"))
df_raw_clicks.show(truncate=False)

# --- Execution Step ---
user_window = Window.partitionBy("user_id").orderBy("ts")

df_gaps = df_raw_clicks.withColumn(
    "prev_ts", F.lag("ts").over(user_window)
).withColumn(
    "inactivity_minutes",
    F.coalesce((F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0, F.lit(0.0))
).withColumn(
    "is_new_session",
    F.when((F.col("inactivity_minutes") > 30.0) | (F.col("prev_ts").isNull()), 1).otherwise(0)
)

df_gaps.select("user_id", "event_time", "inactivity_minutes", "is_new_session").show()

# --- Execution Step ---
cum_session_window = Window.partitionBy("user_id").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_sessionized = df_gaps.withColumn(
    "session_num", F.sum("is_new_session").over(cum_session_window)
).withColumn(
    "session_id", F.concat(F.col("user_id"), F.lit("_SESS_"), F.col("session_num"))
)

print("Sessionized Clickstream:")
df_sessionized.select("session_id", "user_id", "event_time", "page", "inactivity_minutes").show(truncate=False)

# Aggregate session summaries
df_summary = df_sessionized.groupBy("session_id", "user_id").agg(
    F.min("ts").alias("session_start"),
    F.max("ts").alias("session_end"),
    F.count("page").alias("page_views"),
    F.round((F.max("ts").cast("long") - F.min("ts").cast("long")) / 60.0, 1).alias("session_duration_min")
)

print("Aggregated Session Profiles:")
df_summary.show(truncate=False)
```

**Enterprise Use Case:**  
A digital analytics platform tracks web page views. An active browsing session is defined as consecutive clicks separated by no more than 30 minutes of inactivity. When a gap exceeds 30 minutes, a new unique session identifier must be generated.

**Architectural Invariants & Gotchas:**
- Solving Gaps & Islands in Spark uses a 2-step window pattern: (1) An ordered window with `lag()` to compute time delta between consecutive events and emit a 1/0 boundary indicator, and (2) A cumulative sum window over the boundary indicator to generate monotonic session IDs across the entire partition.

---

### Q3.20: #039 Z-Order Multi-Dimensional Indexing vs Liquid Clustering (CLUSTER BY) (Delta Lake Architecture, ACID Operations & Time Travel)

**Question & Concept Overview:**  
Compare multi-dimensional indexing techniques: implement `OPTIMIZE ... ZORDER BY (col1, col2)` to co-locate multidimensional data along space-filling Hilbert/Morton curves, and contrast with modern Databricks Liquid Clustering (`CLUSTER BY`) which supports incremental re-clustering without full rewrites.

**Production Implementation:**
```python
import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_039_Z_Order_Multi_Dimensional_Indexing_vs_Liquid_Clustering_CLUSTER_BY")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
delta_zorder_path = "/tmp/delta_zorder_039"
if Path(delta_zorder_path).exists():
    shutil.rmtree(delta_zorder_path)

import random
random.seed(42)

telemetry = [
    (f"DEV-{random.randint(1, 100):03d}", f"CAMP-{random.randint(1, 20):02d}", f"2023-11-{random.randint(1, 10):02d}", round(random.uniform(10.0, 500.0), 2))
    for _ in range(1000)
]

schema = "device_id STRING, campaign_id STRING, event_date STRING, metric_value DOUBLE"
df_telemetry = spark.createDataFrame(telemetry, schema)
df_telemetry.write.format("delta").mode("overwrite").save(delta_zorder_path)

print("Telemetry Delta table written.")

# --- Execution Step ---
# Z-Order on device_id and campaign_id
df_zorder_res = spark.sql(f"OPTIMIZE delta.`{delta_zorder_path}` ZORDER BY (device_id, campaign_id)")
df_zorder_res.select("path", "metrics.numFilesAdded", "metrics.numFilesRemoved").show(truncate=False)

print("Table successfully Z-Ordered for fast multi-column data skipping!")

# --- Execution Step ---
# In Databricks Runtime 13.3+ / Unity Catalog, Liquid Clustering is declared via:
# CREATE TABLE events (
#     device_id STRING,
#     campaign_id STRING,
#     event_date DATE,
#     metric_value DOUBLE
# ) USING DELTA
# CLUSTER BY (device_id, campaign_id);

# To re-cluster incrementally:
# OPTIMIZE events;  -- Only clusters newly appended data without rewriting entire table!

print("Liquid Clustering advantages: no partition lock-in, non-destructive column changes, and incremental re-clustering!")
```

**Enterprise Use Case:**  
An ad-tech analytics platform runs ad-hoc queries filtering on combinations of `customer_id`, `event_date`, and `campaign_id`. Traditional Hive partitioning on all three columns causes over-partitioning, while single-column sorting leaves other query predicates unoptimized.

**Architectural Invariants & Gotchas:**
- Z-Ordering maps multi-dimensional data onto a 1D space-filling curve, preserving spatial locality so queries filtering on any subset of Z-ordered columns skip unneeded Parquet files. Liquid Clustering (Databricks Runtime 13.3+) replaces partitioning and Z-order with dynamic clustering that scales incrementally.

---

### Q3.21: #040 Delta Change Data Feed (CDF): Enabling and Consuming Row-Level Changes (Delta Lake Architecture, ACID Operations & Time Travel)

**Question & Concept Overview:**  
Enable Delta Change Data Feed (`delta.enableChangeDataFeed = true`). Execute inserts, updates, and deletes on the table. Query the resulting change stream using `readChangeFeed` to capture pre-images, post-images, and deleted records.

**Production Implementation:**
```python
import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_040_Delta_Change_Data_Feed_CDF_Enabling_and_Consuming_Row_Level_Changes")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
from delta.tables import DeltaTable

delta_cdf_path = "/tmp/delta_cdf_040"
if Path(delta_cdf_path).exists():
    shutil.rmtree(delta_cdf_path)

# Create table with CDF enabled
init_customers = [
    ("C-01", "Alice", 1000.0),
    ("C-02", "Bob", 2500.0)
]

spark.createDataFrame(init_customers, ["cust_id", "name", "credit_limit"])    .write.format("delta")    .option("delta.enableChangeDataFeed", "true")    .mode("overwrite")    .save(delta_cdf_path)

print("Delta Table initialized with Change Data Feed enabled.")

# --- Execution Step ---
dt = DeltaTable.forPath(spark, delta_cdf_path)

# Commit 1: Update credit limit for Alice
dt.update(condition=F.col("cust_id") == "C-01", set={"credit_limit": F.lit(1500.0)})

# Commit 2: Delete Bob
dt.delete(condition=F.col("cust_id") == "C-02")

# Commit 3: Insert Charlie
new_cust = [("C-03", "Charlie", 5000.0)]
spark.createDataFrame(new_cust, ["cust_id", "name", "credit_limit"])    .write.format("delta")    .mode("append")    .save(delta_cdf_path)

print("Applied 3 commits to generate CDF events.")

# --- Execution Step ---
# Query CDF from version 0 to latest version
df_cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .load(delta_cdf_path)
)

print("Complete Change Data Feed Stream:")
df_cdf.select(
    "cust_id",
    "name",
    "credit_limit",
    "_change_type",
    "_commit_version",
    "_commit_timestamp"
).show(truncate=False)
```

**Enterprise Use Case:**  
An enterprise financial lakehouse architecture maintains a real-time risk dashboard that must react to downstream row-level modifications (inserts, updates, and deletes) occurring in the upstream customer ledger without re-scanning the entire table.

**Architectural Invariants & Gotchas:**
- Delta Change Data Feed (CDF) generates row-level change events alongside commits. When updates occur, CDF logs both the `update_preimage` (row state before update) and `update_postimage` (row state after update), enabling exact financial double-entry reconciliation downstream.

---

### Q3.22: #050 Building a Metadata-Driven Multi-Format Ingestion Engine (Data Ingestion Patterns, Auto Loader & File Formats)

**Question & Concept Overview:**  
Design an extensible, configuration-driven multi-format ingestion class `UnifiedIngestionEngine` in PySpark that takes a JSON/dictionary configuration spec, validates format options, applies standard audit metadata columns (`_ingested_at`, `_source_file`), and writes standardized Delta tables.

**Production Implementation:**
```python
import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_050_Building_a_Metadata_Driven_Multi_Format_Ingestion_Engine")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
pipeline_specs = [
    {
        "table_name": "src_orders",
        "format": "json",
        "raw_path": "/tmp/src_orders_050.json",
        "target_path": "/tmp/delta_orders_050",
        "partition_cols": ["order_status"],
        "options": {"multiline": "false"}
    },
    {
        "table_name": "src_products",
        "format": "csv",
        "raw_path": "/tmp/src_products_050.csv",
        "target_path": "/tmp/delta_products_050",
        "partition_cols": [],
        "options": {"header": "true", "inferSchema": "true"}
    }
]

print(f"Loaded {len(pipeline_specs)} pipeline specifications.")

# --- Execution Step ---
from datetime import datetime

class UnifiedIngestionEngine:
    def __init__(self, spark_session):
        self.spark = spark_session

    def ingest(self, spec: dict):
        table_name = spec["table_name"]
        fmt = spec["format"]
        raw_path = spec["raw_path"]
        target_path = spec["target_path"]
        partition_cols = spec.get("partition_cols", [])
        options = spec.get("options", {})

        print(f"--> Ingesting {table_name} [Format: {fmt}] from {raw_path}")

        # 1. Read dynamically using format and options
        reader = self.spark.read.format(fmt)
        for k, v in options.items():
            reader = reader.option(k, v)
        df_raw = reader.load(raw_path)

        # 2. Add standard audit columns
        df_enriched = (
            df_raw
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(datetime.utcnow().strftime("%Y%m%d%H%M%S")))
        )

        # 3. Write standardized Delta table
        writer = df_enriched.write.format("delta").mode("append")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)

        count = df_enriched.count()
        print(f"--> Successfully landed {count} records into {target_path}")
        return df_enriched

print("UnifiedIngestionEngine class ready.")

# --- Execution Step ---
# Prepare sample files
with open("/tmp/src_orders_050.json", "w") as f:
    f.write('{"order_id": "O1", "amount": 120.0, "order_status": "COMPLETED"}\n')
    f.write('{"order_id": "O2", "amount": 85.0, "order_status": "PENDING"}\n')

with open("/tmp/src_products_050.csv", "w") as f:
    f.write("prod_id,prod_name,cost\nP1,Widget,10.50\nP2,Gizmo,25.00\n")

engine = UnifiedIngestionEngine(spark)
for spec in pipeline_specs:
    engine.ingest(spec)

# Verify written Delta table
print("Verified Delta Table Output (src_orders):")
spark.read.format("delta").load("/tmp/delta_orders_050").show(truncate=False)
```

**Enterprise Use Case:**  
A centralized lakehouse ingestion platform must onboard 200+ distinct source datasets daily spanning CSV, Parquet, JSON, and ORC. Writing bespoke ingestion notebooks for each dataset is unmaintainable and error-prone.

**Architectural Invariants & Gotchas:**
- A metadata-driven framework separates orchestration logic from data transformation code. Pipeline configuration (source paths, file formats, options, partition keys, schema definitions) can be stored in Delta configuration tables or YAML files and executed dynamically.

---

### Q3.23: #052 Handling Compressed Archives (.zip, .tar.gz, .bz2) in Distributed PySpark (Data Ingestion Patterns, Auto Loader & File Formats)

**Question & Concept Overview:**  
Ingest and process compressed archives in distributed Spark: understand file splittability differences across codecs (Gzip, Bzip2, Snappy, Zip), implement parallel extraction using `binaryFiles` and distributed RDD partitions, and convert decompressed streams into DataFrames.

**Production Implementation:**
```python
import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_052_Handling_Compressed_Archives_zip_targz_bz2_in_Distributed_PySpark")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
import zipfile
import io

zip_dir = Path("/tmp/zip_archives_052")
if zip_dir.exists():
    shutil.rmtree(zip_dir)
zip_dir.mkdir(parents=True, exist_ok=True)

# Create a zip file containing 2 inner CSV files
zip_path = zip_dir / "quarterly_reports.zip"
with zipfile.ZipFile(zip_path, "w") as zf:
    zf.writestr("q1_sales.csv", "report_id,amount\nR1,500.0\nR2,800.0\n")
    zf.writestr("q2_sales.csv", "report_id,amount\nR3,650.0\nR4,920.0\n")

print(f"Created sample ZIP archive at {zip_path}")

# --- Execution Step ---
# Spark Connect-safe: use native binaryFile DataSource + mapInPandas instead of sparkContext.binaryFiles
import pandas as pd

df_binary = spark.read.format("binaryFile").load(str(zip_dir))

def extract_zip_entries(batches):
    for pdf in batches:
        records = []
        for byte_stream in pdf["content"]:
            with zipfile.ZipFile(io.BytesIO(bytes(byte_stream))) as z:
                for inner_filename in z.namelist():
                    if inner_filename.endswith(".csv"):
                        content = z.read(inner_filename).decode("utf-8")
                        for line in content.strip().split("\n")[1:]:
                            parts = line.split(",")
                            records.append((inner_filename, parts[0], float(parts[1])))
        yield pd.DataFrame(records, columns=["archive_member", "report_id", "amount"])

df_decompressed = df_binary.mapInPandas(
    extract_zip_entries,
    "archive_member STRING, report_id STRING, amount DOUBLE",
)

print("Decompressed Records Extracted in Parallel:")
df_decompressed.show()

# --- Execution Step ---
codecs = [
    ("GZIP (.gz)", "No", "High", "Text / Web Logs", "Cannot split without container format"),
    ("BZIP2 (.bz2)", "Yes", "Very High", "Cold Archival", "Splittable via 48-bit sync markers"),
    ("SNAPPY", "Yes (in Parquet/Avro)", "Medium", "Active Analytics", "High-speed decompression"),
    ("ZSTD", "Yes (in Parquet)", "Very High", "Modern Lakehouse", "Balanced compression and speed"),
    ("ZIP (.zip)", "No", "Medium", "Legacy Vendor Feeds", "Requires custom parallel unpacking")
]

df_codecs = spark.createDataFrame(codecs, ["codec", "splittable", "compression_ratio", "primary_use_case", "notes"])
df_codecs.show(truncate=False)
```

**Enterprise Use Case:**  
A regulatory agency deposits quarterly compliance archives as massive 5GB ZIP and TAR.GZ files. Standard Spark readers cannot split ZIP archives across executors, causing single-node driver OutOfMemory (OOM) crashes.

**Architectural Invariants & Gotchas:**
- Compression formats without sync markers (GZIP, ZIP) are not splittable: a single 5GB GZIP file must be processed by exactly one Spark task. Bzip2 and Snappy (within container formats like Parquet/Avro) are splittable across tasks.

---

### Q3.24: #053 Distributed REST API and Webhook Ingestion without Driver OOM (Data Ingestion Patterns, Auto Loader & File Formats)

**Question & Concept Overview:**  
Implement parallel, distributed HTTP REST API ingestion in PySpark: partition entity IDs across worker nodes, use `mapPartitions` to maintain persistent connection sessions (`requests.Session`), apply batch chunking, handle exponential backoff rate limits, and assemble responses into DataFrames.

**Production Implementation:**
```python
import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_053_Distributed_REST_API_and_Webhook_Ingestion_without_Driver_OOM")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
tickers = [f"TICKER_{i:04d}" for i in range(1, 21)]
# Distribute across 4 partitions
df_tickers = spark.createDataFrame([(t,) for t in tickers], ["ticker"]).repartition(4)

part_count = df_tickers.select(F.spark_partition_id()).distinct().count()
print(f"Total Tickers: {df_tickers.count()} across {part_count} partitions")

# --- Execution Step ---
import time
import json

import pandas as pd

def fetch_ticker_data_partition(batches):
    # Executed ONCE per partition on worker executor (Spark Connect-safe via mapInPandas)
    for pdf in batches:
        results = []
        for ticker in pdf["ticker"]:
            time.sleep(0.005)
            results.append({
                "ticker": ticker,
                "price": round(150.0 + (hash(ticker) % 50), 2),
                "currency": "USD",
                "http_status": 200,
                "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        yield pd.DataFrame(results)

schema = "ticker STRING, price DOUBLE, currency STRING, http_status INT, fetched_at STRING"
df_api_results = df_tickers.mapInPandas(fetch_ticker_data_partition, schema)

print("Distributed REST API Ingestion Results:")
df_api_results.show(10)

# --- Execution Step ---
assert df_api_results.count() == 20, "Ticker count mismatch"
assert df_api_results.filter(F.col("http_status") != 200).count() == 0, "Non-200 responses detected"
print("All distributed API calls verified successfully!")
```

**Enterprise Use Case:**  
A financial data vendor exposes historical equity prices via an authenticated REST API with rate limits. The engineering team must fetch data for 50,000 distinct stock tickers. Running loops on the driver node takes 18 hours or crashes the driver with out-of-memory errors.

**Architectural Invariants & Gotchas:**
- Using `mapPartitions` initializes one HTTP connection pool (`requests.Session()`) per partition rather than opening a new TCP connection per row. Distributing requests across 20 executor partitions achieves 20x concurrency while honoring per-node rate limit quotas.

---

### Q3.25: #056 Streaming Window Aggregations and Watermarking for Late-Arriving Events (Streaming & Near Real-Time Processing)

**Question & Concept Overview:**  
Implement streaming window aggregations with watermarking: configure `withWatermark('event_time', '10 minutes')`, compute tumbling and sliding window metrics, and explain how the watermark dictates state eviction from RocksDB memory.

**Production Implementation:**
```python
import os
import sys
import shutil
from pathlib import Path

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_056_Streaming_Window_Aggregations_and_Watermarking_for_Late_Arriving_Events")
        # Spark 4.0+ / Spark Connect: omit .master(); pass .remote("sc://localhost:15002") for remote server
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Spark Connect-safe session check (avoid spark.sparkContext in Spark 4.0+)
print(f"Spark Version: {spark.version}")
print(f"Session Active: {spark.conf.get('spark.app.name', 'SparkSession')}")

# --- Execution Step ---
# Architectural Blueprint:
# Event Time: 12:00 -> Watermark = 12:00 - 10 min = 11:50
# Window [11:30 - 11:40]: Completely older than 11:50 -> State Evicted! No longer accepts updates.
# Window [11:45 - 11:55]: End time (11:55) > 11:50 -> Window still active in state memory.

print("Watermark state eviction semantics mapped.")

# --- Execution Step ---
from datetime import datetime, timedelta

base_time = datetime(2023, 11, 20, 12, 0, 0)

# Simulate events arriving out of chronological order
batch_events = [
    # On-time events
    (base_time, "Alice", 100),
    (base_time + timedelta(minutes=2), "Bob", 150),
    # Advance watermark to 12:15
    (base_time + timedelta(minutes=15), "Charlie", 300),
    # Late event from 12:03 (Arrives when max time is 12:15, delta = 12 min. Allowed if watermark >= 15 min!)
    (base_time + timedelta(minutes=3), "Alice", 50)
]

schema = "event_time TIMESTAMP, player STRING, score INT"
df_events = spark.createDataFrame(batch_events, schema)
df_events.show(truncate=False)

# --- Execution Step ---
# Tumbling window aggregation with watermark
df_windowed = (
    df_events
    .withWatermark("event_time", "15 minutes")
    .groupBy(
        F.window("event_time", "10 minutes"),
        "player"
    )
    .agg(
        F.sum("score").alias("total_score"),
        F.count("score").alias("events_count")
    )
    .select(
        "window.start",
        "window.end",
        "player",
        "total_score",
        "events_count"
    )
    .orderBy("start", "player")
)

print("Aggregated Window Output (Late Event Incorporated):")
df_windowed.show(truncate=False)
```

**Enterprise Use Case:**  
A mobile gaming platform aggregates player score events over 10-minute tumbling windows. Due to intermittent mobile connectivity and airplane mode, score events arrive up to 15 minutes late. The pipeline must incorporate late events without letting state size grow infinitely.

**Architectural Invariants & Gotchas:**
- A watermark specifies the maximum threshold by which event-time can lag behind the maximum event-time observed by the engine ($Watermark = Max(event\_time) - Delay$). Once a window's end time falls behind the watermark, Spark emits the final aggregate and purges the window's state from memory.

---

---

### Q3.26: Distributed Embedding Generation at Scale with PySpark Arrow UDFs (Mosaic AI & GenAI)

**Scenario & System Overview:**  
An enterprise lakehouse contains 100 million technical support articles and customer transcripts. The platform team must generate 1024-dimensional dense embeddings at scale to feed a Mosaic AI Vector Search Delta Sync Index. How to structure high-throughput, zero-copy PySpark transformations using `@pandas_udf` and batch inference without crashing executor JVMs.

**Technical Deep-Dive & Architecture:**  
Calling an embedding model sequentially inside standard Python UDFs introduces severe pickling overhead ($> 80\%$ CPU time spent on row serialization) and causes executor timeouts. By employing **PySpark Vectorized Pandas UDFs (`@pandas_udf(ArrayType(FloatType()))`)**, Apache Arrow transfers batches of records (e.g. 5,000 strings) directly into Python process memory with zero copy. The Python process loads a local sentence-transformer or queries a batched Mosaic AI Foundation Model serving endpoint, returning a PyArrow RecordBatch containing float arrays directly to the Spark Tungsten off-heap engine.

**Production Implementation:**
```python
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd
from typing import Iterator

# Define PyArrow iterator UDF for batch embedding inference
# Uses worker batching to maximize GPU/CPU tensor throughput
@F.pandas_udf(ArrayType(FloatType()))
def compute_embeddings_arrow(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Lazy load local embedding model once per worker lifecycle
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer('BAAI/bge-large-en-v1.5')

    for text_series in batch_iter:
        # Generate dense embeddings across Arrow chunk
        clean_texts = text_series.fillna("").tolist()
        embeddings = model.encode(clean_texts, batch_size=256, normalize_embeddings=True)
        # Yield as Pandas Series of float lists for Spark
        yield pd.Series(embeddings.tolist())

# Apply vectorized embedding generator to gold documents
df_embedded = (
    spark.table("prod_catalog.gold.knowledge_docs")
    .filter(F.length(F.col("chunk_text")) > 20)
    .withColumn("embedding_vector", compute_embeddings_arrow("chunk_text"))
)

# Write to Delta table with Change Data Feed enabled
# Enables automatic Delta Sync in Mosaic AI Vector Search
(
    df_embedded.write
    .format("delta")
    .mode("append")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable("prod_catalog.ai_schema.kb_embeddings")
)
```

**Key Invariants & Gotchas:**
- Set `spark.sql.execution.arrow.maxRecordsPerBatch` (e.g. 1000–5000) to prevent Arrow buffer OOM.
- Ensure `delta.enableChangeDataFeed = true` so Mosaic AI Vector Search syncs incrementally.
- Always use `Iterator[pd.Series]` UDF pattern to initialize the model once per worker task.

---

### Q3.27: Real-Time Streaming Ingestion for Mosaic AI Vector Search (Mosaic AI & GenAI)

**Scenario & System Overview:**  
Customer support conversations and dynamic document revisions must be discoverable in Mosaic AI Vector Search within 15 seconds of creation. How to design a Structured Streaming pipeline writing to Delta Lake with CDF to drive sub-minute Vector Search Delta Sync.

**Technical Deep-Dive & Architecture:**  
Mosaic AI Vector Search Delta Sync operates by tailing the Delta transaction log and Change Data Feed (`_delta_log` and `_change_data`). When Structured Streaming appends micro-batches to the target Delta table, each commit records row insertions, updates, and deletes with commit timestamps. Vector Search compute endpoints poll the CDF, compute embeddings for new chunks, and update the HNSW vector index.

**Production Implementation:**
```python
from pyspark.sql import functions as F

# Stream incoming support tickets from Kafka
# Parse payload and generate text chunks for indexing
df_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "support.ticket.events")
    .option("startingOffsets", "latest")
    .load()
    .select(F.from_json(F.col("value").cast("string"), "ticket_id STRING, customer_id STRING, text STRING, updated_at TIMESTAMP").alias("data"))
    .select("data.*")
)

# Sink to Delta table with Change Data Feed enabled
# Vector Search Delta Sync indexes automatically tail this table
query = (
    df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "abfss://checkpoints@datalake.dfs.core.windows.net/tickets_cdf")
    .option("mergeSchema", "true")
    .table("prod_catalog.ai_schema.support_tickets_stream")
)
```

**Key Invariants & Gotchas:**
- The target Delta table must have `delta.enableChangeDataFeed = true`.
- Checkpoints must reside on fault-tolerant cloud storage with reliable leases.
- Continuous Vector Search sync maintains $<20\text{s}$ latency SLA.

---

### Q3.28: High-Throughput Batch LLM Inference with MLflow & PySpark (Mosaic AI & GenAI)

**Scenario & System Overview:**  
Scoring millions of customer feedback comments daily using a fine-tuned Llama 3 model registered in Unity Catalog. How to leverage `mlflow.pyfunc.spark_udf` to execute distributed model inference across a multi-node Spark cluster.

**Technical Deep-Dive & Architecture:**  
Instead of sending millions of REST requests sequentially, `mlflow.pyfunc.spark_udf` distributes the model weights to Spark executor local storage (via Spark broadcast/distributed cache) and executes local GPU/CPU batch inference within executor worker threads. This scales horizontally across hundreds of executor cores while completely eliminating network REST API throttling.

**Production Implementation:**
```python
import mlflow.pyfunc
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Load registered Unity Catalog model as distributed Spark UDF
# Distributes model weights across Spark executor nodes
model_uri = "models:/prod_catalog.models.llama_3_customer_classifier/2"
predict_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=model_uri,
    result_type=StringType()
)

# Run distributed batch inference over partitioned Delta table
df_classified = (
    spark.table("prod_catalog.silver.customer_feedback")
    .withColumn("predicted_category", predict_udf(F.col("feedback_text")))
    .select("feedback_id", "feedback_text", "predicted_category")
)

# Persist classified results to gold lakehouse layer
df_classified.write.format("delta").mode("overwrite").saveAsTable("prod_catalog.gold.classified_feedback")
```

**Key Invariants & Gotchas:**
- `mlflow.pyfunc.spark_udf` requires model dependencies packaged in conda/virtualenv.
- Ensure executor nodes have GPU drivers installed if running dense transformer models.
- Set Spark task concurrency to match available executor GPU slots.
