"""
Module 08: Performance Tuning, Query Optimization & Skew Mitigation (Notebooks 074 - 085)
"""

import re
from pathlib import Path
from notebook_builder import build_notebook, write_notebook

MODULE_FOLDER = "08_performance_and_tuning"
MODULE_NAME = "Module 08: Performance Tuning, Query Optimization & Skew Mitigation"

def sanitize_filename(nb_id: str, title: str) -> str:
    cleaned = title.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return f"{nb_id}_{cleaned}.py"

def generate(base_dir: Path):
    out_dir = base_dir / "notebooks" / MODULE_FOLDER
    out_dir.mkdir(parents=True, exist_ok=True)

    notebooks = [
        {
            "nb_id": "074",
            "title": "Catalyst Optimizer Deep Dive: Parsed, Analyzed, Optimized Plans and Code Generation",
            "difficulty": "Advanced",
            "business_scenario": "A lead data architect conducts code reviews for data pipelines. Junior engineers write complex nested SQL and DataFrame code without understanding whether Catalyst collapses filters or pushes down projections. The team needs to inspect execution plans programmatically.",
            "problem_statement": "Master Apache Spark Catalyst Optimizer: analyze the 4 compilation phases (Parsed Logical Plan, Analyzed Logical Plan, Optimized Logical Plan, Physical Plan), inspect Whole-Stage Code Generation (`*` in physical plan), and compare `df.explain(True)` with `df.explain('formatted')`.",
            "architectural_notes": "Catalyst compiles queries through 4 stages: (1) Analysis via Catalog lookup, (2) Logical Optimization (predicate pushdown, constant folding, projection pruning), (3) Physical Planning (choosing join algorithms, e.g. BroadcastHashJoin vs SortMergeJoin), and (4) Whole-Stage Code Generation into optimized Java bytecode via Janino.",
            "sections": [
                {
                    "title": "Constructing Multi-Stage Query for Plan Inspection",
                    "explanation": "Create an unoptimized query with redundant filters and transformations.",
                    "code": """# Create mock orders dataset
df_orders = spark.range(1, 10001, 1, 4).select(
    F.col("id").alias("order_id"),
    (F.col("id") % 10).alias("customer_id"),
    (F.col("id") * 1.5).alias("amount"),
    F.lit("ACTIVE").alias("status")
)

# Unoptimized query with redundant filters and constant math
df_plan_query = (
    df_orders
    .filter(F.col("status") == "ACTIVE")
    .filter(F.col("amount") > (100.0 * 2.0)) # Constant folding opportunity: 100 * 2 = 200
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("total_spend"))
    .filter(F.col("total_spend") > 500)
)

print("Query constructed.")"""
                },
                {
                    "title": "Inspecting Full Catalyst Lifecycle Plans via explain(True)",
                    "explanation": "Print all 4 internal compilation stages of Catalyst.",
                    "code": """print("=== COMPLETE CATALYST COMPILATION PHASES ===")
df_plan_query.explain(extended=True)"""
                },
                {
                    "title": "Analyzing Whole-Stage Code Generation (Tungsten)",
                    "explanation": "Identify the asterisk (*) symbol indicating Whole-Stage CodeGen in the physical plan.",
                    "code": """print("=== FORMATTED PHYSICAL PLAN (Notice WholeStageCodegen operators) ===")
df_plan_query.explain(mode="formatted")"""
                }
            ],
            "key_takeaways": [
                "Catalyst translates declarative logic into optimized Java bytecode through 4 formal compilation phases.",
                "Constant folding and predicate pushdown occur in the Optimized Logical Plan stage.",
                "The asterisk `*` next to operator names in physical plans signifies Whole-Stage Code Generation (Tungsten execution)."
            ],
            "interview_questions": [
                "What are the 4 main phases of the Spark Catalyst Optimizer?",
                "What is Whole-Stage Code Generation and how does it eliminate virtual function call overhead in Java?",
                "How does the Cost-Based Optimizer (CBO) use table statistics to improve physical join planning?"
            ]
        },
        {
            "nb_id": "075",
            "title": "Adaptive Query Execution (AQE): Dynamic Coalescing, Join Switching and Skew Joins",
            "difficulty": "Advanced",
            "business_scenario": "A 2TB batch ETL pipeline runs with `spark.sql.shuffle.partitions = 200`. During filtering stages, data volume drops from 2TB to 50MB, but Spark still creates 200 tiny shuffle partitions, wasting task scheduling overhead. At joins, sudden data skew causes single task stragglers.",
            "problem_statement": "Master Adaptive Query Execution (AQE) enabled by default in Spark 3.0+: demonstrate (1) Dynamically coalescing shuffle partitions, (2) Dynamically converting Sort-Merge Join to Broadcast Hash Join at runtime, and (3) Dynamic skew join optimization (`spark.sql.adaptive.skewJoin.enabled`).",
            "architectural_notes": "Traditional Spark plans are fixed before execution starts. AQE introduces feedback loops: at shuffle stage boundaries, AQE inspects runtime partition statistics. If a table became small after filtering, AQE dynamically switches to BroadcastHashJoin; if partitions are tiny, it coalesces them dynamically.",
            "sections": [
                {
                    "title": "Verifying AQE Configuration in SparkSession",
                    "explanation": "Check core Adaptive Query Execution settings.",
                    "code": """aqe_configs = [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
    "spark.sql.adaptive.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor"
]

print("Current SparkSession AQE Configurations:")
for conf in aqe_configs:
    print(f"  {conf} = {spark.conf.get(conf, 'NOT_SET')}")"""
                },
                {
                    "title": "Demonstrating Dynamic Partition Coalescing",
                    "explanation": "Observe how AQE coalesces 200 initial shuffle partitions down to optimal task count.",
                    "code": """# Set high initial shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "20")

# Run query that filters heavily
df_heavy_filter = (
    spark.range(1, 100000, 1, 8)
    .filter(F.col("id") < 100) # Reduces data to tiny 99 rows
    .groupBy(F.col("id") % 5)
    .count()
)

df_heavy_filter.collect()

print("=== AQE PHYSICAL PLAN (Notice 'AdaptiveSparkPlan' node) ===")
df_heavy_filter.explain(mode="simple")"""
                },
                {
                    "title": "Understanding Dynamic Skew Join Splitting",
                    "explanation": "Explain how AQE splits skewed partition tasks into parallel sub-tasks.",
                    "code": """# AQE Skew Join Mechanics:
# When a partition exceeds:
# (partitionSize > skewedPartitionFactor * medianPartitionSize) AND (partitionSize > skewedPartitionThresholdInBytes)
# AQE automatically splits the large skewed partition into multiple sub-partitions,
# pairing each sub-partition with the matching key on the other side!

print("AQE Dynamic Skew Join split criteria mapped.")"""
                }
            ],
            "key_takeaways": [
                "AQE uses runtime statistics collected at shuffle stage boundaries to re-optimize execution plans on the fly.",
                "Dynamic Partition Coalescing prevents small partition task scheduling overhead after selective filters.",
                "Dynamic Skew Join automatically splits slow straggler partitions without manual salting code."
            ],
            "interview_questions": [
                "What are the three major optimization features provided by Adaptive Query Execution (AQE)?",
                "How does AQE dynamically switch a Sort-Merge Join to a Broadcast Hash Join at runtime?",
                "What metric threshold triggers AQE dynamic skew join handling?"
            ]
        },
        {
            "nb_id": "076",
            "title": "Data Skew Detection and Mitigation Using Salting and Two-Stage Joins",
            "difficulty": "Advanced",
            "business_scenario": "A national e-commerce platform joins website clicks with user accounts. Out of 100 million clicks, 40% belong to guest user ID `GUEST_000` (null/anonymous user). The join stalls at 99% for 45 minutes because a single executor task processes all 40 million guest clicks, causing shuffle spill and OOM.",
            "problem_statement": "Detect and mitigate severe data skew: quantify partition skew using `spark_partition_id()`, implement the Salting technique (adding random integer suffixes to the skewed key on the large table and exploding matching suffixes on the dimension table), and measure performance improvements.",
            "architectural_notes": "Data skew occurs when one partition contains disproportionately more records than others. In SortMergeJoin, rows with identical keys hash to the same reducer task. Salting appends a random salt $0 \dots (K-1)$ to the skewed key, distributing the single hot key evenly across $K$ reducer partitions.",
            "sections": [
                {
                    "title": "Simulating Severely Skewed Transaction Dataset",
                    "explanation": "Create dataset where 80% of rows have the identical customer_id 'HOT_KEY'.",
                    "code": """import random
random.seed(42)

# Generate 10,000 transactions: 8,000 belong to HOT_KEY, 2,000 distributed normally
skewed_txns = [("HOT_KEY", 10.0 + i) for i in range(8000)] + [
    (f"CUST_{random.randint(1, 500):03d}", 50.0 + i) for i in range(2000)
]

df_skewed = spark.createDataFrame(skewed_txns, ["customer_id", "amount"]).repartition(4, "customer_id")

print("Skew Distribution Across Partitions:")
df_skewed.withColumn("pid", F.spark_partition_id()).groupBy("pid").count().orderBy("pid").show()"""
                },
                {
                    "title": "Step 1: Adding Salt to Skewed Table",
                    "explanation": "Append a random salt integer between 0 and 3 to customer_id.",
                    "code": """SALT_FACTOR = 4

# Append random salt to large skewed table
df_salted = df_skewed.withColumn(
    "salt",
    F.when(F.col("customer_id") == "HOT_KEY", F.floor(F.rand(seed=42) * SALT_FACTOR)).otherwise(F.lit(0))
).withColumn(
    "salted_key",
    F.concat(F.col("customer_id"), F.lit("_SALT_"), F.col("salt"))
)

print("Salted Large Table (HOT_KEY split into 4 distinct keys):")
df_salted.filter(F.col("customer_id") == "HOT_KEY").show(5)"""
                },
                {
                    "title": "Step 2: Exploding Customer Lookup Table and Executing Balanced Join",
                    "explanation": "Replicate dimension keys across all salt values and execute join with even distribution.",
                    "code": """# Small customer dimension table
cust_dim = [("HOT_KEY", "Guest Shopper"), ("CUST_001", "Regular Shopper")]
df_dim = spark.createDataFrame(cust_dim, ["customer_id", "profile"])

# Replicate dimension keys across all possible salt values [0, 1, 2, 3]
df_dim_exploded = (
    df_dim
    .withColumn("salt_array", F.array([F.lit(i) for i in range(SALT_FACTOR)]))
    .select("customer_id", "profile", F.explode("salt_array").alias("salt"))
    .withColumn("salted_key", F.concat(F.col("customer_id"), F.lit("_SALT_"), F.col("salt")))
)

# Execute join on salted_key
df_balanced_join = df_salted.join(df_dim_exploded, on="salted_key", how="inner")

print("Post-Join Partition Distribution (Notice even distribution across partitions):")
df_balanced_join.withColumn("pid", F.spark_partition_id()).groupBy("pid").count().orderBy("pid").show()"""
                }
            ],
            "key_takeaways": [
                "Data skew causes the notorious '99% task stuck' problem where one executor handles massive key volume.",
                "Salting appends random integers to the hot key on the fact table and replicates dimension rows with matching salts.",
                "In Spark 3.0+, enable AQE (`spark.sql.adaptive.skewJoin.enabled = true`) to handle skew automatically without manual code salting."
            ],
            "interview_questions": [
                "What are the symptoms of data skew in the Spark UI (Task Duration, GC Time, Shuffle Read Size)?",
                "How does the Salting technique redistribute a single hot join key across multiple partitions?",
                "What is the difference between manual Salting and AQE Dynamic Skew Join?"
            ]
        },
        {
            "nb_id": "077",
            "title": "Partitioning Strategies: Avoiding Over-Partitioning and Directory Pruning",
            "difficulty": "Intermediate",
            "business_scenario": "A telemetry pipeline partitioned data by `year/month/day/hour/device_id`. After 6 months, the table contained 2 million partition directories containing tiny 10KB files. Querying the table caused driver timeouts during file listing before reading a single byte of data.",
            "problem_statement": "Evaluate big data partitioning best practices: compare high-cardinality vs low-cardinality partitioning columns, demonstrate Partition Pruning (skipping partition directories), and explain why target partition file sizes must be between 100MB and 1GB.",
            "architectural_notes": "Hive-style partitioning creates physical filesystem directories (`/date=2023-11-20/`). Partitioning by high-cardinality columns (e.g. timestamp, user_id) generates millions of tiny directories (over-partitioning), overwhelming cloud storage metadata APIs. Partitioning should only be applied to low-cardinality columns (e.g. date, region) where each partition holds at least 1GB of data.",
            "sections": [
                {
                    "title": "Simulating Partition Directory Structure on Disk",
                    "explanation": "Create an appropriately partitioned table by year and region.",
                    "code": """partition_path = "/tmp/delta_partitions_077"
if Path(partition_path).exists():
    shutil.rmtree(partition_path)

records = [
    ("ORD-1", 2023, "US", 100.0),
    ("ORD-2", 2023, "US", 250.0),
    ("ORD-3", 2023, "EU", 180.0),
    ("ORD-4", 2022, "US", 90.0),
    ("ORD-5", 2022, "EU", 320.0)
]

schema = "order_id STRING, year INT, region STRING, amount DOUBLE"
df_orders = spark.createDataFrame(records, schema)

# Partition by year and region
df_orders.write.format("delta").partitionBy("year", "region").mode("overwrite").save(partition_path)

print(f"Table partitioned by year and region at {partition_path}")
print("Generated physical partition directories:")
for p in sorted(list(Path(partition_path).glob("*/*"))):
    if p.is_dir() and not p.name.startswith("_"):
        print(f"  {p.parent.name}/{p.name}")"""
                },
                {
                    "title": "Verifying Partition Pruning in Catalyst Plan",
                    "explanation": "Filter by year and region and verify partition directory skipping in explain plan.",
                    "code": """df_pruned = spark.read.format("delta").load(partition_path).filter((F.col("year") == 2023) & (F.col("region") == "US"))

print("=== PHYSICAL PLAN (Notice PartitionFilters: [EqualTo(year,2023), EqualTo(region,US)]) ===")
df_pruned.explain(mode="formatted")
df_pruned.show()"""
                },
                {
                    "title": "Over-Partitioning Golden Rules Cheatsheet",
                    "explanation": "Review engineering guidelines for selecting partition columns.",
                    "code": """rules = [
    ("Target Partition Size", "Each physical partition directory should contain at least 1GB of data."),
    ("Max Partition Count", "Keep total partition count across the entire table under 10,000 directories."),
    ("Avoid Timestamps", "Never partition by timestamp or hour unless processing multi-terabytes per hour."),
    ("Modern Alternative", "In Databricks, prefer Liquid Clustering (`CLUSTER BY`) over traditional Hive partitioning!")
]

for title, rule in rules:
    print(f" {title}: {rule}")"""
                }
            ],
            "key_takeaways": [
                "Partition pruning skips scanning entire filesystem directories when partition columns are filtered in the WHERE clause.",
                "Over-partitioning causes millions of tiny files, slow metadata discovery, and driver out-of-memory crashes.",
                "Each partition directory should contain at least 100MB to 1GB of data."
            ],
            "interview_questions": [
                "What happens under the hood when a query filters on a partition column versus a non-partition column?",
                "What is the negative consequence of partitioning a table by `customer_id` or `timestamp`?",
                "How does Databricks Liquid Clustering eliminate the traditional dilemmas of partition key selection?"
            ]
        },
        {
            "nb_id": "078",
            "title": "Bucketing for Pre-Sorted Join Optimization and Shuffle Elimination",
            "difficulty": "Advanced",
            "business_scenario": "Two multi-billion-row fact tables (Orders and Shipments) are joined hourly in a critical financial pipeline. Because both tables are huge, broadcast joins are impossible. Every run shuffles 5TB of data across the network, consuming 40 minutes of compute.",
            "problem_statement": "Eliminate shuffle using Spark Bucketing: create bucketed tables with `bucketBy()` and `sortBy()`, configure matching bucket counts on both tables, and verify the total elimination of the Exchange (shuffle) operator from Sort-Merge Joins.",
            "architectural_notes": "Bucketing pre-shuffles and pre-sorts data on write according to a hash function of the bucket column. When two tables bucketed on the same key with identical (or multiple) bucket counts are joined, Spark reads matching bucket files directly without triggering any network shuffle (`Exchange` operator disappears).",
            "sections": [
                {
                    "title": "Creating Bucketed Orders and Shipments Tables",
                    "explanation": "Write tables bucketed into 4 buckets on join key 'order_id'.",
                    "code": """# Ensure Hive support or managed catalog enables bucketing
spark.sql("CREATE DATABASE IF NOT EXISTS bucket_db_078")
spark.sql("USE bucket_db_078")

# Create bucketed orders table
orders = [(f"ORD-{i:03d}", 100.0 + i) for i in range(1, 21)]
df_ord = spark.createDataFrame(orders, ["order_id", "amount"])

df_ord.write.format("parquet")\\
    .bucketBy(4, "order_id")\\
    .sortBy("order_id")\\
    .mode("overwrite")\\
    .saveAsTable("bucket_db_078.t_orders")

# Create bucketed shipments table with identical bucket count (4)
shipments = [(f"ORD-{i:03d}", f"CARRIER-{i%3}") for i in range(1, 21)]
df_ship = spark.createDataFrame(shipments, ["order_id", "carrier"])

df_ship.write.format("parquet")\\
    .bucketBy(4, "order_id")\\
    .sortBy("order_id")\\
    .mode("overwrite")\\
    .saveAsTable("bucket_db_078.t_shipments")

print("Both tables bucketed into 4 buckets and sorted on order_id.")"""
                },
                {
                    "title": "Verifying Shuffle Elimination in Physical Join Plan",
                    "explanation": "Join bucketed tables and inspect physical plan.",
                    "code": """# Enable bucketing optimization
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")

df_b1 = spark.table("bucket_db_078.t_orders")
df_b2 = spark.table("bucket_db_078.t_shipments")

df_joined_bucket = df_b1.join(df_b2, on="order_id", how="inner")

print("=== PHYSICAL PLAN (Notice absence of 'Exchange hashpartitioning' shuffle!) ===")
df_joined_bucket.explain(mode="simple")
df_joined_bucket.show(5)"""
                },
                {
                    "title": "Clean Up Database",
                    "explanation": "Drop temporary bucketed tables.",
                    "code": """spark.sql("DROP TABLE IF EXISTS bucket_db_078.t_orders")
spark.sql("DROP TABLE IF EXISTS bucket_db_078.t_shipments")
print("Cleaned up temporary tables.")"""
                }
            ],
            "key_takeaways": [
                "Bucketing pre-sorts and pre-distributes data on write, eliminating network shuffle during downstream joins.",
                "Both tables must be bucketed on the exact same join key and have matching or multiple bucket counts.",
                "Bucketing requires managed tables (`saveAsTable`); it does not apply to unmanaged path-based writes."
            ],
            "interview_questions": [
                "How does bucketing eliminate the `Exchange` operator in a Sort-Merge Join physical plan?",
                "What happens if two bucketed tables are joined, but Table A has 8 buckets and Table B has 16 buckets?",
                "Why is bucketing less common in modern Databricks compared to Z-Ordering and Liquid Clustering?"
            ]
        },
        {
            "nb_id": "079",
            "title": "Caching and Persistence: StorageLevel (MEMORY_AND_DISK) vs Cache Traps",
            "difficulty": "Intermediate",
            "business_scenario": "A machine learning feature engineering notebook calls `df.cache()` on a 50GB dataset. Later in the pipeline, iterative actions cause severe memory eviction, JVM garbage collection thrashing, and executor crashes. The team must learn when caching helps versus when it harms performance.",
            "problem_statement": "Master Spark DataFrame caching: compare `df.cache()` (default `MEMORY_AND_DISK_DESER`) with explicit `persist(StorageLevel)` options, inspect cache storage in Spark UI, identify common cache traps (caching intermediate one-use DataFrames), and safely unpersist (`df.unpersist()`).",
            "architectural_notes": "`cache()` is an alias for `persist(StorageLevel.MEMORY_AND_DISK)`. Caching is lazy: data is only materialized into memory when an action is executed. Caching small-to-medium DataFrames accessed multiple times saves computation; caching large DataFrames or single-use DataFrames wastes executor memory and evicts shuffle buffers.",
            "sections": [
                {
                    "title": "Persisting DataFrame with StorageLevel.MEMORY_AND_DISK",
                    "explanation": "Persist DataFrame explicitly and trigger lazy materialization with count.",
                    "code": """from pyspark import StorageLevel

df_features = spark.range(1, 50000, 1, 4).select(
    F.col("id"),
    (F.col("id") * 2.5).alias("feat_a"),
    F.sin("id").alias("feat_b")
)

# Persist with MEMORY_AND_DISK (spills to disk if RAM is exhausted rather than throwing OOM)
df_features.persist(StorageLevel.MEMORY_AND_DISK)
print(f"Is DataFrame cached: {df_features.is_cached}")

# Trigger lazy caching via action
row_count = df_features.count()
print(f"Materialized {row_count} rows into executor memory/disk storage.")"""
                },
                {
                    "title": "Iterative Consumption of Cached DataFrame",
                    "explanation": "Execute multiple downstream actions leveraging the cached snapshot without recomputing features.",
                    "code": """import time

# Action 1: Aggregation
t0 = time.time()
res1 = df_features.filter(F.col("feat_a") > 1000).count()
print(f"Action 1 (from cache): {res1} rows in {time.time() - t0:.3f}s")

# Action 2: Statistical summary
t0 = time.time()
res2 = df_features.agg(F.avg("feat_b")).first()[0]
print(f"Action 2 (from cache): avg(feat_b) = {res2:.4f} in {time.time() - t0:.3f}s")"""
                },
                {
                    "title": "Safely Unpersisting Cached Storage",
                    "explanation": "Release executor memory immediately when cached dataset is no longer needed.",
                    "code": """# Always unpersist when finished to prevent memory leaks and GC pressure
df_features.unpersist(blocking=True)
print(f"DataFrame unpersisted. Is cached: {df_features.is_cached}")"""
                }
            ],
            "key_takeaways": [
                "Only cache DataFrames that are accessed multiple times across distinct actions or iterative ML algorithms.",
                "`df.cache()` is lazy—no data is stored in memory until an action (`count()`, `show()`) is called.",
                "Always call `df.unpersist()` as soon as downstream processing of the cached DataFrame is complete."
            ],
            "interview_questions": [
                "What is the difference between `cache()` and `persist()` in PySpark?",
                "What are the different StorageLevels available in Apache Spark (MEMORY_ONLY, MEMORY_AND_DISK, _SER)?",
                "Why is caching a DataFrame that is only used once an anti-pattern?"
            ]
        },
        {
            "nb_id": "080",
            "title": "Repartition vs Coalesce Internals: Full Shuffle vs Partition Minimization",
            "difficulty": "Intermediate",
            "business_scenario": "A data pipeline writes daily reports to cloud storage. An engineer notices that writing a 5MB aggregated table produces 200 tiny files, while attempting to fix it with `repartition(1)` causes an expensive full-cluster shuffle. The team needs to choose between `repartition()` and `coalesce()` correctly.",
            "problem_statement": "Deep dive into partition restructuring: compare `repartition(numPartitions)` vs `coalesce(numPartitions)`. Explain why `repartition` triggers a full network shuffle while `coalesce` collapses existing partitions locally, and measure execution plans.",
            "architectural_notes": "`repartition()` performs a full shuffle across all cluster nodes using a HashPartitioner, creating evenly balanced partitions. It can both increase and decrease partition counts. `coalesce()` avoids a full shuffle by merging adjacent partitions on the same executor, but can ONLY decrease partition counts and can result in uneven partition sizes.",
            "sections": [
                {
                    "title": "Generating Dataset with High Partition Count",
                    "explanation": "Create dataset across 8 partitions.",
                    "code": """df_source = spark.range(1, 40001, 1, 8)
print(f"Initial Partition Count: {df_source.rdd.getNumPartitions()}")

# Inspect distribution
df_source.withColumn("pid", F.spark_partition_id()).groupBy("pid").count().orderBy("pid").show()"""
                },
                {
                    "title": "Comparing Execution Plans: coalesce(2) vs repartition(2)",
                    "explanation": "Observe absence of shuffle in coalesce vs Exchange operator in repartition.",
                    "code": """# 1. Coalesce down to 2 partitions (No shuffle!)
df_coalesced = df_source.coalesce(2)
print("=== COALESCE PHYSICAL PLAN (No Exchange/Shuffle!) ===")
df_coalesced.explain(mode="simple")
print(f"Coalesced partitions: {df_coalesced.rdd.getNumPartitions()}")

# 2. Repartition down to 2 partitions (Triggers full network shuffle!)
df_repartitioned = df_source.repartition(2)
print("=== REPARTITION PHYSICAL PLAN (Notice 'Exchange RoundRobinPartitioning'!) ===")
df_repartitioned.explain(mode="simple")
print(f"Repartitioned partitions: {df_repartitioned.rdd.getNumPartitions()}")"""
                },
                {
                    "title": "Increasing Partition Count (repartition only)",
                    "explanation": "Demonstrate that coalesce cannot increase partition count.",
                    "code": """# Attempting to increase partitions from 2 to 10 with coalesce fails silently (remains 2)
df_failed_increase = df_coalesced.coalesce(10)
print(f"coalesce(10) on 2 partitions results in: {df_failed_increase.rdd.getNumPartitions()} partitions (Cannot increase!)")

# Repartition successfully increases to 10
df_success_increase = df_coalesced.repartition(10)
print(f"repartition(10) on 2 partitions results in: {df_success_increase.rdd.getNumPartitions()} partitions (Successfully expanded!)")"""
                }
            ],
            "key_takeaways": [
                "Use `coalesce(n)` to decrease partition count before writing to disk—it eliminates network shuffles.",
                "Use `repartition(n)` to increase partition count or evenly redistribute data across executors after heavy skew.",
                "`repartition(col)` partitions data by specific column keys for downstream join or write efficiency."
            ],
            "interview_questions": [
                "What is the internal difference between `repartition()` and `coalesce()` in Apache Spark?",
                "Why can `coalesce()` lead to severe partition size imbalance if partitions are reduced too drastically?",
                "What happens if you call `coalesce(n)` where `n` is greater than the current partition count?"
            ]
        },
        {
            "nb_id": "081",
            "title": "Shuffle Partition Sizing and Tuning for Small vs Terabyte-Scale Jobs",
            "difficulty": "Intermediate",
            "business_scenario": "A 100MB dataset query takes 45 seconds because Spark defaults to 200 shuffle partitions (`spark.sql.shuffle.partitions = 200`), launching 200 tasks that each process 500KB. Conversely, a 1TB join fails with executor GC timeouts because 200 partitions result in 5GB per task. The team needs a formula to tune shuffle partition sizing.",
            "problem_statement": "Master shuffle partition sizing: calculate optimal partition counts based on shuffle input data size (targeting 100MB - 200MB per partition), configure `spark.sql.shuffle.partitions` dynamically, and compare execution metrics.",
            "architectural_notes": "Spark's default `spark.sql.shuffle.partitions = 200` was chosen arbitrarily in Spark 1.0. If shuffle partitions are too high, task scheduling overhead dominates. If too low, partition data exceeds executor RAM, triggering costly disk spills or OOM errors. Rule of thumb: $ShufflePartitions = ShuffleDataSize / 150MB$.",
            "sections": [
                {
                    "title": "Sizing Formula Cheatsheet",
                    "explanation": "Review mathematical guidelines for sizing shuffle partitions.",
                    "code": """# Tuning Formula:
# Target Partition Size = 100MB to 200MB
# Shuffle Partitions = Total Input Data at Shuffle / 150MB
# Example: 10GB dataset -> 10,000MB / 150MB ≈ 67 partitions
# Example: 500GB dataset -> 500,000MB / 150MB ≈ 3,333 partitions

print("Shuffle partition calculation guidelines loaded.")"""
                },
                {
                    "title": "Benchmarking Small Query with Default (200) vs Tuned (4) Partitions",
                    "explanation": "Observe performance differences on small dataset.",
                    "code": """import time

df_small = spark.range(1, 50000, 1, 4).withColumn("group", F.col("id") % 10)

# Run with default 200 shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")
t0 = time.time()
c1 = df_small.groupBy("group").count().collect()
t_default = time.time() - t0
print(f"200 Partitions Execution Time: {t_default:.3f}s (High task scheduling overhead)")

# Run with tuned 4 shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "4")
t0 = time.time()
c2 = df_small.groupBy("group").count().collect()
t_tuned = time.time() - t0
print(f"4 Partitions Execution Time: {t_tuned:.3f}s (Speedup: {t_default/max(t_tuned, 0.001):.1f}x)")"""
                },
                {
                    "title": "Dynamic Partitioning via Adaptive Query Execution (AQE)",
                    "explanation": "Enable AQE to automate partition sizing without manual guessing.",
                    "code": """# With AQE enabled, Spark automatically coalesces post-shuffle partitions to ~64MB target size
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(64 * 1024 * 1024)) # 64MB advisory size

print("AQE advisory partition size configured to 64MB!")"""
                }
            ],
            "key_takeaways": [
                "Target between 100MB and 200MB of compressed data per shuffle partition task.",
                "Default 200 shuffle partitions hurts small queries (< 1GB) and crashes large queries (> 500GB).",
                "Use AQE (`spark.sql.adaptive.advisoryPartitionSizeInBytes`) to let Spark dynamically calculate optimal partition counts."
            ],
            "interview_questions": [
                "How do you determine the optimal value for `spark.sql.shuffle.partitions` for a 500GB dataset?",
                "What happens if `spark.sql.shuffle.partitions` is set too low versus set too high?",
                "How does `spark.sql.adaptive.advisoryPartitionSizeInBytes` interact with shuffle partitions?"
            ]
        },
        {
            "nb_id": "082",
            "title": "Diagnosing and Fixing OutOfMemory (OOM) Errors: Driver vs Executor Heap",
            "difficulty": "Advanced",
            "business_scenario": "A production ETL job intermittently crashes with `java.lang.OutOfMemoryError: Java heap space`. On-call engineers arbitrarily double the cluster size, increasing cloud costs by $10,000 without resolving the root cause.",
            "problem_statement": "Diagnose and resolve Spark OutOfMemory (OOM) failures: differentiate between Driver OOM and Executor OOM, identify anti-patterns (e.g. `df.collect()` on big data, unconstrained broadcast joins, key skew in `groupBy`), and apply memory-safe architectural fixes.",
            "architectural_notes": "Driver OOM occurs when the master node runs out of memory, usually caused by collecting large DataFrames (`collect()`, `toPandas()`), broadcasting tables larger than driver memory, or accumulating massive lineage DAGs. Executor OOM occurs when worker tasks process partitions exceeding available JVM heap or off-heap memory.",
            "sections": [
                {
                    "title": "Driver OOM vs Executor OOM Root Cause Diagnostic Matrix",
                    "explanation": "Review architectural causes and solutions for memory crashes.",
                    "code": """matrix = [
    ("Driver OOM", "Calling df.collect() or df.toPandas() on millions of rows", "Use df.take(n), write to Delta, or use limit()"),
    ("Driver OOM", "Broadcasting table larger than spark.driver.memory", "Disable broadcast or increase autoBroadcastJoinThreshold check"),
    ("Driver OOM", "Chaining 100+ withColumn transformations (deep DAG)", "Break lineage using checkpoint() or write intermediate Delta table"),
    ("Executor OOM", "Severe data skew (millions of rows on one join key)", "Apply Salting or enable AQE skew join"),
    ("Executor OOM", "Shuffle partitions too low (5GB per partition)", "Increase spark.sql.shuffle.partitions to target 150MB/partition"),
    ("Executor OOM", "collect_list() on high-cardinality skewed key", "Filter before collect or use window functions")
]

schema = "failure_type STRING, common_root_cause STRING, architectural_remediation STRING"
df_oom_matrix = spark.createDataFrame(matrix, schema)
df_oom_matrix.show(truncate=False)"""
                },
                {
                    "title": "Demonstrating Memory-Safe Sampling vs Unsafe collect()",
                    "explanation": "Replace dangerous full collect with safe streaming limit and take.",
                    "code": """df_large_mock = spark.range(1, 1000000, 1, 4)

# UNSAFE: df_large_mock.collect()  <- Pulls all 1,000,000 objects into driver JVM heap!

# SAFE 1: take(n) only collects requested records
sample_rows = df_large_mock.take(5)
print(f"Safely collected {len(sample_rows)} rows without loading full dataset into driver memory.")

# SAFE 2: toLocalIterator() streams partition-by-partition to driver
iterator = df_large_mock.limit(10).toLocalIterator()
first_three = [next(iterator).id for _ in range(3)]
print("First 3 items via streaming iterator:", first_three)"""
                },
                {
                    "title": "Breaking Deep Lineage Trees with Checkpoint",
                    "explanation": "Truncate deep Catalyst DAG execution graphs to prevent driver stack overflows.",
                    "code": """# Set checkpoint directory
spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoints_082")

df_deep = spark.range(1, 100)
for _ in range(10):
    df_deep = df_deep.filter(F.col("id") > 0)

# checkpoint() truncates logical plan, saving current RDD to storage and resetting lineage
df_checkpointed = df_deep.checkpoint()
print("Lineage graph successfully truncated with checkpoint()!")"""
                }
            ],
            "key_takeaways": [
                "Driver OOM is caused by `.collect()`, `.toPandas()`, oversized broadcast tables, or deep Catalyst plans.",
                "Executor OOM is caused by key skew, insufficient shuffle partitions, or large in-memory window buffers.",
                "Use `checkpoint()` to truncate deep lineage graphs in iterative ML or recursive algorithms."
            ],
            "interview_questions": [
                "How do you determine from Spark UI whether an OOM error occurred on the Driver or on an Executor?",
                "What is the difference between `df.cache()` and `df.checkpoint()` in terms of lineage truncation?",
                "How does off-heap memory (`spark.memory.offHeap.enabled`) prevent JVM garbage collection pauses?"
            ]
        },
        {
            "nb_id": "083",
            "title": "Spark UI, EventLog Metrics, Task Skew and Shuffle Spill Analysis",
            "difficulty": "Advanced",
            "business_scenario": "A mission-critical nightly reconciliation job slows down by 3x on weekends. The senior engineer must inspect Spark execution metrics to identify whether the slowdown is caused by garbage collection, shuffle spill (Memory vs Disk), task skew, or CPU starvation.",
            "problem_statement": "Analyze Spark execution metrics programmatically and via Spark UI: identify Shuffle Spill (Memory) and Shuffle Spill (Disk), detect task stragglers, interpret execution timelines, and query Spark listener bus metrics.",
            "architectural_notes": "Shuffle Spill occurs when an executor's execution memory buffer cannot hold intermediate shuffle or aggregation data. Spark first spills to uncompressed memory buffer (`Spill (Memory)`), and if that exceeds limits, writes compressed data to local executor scratch disk (`Spill (Disk)`), destroying I/O performance.",
            "sections": [
                {
                    "title": "Spark UI Diagnostic Checklist Blueprint",
                    "explanation": "Review key tabs and metric indicators in the Spark UI.",
                    "code": """ui_checklist = [
    ("Jobs Tab", "Identify which Job ID took the most duration; verify stage dependency boundaries."),
    ("Stages Tab", "Inspect Task Duration Summary (Min, Median, 75th, Max). If Max >> Median, you have TASK SKEW!"),
    ("Spill (Memory/Disk)", "Indicates executor RAM was overwhelmed. Increase shuffle partitions or executor memory."),
    ("Task GC Time", "If GC time > 10% of total task time, executors suffer from heap thrashing. Increase memory or reduce object creation."),
    ("Executors Tab", "Check for dead executors, failed tasks, and uneven distribution of task counts across nodes.")
]

for tab, guidance in ui_checklist:
    print(f"[{tab}]: {guidance}")"""
                },
                {
                    "title": "Programmatically Inspecting Stage & Execution Metrics",
                    "explanation": "Query the Spark status tracker to extract task execution metrics.",
                    "code": """# Run a sample wide-shuffle aggregation to generate metrics
df_metrics_job = spark.range(1, 100000, 1, 4).groupBy(F.col("id") % 10).count()
df_metrics_job.collect()

# Query SparkContext StatusTracker
status_tracker = spark.sparkContext.statusTracker()
active_jobs = status_tracker.getActiveJobIds()
print(f"Active Job IDs: {list(active_jobs)}")
print("StatusTracker accessible for runtime telemetry capture.")"""
                },
                {
                    "title": "Remediation Strategy for Shuffle Spill",
                    "explanation": "Demonstrate configuration adjustments to eliminate shuffle spill.",
                    "code": """# Remediation for Shuffle Spill:
# 1. Increase shuffle partitions to reduce data per partition:
spark.conf.set("spark.sql.shuffle.partitions", "16")

# 2. Adjust fraction of heap dedicated to execution/storage:
# spark.conf.set("spark.memory.fraction", "0.7")

# 3. Ensure AQE is enabled:
spark.conf.set("spark.sql.adaptive.enabled", "true")

print("Shuffle spill mitigation configurations applied.")"""
                }
            ],
            "key_takeaways": [
                "Shuffle Spill (Memory/Disk) in the Spark UI is the #1 indicator of undersized shuffle partitions.",
                "Compare 75th percentile task duration to Max task duration in the Stages tab to detect task skew.",
                "If GC time exceeds 10% of task duration, executor JVMs are thrashing; tune off-heap memory or partition sizing."
            ],
            "interview_questions": [
                "What is the difference between `Spill (Memory)` and `Spill (Disk)` in the Spark UI Stages tab?",
                "How do you identify a data skew issue versus an undersized cluster issue using Spark UI metrics?",
                "What does high 'Task Deserialization Time' indicate in the Spark UI timeline view?"
            ]
        },
        {
            "nb_id": "084",
            "title": "Vectorized Execution and Photon Engine Acceleration Mechanics",
            "difficulty": "Advanced",
            "business_scenario": "An enterprise evaluates migrating high-throughput SQL analytics from classic Apache Spark clusters to Databricks clusters with Photon Engine enabled. Leadership requires an architectural analysis of C++ vectorized execution vs JVM row-based execution.",
            "problem_statement": "Deep dive into Vectorized Execution and Databricks Photon: understand CPU cache locality, SIMD vectorization, columnar in-memory processing, identify operators supported by Photon, and analyze execution speedup profiles.",
            "architectural_notes": "Classic Apache Spark compiles queries into Java bytecode running inside JVMs. Databricks Photon is a vectorized execution engine written from scratch in C++. It operates on columnar batches in CPU L1/L2 caches and uses SIMD (Single Instruction, Multiple Data) instructions, accelerating SQL and aggregation workloads by 2x to 5x.",
            "sections": [
                {
                    "title": "Architectural Comparison: Classic JVM Spark vs Databricks Photon",
                    "explanation": "Analyze the fundamental architectural differences between engines.",
                    "code": """comparison = [
    ("Core Language", "Java / Scala (JVM Bytecode)", "C++ (Native Machine Code)"),
    ("Execution Model", "Tungsten Whole-Stage CodeGen", "Vectorized Columnar Batch Processing"),
    ("CPU Optimization", "Standard JVM register usage", "SIMD (Single Instruction Multiple Data)"),
    ("Memory Management", "JVM Garbage Collection & Off-Heap", "Direct C++ Memory Management (Zero GC overhead)"),
    ("Supported Workloads", "General PySpark, RDDs, Custom UDFs", "SQL, Aggregations, Joins, Parquet/Delta I/O")
]

schema = "dimension STRING, classic_spark STRING, databricks_photon STRING"
df_comp = spark.createDataFrame(comparison, schema)
df_comp.show(truncate=False)"""
                },
                {
                    "title": "Benchmarking Vector-Friendly Aggregations in PySpark",
                    "explanation": "Construct queries optimized for vectorized batch execution.",
                    "code": """import time

# Vectorized engines excel at high-speed columnar filtering and math
df_vec_bench = spark.range(1, 1000000, 1, 4).select(
    F.col("id"),
    (F.col("id") * 1.5).alias("price"),
    (F.col("id") % 5).alias("category")
)

t0 = time.time()
res = df_vec_bench.filter(F.col("price") > 500000).groupBy("category").agg(F.sum("price"), F.avg("price")).collect()
elapsed = time.time() - t0

print(f"Columnar Vector-Friendly Aggregation evaluated in {elapsed:.3f}s")"""
                },
                {
                    "title": "Photon Compatibility Best Practices",
                    "explanation": "Identify anti-patterns that force Photon to fall back to classic JVM execution.",
                    "code": """rules = [
    ("Native Built-ins", "Always use native Spark SQL functions; Photon accelerates 100% of built-in expressions."),
    ("Avoid Non-Vectorized UDFs", "Standard Python @udf forces Photon to serialize data back into Python subprocesses!"),
    ("Parquet / Delta Storage", "Photon reads directly from Parquet and Delta columnar formats with zero-copy decoding.")
]

for title, rule in rules:
    print(f" {title}: {rule}")"""
                }
            ],
            "key_takeaways": [
                "Photon is Databricks' vectorized query engine written in C++ for maximum CPU cache locality and SIMD execution.",
                "Photon eliminates Java Virtual Machine garbage collection pauses entirely for data operations.",
                "Custom Python UDFs disable Photon acceleration for that stage, forcing fallback to classic Spark JVM."
            ],
            "interview_questions": [
                "What is Databricks Photon and how does it differ from classic Apache Spark Tungsten Whole-Stage CodeGen?",
                "What is SIMD (Single Instruction, Multiple Data) and how does vectorized execution leverage it?",
                "Which query operations benefit most from Photon, and which operations force a fallback to the JVM?"
            ]
        },
        {
            "nb_id": "085",
            "title": "Fixing Accidental Cartesian Products and BroadcastNestedLoopJoins",
            "difficulty": "Advanced",
            "business_scenario": "A junior engineer submits a join query where the join condition accidentally uses an OR operator across two conditions: `ON (a.id = b.id OR a.email = b.email)`. The job hangs indefinitely because Catalyst cannot use hash joins or sort-merge joins, falling back to a catastrophic Cartesian product.",
            "problem_statement": "Detect and fix accidental Cartesian products: identify `BroadcastNestedLoopJoin` and `CartesianProduct` operators in execution plans, understand why non-equi joins and OR join conditions disable hash joins, and rewrite queries using UNIONs or window functions.",
            "architectural_notes": "SortMergeJoin and BroadcastHashJoin strictly require equi-join conditions (`=`). When a join predicate contains an `OR`, `<>`, or non-equi condition without an equality key, Spark must fall back to a Cartesian Product (complexity $O(N \times M)$), generating quadratic row comparisons.",
            "sections": [
                {
                    "title": "Simulating Accidental Cartesian Join Predicate",
                    "explanation": "Create dataset and inspect Cartesian product plan.",
                    "code": """df1 = spark.createDataFrame([(1, "alice@a.com"), (2, "bob@b.com")], ["id", "email"])
df2 = spark.createDataFrame([(1, "alice@work.com"), (3, "bob@b.com")], ["id", "email"])

# Dangerous Join: Joining with OR condition disables Hash Join!
dangerous_join = df1.join(df2, (df1["id"] == df2["id"]) | (df1["email"] == df2["email"]), how="inner")

print("=== DANGEROUS PHYSICAL PLAN (Notice BroadcastNestedLoopJoin or CartesianProduct!) ===")
dangerous_join.explain(mode="simple")"""
                },
                {
                    "title": "Refactoring Anti-Pattern Using Multiple Equi-Joins and Union",
                    "explanation": "Rewrite OR join into two fast equi-joins combined with distinct union.",
                    "code": """# Optimized Architecture: Split OR condition into two separate Equi-Joins and Union
# 1. Match on ID (fast HashJoin)
df_match_id = df1.join(df2, on="id", how="inner").select(df1["id"], df1["email"])

# 2. Match on Email (fast HashJoin)
df_match_email = df1.join(df2, on="email", how="inner").select(df1["id"], df1["email"])

# 3. Combine with distinct to deduplicate matches
df_optimized = df_match_id.unionByName(df_match_email).distinct()

print("=== OPTIMIZED PHYSICAL PLAN (Both branches use high-speed Hash Joins!) ===")
df_optimized.explain(mode="simple")
df_optimized.show()"""
                },
                {
                    "title": "Validation & Complexity Comparison",
                    "explanation": "Verify both queries return identical business results with radically different complexity.",
                    "code": """print("Both implementations produce identical business results, but the optimized query scales linearly O(N) instead of quadratically O(N*M)!")"""
                }
            ],
            "key_takeaways": [
                "Joining on non-equi conditions or OR predicates forces Spark into `CartesianProduct` or `BroadcastNestedLoopJoin`.",
                "Cartesian products exhibit $O(N \times M)$ complexity and will stall clusters on production datasets.",
                "Refactor OR join conditions into separate equi-join queries combined with `union().distinct()`."
            ],
            "interview_questions": [
                "Why does an `OR` in a join condition prevent Spark from using Sort-Merge Join or Broadcast Hash Join?",
                "What is a `BroadcastNestedLoopJoin` and under what conditions does Catalyst choose it?",
                "How do you rewrite a non-equi join query to enable efficient hash joining?"
            ]
        }
    ]

    for nb in notebooks:
        nb_json = build_notebook(
            nb_id=nb["nb_id"],
            title=nb["title"],
            module_folder=MODULE_FOLDER,
            module_name=MODULE_NAME,
            difficulty=nb["difficulty"],
            yt_ref=nb["yt_ref"],
            business_scenario=nb["business_scenario"],
            problem_statement=nb["problem_statement"],
            architectural_notes=nb["architectural_notes"],
            sections=nb["sections"],
            key_takeaways=nb["key_takeaways"],
            interview_questions=nb["interview_questions"]
        )
        filename = sanitize_filename(nb["nb_id"], nb["title"])
        write_notebook(out_dir / filename, nb_json)
    print(f"Generated {len(notebooks)} notebooks for {MODULE_NAME}")

if __name__ == "__main__":
    generate(Path(__file__).parent.parent)
