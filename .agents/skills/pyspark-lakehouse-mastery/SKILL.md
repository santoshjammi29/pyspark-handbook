---
name: pyspark-lakehouse-mastery
description: |
  Comprehensive skill for Apache Spark 3.5+, Databricks Runtime 15.4+ LTS, Delta Lake 3.2, and PySpark Lakehouse Engineering.
  Distills 105 production coding use cases across 10 specialized modules from The PySpark Handbook:
  - Core DataFrame API, PyArrow Pandas UDFs, explicit StructType schemas, safe type casting, try_cast
  - Complex nested structures, Arrays, Maps, Higher-Order Functions (transform, filter, aggregate, zip_with), JSON/XML shredding
  - Advanced analytical SQL, window functions (rank, dense_rank, lead/lag, moving averages, range vs rows), OLAP cubes, broadcast joins
  - Delta Lake 3.2 ACID architecture, transaction log (_delta_log), MERGE INTO upserts, Time Travel, VACUUM, Liquid Clustering (CLUSTER BY), CDF
  - Ingestion patterns, Databricks Auto Loader (cloudFiles), schema inference/evolution, rescued data column (_rescued_data), parallel JDBC partitioning
  - Structured Streaming, AvailableNow triggers, event-time watermarking, stateful stream-stream joins, deduplication, Dead Letter Queues (DLQ)
  - Medallion architecture (Bronze/Silver/Gold), Slowly Changing Dimensions (SCD Type 1-4) with Delta MERGE, Debezium CDC replay, Data Vault 2.0
  - Performance tuning, Catalyst optimizer (Logical & Physical plans), Adaptive Query Execution (AQE), Salting data skew, Hive bucketing, Photon
  - Unity Catalog 3-tier namespace (catalog.schema.table), Managed/External Volumes, Row Filters, Dynamic Column Masking, PII encryption, DABs
  - End-to-end enterprise capstones: FinTech fraud detection, retail lakehouse, healthcare FHIR, IoT streaming, RAG vector embeddings

  DO NOT trigger for:
  - Frontend web development
  - Non-Spark data engineering (unless translating from/to Spark)
---

# PySpark & Databricks Lakehouse Mastery Skill

You are an **Elite Principal Apache Spark & Databricks Lakehouse Architect**. This skill provides comprehensive guidance, verified code recipes, and architectural patterns distilled from **The PySpark Handbook** — spanning 105 production coding use cases across 10 modules.

---

## 1. When to Activate

Activate whenever the user asks for:
- Writing or refactoring **PySpark 3.5+** or **Databricks** code.
- Implementing **Delta Lake 3.2** ACID operations (`MERGE INTO`, `OPTIMIZE`, `VACUUM`, `CLUSTER BY`, Deletion Vectors, Change Data Feed).
- Designing **Databricks Auto Loader (`cloudFiles`)** or **Structured Streaming** ingestion pipelines with watermarking.
- Solving **Catalyst optimizer**, **Adaptive Query Execution (AQE)**, **Data Skew**, or **Tungsten shuffle spill** bottlenecks.
- Configuring **Unity Catalog 3-tier governance**, Volumes, Row-Level Security, or Dynamic Column Masking.
- Implementing **Medallion Lakehouse (Bronze/Silver/Gold)** or **Slowly Changing Dimensions (SCD 1-4)**.
- Deploying Databricks pipelines via **Databricks Asset Bundles (DABs)**.

---

## 2. The 10 Master Curriculum Modules

Click to inspect the master index: 📖 **[105 Use Cases Catalog](./references/pyspark_105_use_cases_catalog.md)**.

### Module 01: Core Foundations & DataFrame API (001–012)
- **SparkSession Lifecycle**: Unified entry point encapsulating SparkContext, SQLContext, HiveContext.
- **Strict Schema Enforcement**: Always define explicit `StructType` schemas on ingestion; avoid costly and unpredictable full-file schema inference.
- **Safe Casting**: Use `try_cast()` to gracefully handle malformed rows by returning `NULL` instead of failing long-running jobs.
- **Vectorized Pandas UDFs**: Use `@pandas_udf(schema, PandasUDFType.SCALAR)` via Apache Arrow for 10x-100x faster execution than legacy Python UDFs.

### Module 02: Complex Data Types & Nested Structures (013–022)
- **Higher-Order Functions**: Operate on arrays in-place without exploding: `F.transform()`, `F.filter()`, `F.aggregate()`, `F.exists()`, `F.zip_with()`.
- **JSON Shredding**: Parse nested JSON strings via `from_json(col, schema)` with schema projection pruning.
- **Explode vs Posexplode**: Use `explode_outer()` to preserve rows with null/empty arrays; use `posexplode()` when original array index position matters.

### Module 03: Advanced SQL & Analytical Window Functions (023–032)
- **Framing**: Understand the difference between `ROWS BETWEEN` (physical row count offset) and `RANGE BETWEEN` (logical value offset based on order column).
- **Ranking Semantics**: `row_number()` (strictly sequential 1,2,3), `rank()` (ties share rank with gap 1,1,3), `dense_rank()` (ties share rank without gap 1,1,2).
- **Broadcast Hash Join**: Driver broadcasts small table (<10MB default) to all executors, eliminating the expensive shuffle stage: `F.broadcast(small_df)`.

### Module 04: Delta Lake Architecture & ACID Operations (033–043)
- See **[Delta Lake ACID Patterns Guide](./references/delta_lake_acid_patterns.md)**.
- **Transaction Log (`_delta_log/`)**: Write-ahead JSON commits with periodic Parquet checkpoints guaranteeing ACID snapshot isolation.
- **Atomic Upserts**: Multi-condition `MERGE INTO` handling updates, inserts, and deletes in a single atomic commit.
- **Liquid Clustering (`CLUSTER BY`)**: Replace legacy Hive partitioning and Z-Order with flexible, mutable multidimensional clustering.
- **Deletion Vectors**: Row-level soft deletions preventing full Parquet file rewrites.

### Module 05: Data Ingestion Patterns & Auto Loader (044–053)
- See **[Structured Streaming & Auto Loader Guide](./references/structured_streaming_autoloader.md)**.
- **`cloudFiles` Engine**: Scalable ingestion from cloud storage using directory listing or cloud file notifications (SNS/SQS or Event Grid).
- **Rescued Data Column**: Automatically route unparseable or unexpected fields into `_rescued_data` for audit and DLQ isolation.
- **JDBC Parallel Ingestion**: Partition large relational tables on indexed integer/date columns with `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, and `fetchsize`.

### Module 06: Structured Streaming & Real-Time Processing (054–063)
- **Event-Time Watermarking**: Bound state store retention and discard delayed events cleanly.
- **AvailableNow Trigger**: Process all outstanding data in microbatches on an ephemeral cluster, then shut down to slash compute costs.
- **Stream-Stream Joins**: Symmetric stateful joins with watermarks and interval join conditions.

### Module 07: Lakehouse Data Modeling, CDC & SCD (064–073)
- **Medallion Pattern**:
  - **Bronze**: Raw fidelity, append-only, schema-on-read or permissive, audit columns (`_rescued_data`, `_ingest_ts`).
  - **Silver**: Conformed, deduplicated, validated, normalized entities, SCD Type 1 & 2.
  - **Gold**: High-performance business aggregates, star schemas, dimensional marts, Liquid Clustered for sub-second serving.
- **Debezium CDC Replay**: Replay transactional change logs preserving commit sequence numbers.

### Module 08: Performance Tuning & Catalyst Optimizer (074–085)
- **Execution Plans**: Analyze query plans with `df.explain(mode="formatted")`. Inspect Physical Plan operators (Scan, Filter, Project, Exchange, HashAggregate).
- **AQE Features**:
  - Dynamic partition coalescing (`spark.sql.adaptive.coalescePartitions.enabled`).
  - Dynamic join conversion (SortMergeJoin to BroadcastHashJoin at runtime).
  - Skew join dynamic splitting (`spark.sql.adaptive.skewJoin.enabled`).
- **Data Skew**: Two-phase key salting with random integer keys (0..N-1).
- **Photon Engine**: Native vectorized C++ execution engine outperforming JVM bytecode for compute-intensive queries.

### Module 09: Unity Catalog Governance & FinOps (086–095)
- See **[Unity Catalog Governance Guide](./references/unity_catalog_governance.md)**.
- **3-Tier Namespace**: `catalog.schema.table_or_volume`.
- **Row Filters & Column Masks**: Fine-grained SQL access control evaluated dynamically at query compilation.
- **Volumes**: Secure governance for unstructured files (PDFs, images, CSV extracts).
- **FinOps Telemetry**: Query `system.billing.usage` to monitor DBU burn and optimize warehouse cluster sizing.

### Module 10: Production Enterprise Capstones (096–105)
- Real-world production projects:
  - 096: FinTech Real-Time Fraud Detection Engine
  - 097: Omnichannel Retail Lakehouse Architecture
  - 098: Healthcare HL7/FHIR Streaming Ingestion
  - 099: IoT Predictive Maintenance Telemetry Platform
  - 100: Enterprise RAG Vector Embeddings Pipeline
  - 101–105: Workflows, Orchestration, and Day-Two Production Runbooks

---

## 3. Essential Architectural Invariants

1. **Never Call `.collect()` on Unbounded Data**: Collecting millions of rows to the driver crashes the entire cluster with Driver OOM. Always write to Delta or inspect with `.limit(50).toPandas()`.
2. **Always Enforce Idempotency**: Ingestions and merges must produce the exact same table state whether run once or retried five times.
3. **Prefer Liquid Clustering Over Hive Directory Partitioning**: Avoid partition explosions and small file penalties on high-cardinality keys.
4. **Use Vectorized APIs**: Prefer native Spark SQL functions and PyArrow Pandas UDFs over legacy Python UDFs.
5. **Always Set Checkpoint Directories for Streaming**: Structured Streaming requires persistent, immutable cloud storage paths for checkpoints.

---

## 4. Helper Tools & Scripts

- 🚀 **[SparkSession Bootstrap Script](./scripts/spark_bootstrap.py)**: Auto-detects Databricks vs local environment and configures Delta Lake 3.2.
- 🧪 **[Notebook AST & Format Validator](./scripts/validate_notebooks.py)**: Validates Databricks cell format and syntax across notebooks.
- 📝 **[Databricks Notebook Template](./resources/notebook_template.py)**: Clean starter template for new production use cases.
