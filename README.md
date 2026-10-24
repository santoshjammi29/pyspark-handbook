<div align="center">

# ⚡ The PySpark Handbook

**105 Production Coding Use Cases, Performance Tuning Recipes & Lakehouse Masterclass**

[![Stars](https://img.shields.io/github/stars/santoshjammi29/pyspark-handbook?style=for-the-badge&logo=github&color=FFD700)](https://github.com/santoshjammi29/pyspark-handbook/stargazers)
[![Forks](https://img.shields.io/github/forks/santoshjammi29/pyspark-handbook?style=for-the-badge&logo=github&color=blue)](https://github.com/santoshjammi29/pyspark-handbook/network/members)
[![CI Notebook Validation](https://github.com/santoshjammi29/pyspark-handbook/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshjammi29/pyspark-handbook/actions)
[![Databricks](https://img.shields.io/badge/Databricks-Native_.py_Notebooks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5%2B-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2_ACID-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-008080?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Python](https://img.shields.io/badge/Python-3.9_--_3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg?style=for-the-badge)](LICENSE)
[![Notebook Count](https://img.shields.io/badge/Notebooks-105_Complete-success.svg?style=for-the-badge)](#-master-curriculum-catalog-of-105-use-cases)

*The definitive enterprise reference repository of 105 battle-tested Apache Spark & Databricks Lakehouse coding use cases.*  
*Structured in the official **Databricks Source Notebook format (`.py`)** for seamless 1-click sync with **Databricks Repos / Git Folders**, **Databricks Asset Bundles (DABs)**, **VS Code**, and local PySpark / CI runtimes.*

[📖 Curriculum Catalog](#-master-curriculum-catalog-of-105-use-cases) • [⚡ Quick Cheat Sheet](#-quick-cheat-sheet--tldr) • [🎯 Interview Matrix](#-senior-data-engineer-interview-matrix) • [🚀 Quickstart](#-databricks--local-quickstart)

---

</div>

## 🎯 Executive Summary & Mission

Modern Lakehouse engineering demands deep fluency in distributed memory execution, ACID transaction semantics, streaming ingestion, query plan optimization, and enterprise data governance. 

This repository provides **105 end-to-end, runnable, modular use cases** designed for:
- **Senior & Staff Data Engineers:** Mastering complex data types, query execution tuning (AQE, DPP), join skew resolution (salting), and high-throughput Delta MERGE operations.
- **Lakehouse & Solution Architects:** Implementing production Medallion Architectures (Bronze → Silver → Gold), Change Data Capture (CDC), Slowly Changing Dimensions (SCD Types 1, 2, 3, 4), and Unity Catalog governance.
- **Interview Candidates:** Preparing for senior/lead technical interviews with real-world scenarios, architectural trade-offs, and 300+ interview questions.

---

## 🧭 Interactive Curriculum Navigation

Click any module below to jump directly to its complete catalog of notebooks, business scenarios, and architectural takeaways:

| Learning Track & Module | Notebooks | Difficulty Level | Core Architectural Coverage |
|:---|:---:|:---:|:---|
| 📊 **[Core PySpark Foundations & DataFrame API](#module-01)** | `001 – 012` | 🟢 Beginner – 🟡 Intermediate | Core DataFrame manipulation, explicit StructType schemas, PyArrow Pandas UDFs, safe type casting, column projections, and Databricks dbutils emulation. |
| 🧩 **[Complex Data Types & Nested Structures Handling](#module-02)** | `013 – 022` | 🟡 Intermediate | Nested StructTypes, Arrays, Maps, Higher-Order Functions (transform, filter, aggregate), JSON parsing, XML shredding, and schema evolution. |
| 🪟 **[Advanced SQL, Aggregations & Analytical Window Functions](#module-03)** | `023 – 032` | 🟡 Intermediate – 🔴 Advanced | Ranking, lead/lag, rolling moving averages, cumulative distribution, OLAP cubes, pivots, broadcast hash joins, and range vs rows framing. |
| 🧱 **[Delta Lake Architecture, ACID Operations & Time Travel](#module-04)** | `033 – 043` | 🟡 Intermediate – 🔴 Advanced | Delta transaction log (_delta_log JSON & checkpoints), ACID CRUD, high-throughput MERGE INTO, time travel, VACUUM retention, and Liquid Clustering. |
| 📥 **[Data Ingestion Patterns, Auto Loader & File Formats](#module-05)** | `044 – 053` | 🟡 Intermediate – 🔴 Advanced | Databricks Auto Loader (cloudFiles), notification modes, rescued data column, parallel JDBC partitioning, and multi-format lakehouse ingestion. |
| ⚡ **[Streaming & Near Real-Time Processing](#module-06)** | `054 – 063` | 🟡 Intermediate – 🔴 Advanced | Structured Streaming, AvailableNow triggers, event-time watermarking, stateful stream-stream joins, deduplication, and Dead Letter Queues (DLQ). |
| 🏛️ **[Lakehouse Data Modeling, CDC & SCD Implementation](#module-07)** | `064 – 073` | 🔴 Advanced | Medallion Architecture (Bronze/Silver/Gold), Slowly Changing Dimensions (SCD Type 1, 2, 3, 4), Debezium CDC replay, and Data Vault 2.0. |
| 🏎️ **[Performance Tuning, Query Optimization & Skew Mitigation](#module-08)** | `074 – 085` | 🔴 Advanced | Catalyst Logical & Physical Plans, Adaptive Query Execution (AQE), Salting join skew, Hive-style bucketing, OOM diagnosis, and Photon. |
| 🛡️ **[Databricks Platform, Unity Catalog, Security & FinOps](#module-09)** | `086 – 095` | 🟡 Intermediate – 🔴 Advanced | Unity Catalog 3-tier namespace, Managed & External Volumes, Row-Level Security, Dynamic Column Masking, Secret Scopes, and Databricks Asset Bundles. |
| 🚀 **[End-to-End Enterprise Projects & Production Workflows](#module-10)** | `096 – 105` | 🔴 Advanced Capstone | Full capstones: FinTech Fraud Detection, Omnichannel Retail Lakehouse, Healthcare FHIR Ingestion, IoT Telemetry, RAG Embeddings, and Workflows. |

---

## 🏛️ Lakehouse Architecture & Visual Designs

### 1. The Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                  MEDALLION LAKEHOUSE ARCHITECTURE                                       │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

  ┌───────────────────┐
  │   RAW SOURCES     │
  │ • Cloud Storage   │
  │ • IoT Telemetry   │
  │ • Debezium CDC    │
  │ • REST APIs / DBs │
  └─────────┬─────────┘
            │
            ▼   [Auto Loader / cloudFiles: Schema Inference, Rescued Data, Incremental Notification]
  ┌───────────────────────────────────────────────────────────────────────────────────────┐
  │ 🥉 BRONZE LAYER (Raw Ingestion / Raw Lakehouse Storage)                               │
  │ • Raw source fidelity, append-only Delta tables                                       │
  │ • System metadata columns (_rescued_data, _input_file_name, _ingest_timestamp)        │
  │ • Fast ingestion without expensive transformations                                    │
  └─────────┬─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼   [Delta MERGE INTO, Deduplication, Safe Type Casting, Quarantine Rules, Watermarking]
  ┌───────────────────────────────────────────────────────────────────────────────────────┐
  │ 🥈 SILVER LAYER (Cleaned, Conformed, Enriched Lakehouse Model)                        │
  │ • Cleansed, validated, and normalized tables with strict schema enforcement            │
  │ • Slowly Changing Dimensions (SCD Type 1 & Type 2), CDC upserts, Conformed Dimensions │
  │ • Corrupted records routed to Dead Letter Queues (DLQ)                                 │
  └─────────┬─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼   [Aggregations, Window Functions, Dimensional Modeling, Star/Snowflake Schema, Caching]
  ┌───────────────────────────────────────────────────────────────────────────────────────┐
  │ 🥇 GOLD LAYER (Curated Business & Analytics Marts)                                    │
  │ • Aggregated KPIs, summary tables, OLAP cubes, customer 360 views                     │
  │ • Highly optimized with Z-Ordering / Liquid Clustering for sub-second BI latency       │
  │ • Powering BI dashboards (Tableau/PowerBI), Feature Stores, and GenAI / RAG pipelines │
  └───────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2. Databricks Unity Catalog 3-Tier Governance Namespace

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  UNITY CATALOG METASTORE                                         │
│                      (Centralized Governance, Auditing, Lineage & IAM)                           │
└───────────────────────────────────┬──────────────────────────────────────────────────────────────┘
                                    │
    ┌───────────────────────────────┴───────────────────────────────┐
    │                                                               │
    ▼                                                               ▼
┌──────────────────────────────────────────────┐  ┌──────────────────────────────────────────────┐
│ CATALOG: `production_lakehouse`             │  │ CATALOG: `development_sandbox`                │
└───────────────────┬──────────────────────────┘  └───────────────────┬──────────────────────────┘
                    │                                                 │
    ┌───────────────┴───────────────┐                 ┌───────────────┴───────────────┐
    ▼                               ▼                 ▼                               ▼
┌──────────────────────┐  ┌────────────────────┐  ┌──────────────────────┐  ┌────────────────────┐
│ SCHEMA: `finance`    │  │ SCHEMA: `sales`    │  │ SCHEMA: `analytics`  │  │ SCHEMA: `staging`  │
└──────────┬───────────┘  └─────────┬──────────┘  └──────────┬───────────┘  └─────────┬──────────┘
           │                        │                        │                        │
  ┌────────┴────────┐      ┌────────┴────────┐      ┌────────┴────────┐      ┌────────┴────────┐
  │ • Tables        │      │ • Tables        │      │ • Tables        │      │ • Tables        │
  │ • Dynamic Masks │      │ • Row Filters   │      │ • Views         │      │ • External Tabs │
  │ • Volumes (Raw) │      │ • Volumes (Doc) │      │ • Functions/UDFs│      │ • Managed Vols  │
  └─────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
```

---

### 3. Spark Catalyst Optimization & Adaptive Query Execution (AQE)

```
SQL Query / DataFrame API
         │
         ▼
[ Unresolved Logical Plan ] ──> (Analyzer + Catalog) ──> [ Resolved Logical Plan ]
                                                                  │
                                                                  ▼
                                                      [ Optimized Logical Plan ]
                                                      • Constant Folding
                                                      • Predicate Pushdown
                                                      • Projection Pruning
                                                                  │
                                                                  ▼
                                                       [ Physical Plans (1..N) ]
                                                                  │
                                                                  ▼ (Cost-Based Model)
                                                        [ Selected Physical Plan ]
                                                                  │
                                                                  ▼ (Code Generation)
                                                      [ RDD Bytecode Execution ]
                                                                  │
                                                                  ▼ (Runtime Shuffle Stats)
                                                    [ Adaptive Query Execution (AQE) ]
                                                    ├── Coalesce shuffle partitions dynamically
                                                    ├── Convert SortMergeJoin to BroadcastHashJoin
                                                    └── Split & optimize skewed join partitions
```

---

## 🚀 Databricks & Local Quickstart

### 1. Direct Sync into Databricks Workspaces (1-Click Setup)
Because every notebook in this repository is written in the native Databricks `.py` format, importing into your workspace requires zero conversion:
1. Open your **Databricks Workspace**.
2. Navigate to **Workspace** > **Repos** (or **Git Folders**).
3. Click **Add Repo** (or **Create Git Folder**) and enter:
   ```text
   https://github.com/santoshjammi29/pyspark-handbook.git
   ```
4. Open any notebook under `notebooks/`. It will instantly render interactive Databricks cells, markdown documentation, widgets, and execution controls!
5. Attach any active cluster and click **Run All**!

---

### 2. Local Execution in VS Code or Terminal
Every notebook includes an environment-aware SparkSession bootstrapper (`try: spark except NameError:`). If executed locally, it automatically initializes a self-contained local PySpark session with Delta Lake support:

```bash
# 1. Clone repository
git clone https://github.com/santoshjammi29/pyspark-handbook.git
cd pyspark-handbook

# 2. Set up virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install PySpark, Delta Lake, and PyArrow
pip install -r requirements.txt

# 4. Execute any notebook directly as a standard Python script
python notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.py
```

---

### 3. Convert to Jupyter `.ipynb` (Optional)
If you prefer viewing in Jupyter Lab or Jupyter Notebook:
```bash
python scripts/export_to_ipynb.py
```
This generates standard `.ipynb` files under the `jupyter_exports/` directory.

---

## 🎯 Senior Data Engineer Interview Matrix

The table below maps real-world production challenges and frequently asked Staff / Senior Data Engineer interview topics to the exact notebook implementations in this repository:

| Interview Topic & Challenge | Core Technical Problem | Architectural Solution | Reference Notebook |
|:---|:---|:---|:---:|
| **Severe Data Skew in Distributed Joins** | One partition holds 90% of data, straggler tasks trigger job timeouts. | Key salting, synthetic repartitioning, and two-stage skew aggregation. | [`076_data_skew_mitigation`](notebooks/08_performance_and_tuning/076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins.py) |
| **ACID Concurrency & Write Conflicts** | Concurrent batch updates and streaming writes cause `ConcurrentAppendException`. | Optimistic Concurrency Control (OCC), transaction log mechanics, and partition isolation. | [`034_acid_crud_operations`](notebooks/04_delta_lake_and_acid/034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics.py) |
| **SCD Type 2 Historical Tracking** | Track point-in-time attribute changes without rewriting whole tables. | Two-stage Delta `MERGE INTO` with effective date intervals and `is_current` flags. | [`069_scd_type_2_tracking`](notebooks/07_lakehouse_modeling_scd/069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.py) |
| **Streaming Late-Arriving Data & Watermarks** | Real-time streams receive delayed events; unbounded memory growth. | Event-time watermarking with appropriate drop tolerances and window aggregations. | [`056_streaming_watermarking`](notebooks/06_streaming_realtime/056_streaming_window_aggregations_and_watermarking_for_late_arriving_events.py) |
| **Incremental File Ingestion at Scale** | S3/ADLS bucket with 1,000,000+ files causes file listing timeouts. | Databricks Auto Loader (`cloudFiles`) with RocksDB state and checkpointing. | [`048_auto_loader_cloudfiles`](notebooks/05_data_ingestion_autoloader/048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.py) |
| **Adaptive Query Execution (AQE) Dynamic Tuning** | Static Spark configurations fail to adapt to unpredictable runtime shuffle sizes. | AQE dynamic shuffle partition coalescing, join switching, and skew joins. | [`075_adaptive_query_execution`](notebooks/08_performance_and_tuning/075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins.py) |
| **Fine-Grained Data Governance & Compliance** | Multi-tenant users require dynamic masking for PII without duplicate tables. | Unity Catalog Row Filters and Dynamic Column Masking functions. | [`089_dynamic_column_masking`](notebooks/09_unity_catalog_and_security/089_dynamic_column_masking_for_pii_protection_and_compliance.py) |
| **Delta Lake Small Files & Compaction** | Thousands of micro-batch commits degrade query planning and metadata I/O. | Bin-packing compaction, `OPTIMIZE`, `auto_compaction`, and `VACUUM`. | [`038_delta_optimize_compaction`](notebooks/04_delta_lake_and_acid/038_delta_optimize_file_compaction_and_auto_compaction_strategies.py) |

---

## 📚 Master Curriculum Catalog of 105 Use Cases

<a id="module-01"></a>
<details open>
<summary><h3>📊 Module 01: Core PySpark Foundations & DataFrame API (001 – 012)</h3></summary>

> 💡 **Focus:** Core DataFrame manipulation, explicit StructType schemas, PyArrow Pandas UDFs, safe type casting, column projections, and Databricks dbutils emulation.

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `001` | [`SparkSession Initialization & DataFrame Creation Strategies`](notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.py) | 🟢 `Beginner` | A digital commerce platform needs to ingest customer profile extracts from multiple legacy sources (in-memory lists, Python dictiona... | Always supply explicit StructType or DDL schemas in production to avoid schema inference performance p... |
| `002` | [`Schema Enforcement, Safe Type Casting & Try-Cast Handling`](notebooks/01_core_dataframe_api/002_schema_enforcement_safe_type_casting_and_try_cast_handling.py) | 🟢 `Beginner` | A payments processor receives raw transaction dumps where numeric amounts contain currency symbols, timestamps are in non-standard f... | In Spark 3.2+, `F.expr('try_cast(col AS TYPE)')` safely converts invalid strings to NULL without raisi... |
| `003` | [`Column Manipulation, Batch Addition & Projection Pruning`](notebooks/01_core_dataframe_api/003_column_manipulation_batch_addition_and_projection_pruning.py) | 🟢 `Beginner` | A SaaS telemetry service needs to enrich raw session events with calculated duration, derived flags, normalized client identifiers, ... | Avoid chaining 20+ `.withColumn()` calls in loops; use `withColumns()` (Spark 3.3+) or `.select()` pro... |
| `004` | [`Filtering, Multi-Condition Predicates & Pushdown Analysis`](notebooks/01_core_dataframe_api/004_filtering_multi_condition_predicates_and_pushdown_analysis.py) | 🟢 `Beginner` | A logistics fleet management company needs to identify critical alerts: vehicles with engine temperatures exceeding safe thresholds,... | Always wrap individual filter conditions in parentheses `(cond1) & (cond2)` due to Python bitwise oper... |
| `005` | [`String Operations, Regex Extraction & Data Cleansing`](notebooks/01_core_dataframe_api/005_string_operations_regex_extraction_and_data_cleansing.py) | 🟢 `Beginner` | A marketing automation system ingests unstructured customer lead notes containing messy phone numbers, email addresses with inconsis... | `regexp_extract(col, pattern, group_index)` extracts capturing groups where group 0 is the full match. |
| `006` | [`Date & Timestamp Mastery: Parsing, Arithmetic & Timezones`](notebooks/01_core_dataframe_api/006_date_and_timestamp_mastery_parsing_arithmetic_and_timezones.py) | 🟢 `Beginner` | A global ride-sharing platform records ride dispatch, pickup, and dropoff events across multiple international timezones. Operations... | Always store raw ingestion timestamps in UTC to prevent daylight saving ambiguities. |
| `007` | [`Handling Nulls, NaNs & Imputation Strategies`](notebooks/01_core_dataframe_api/007_handling_nulls_nans_and_imputation_strategies.py) | 🟢 `Beginner` | A medical diagnostics platform aggregates clinical lab test results. Missing biomarker readings occur due to machine sensor dropouts... | `F.isnan()` applies strictly to Float/Double types, while `F.isNull()` applies universally. |
| `008` | [`Sorting, Ordering Semantics & Stratified Sampling`](notebooks/01_core_dataframe_api/008_sorting_ordering_semantics_and_stratified_sampling.py) | 🟢 `Beginner` | A financial auditing team must sample transaction logs for regulatory compliance. The sample must be stratified across risk tiers (L... | `orderBy` performs a global sort via RangePartitioner shuffle, while `sortWithinPartitions` performs l... |
| `009` | [`Deduplication Strategies: dropDuplicates vs Window Row-Number`](notebooks/01_core_dataframe_api/009_deduplication_strategies_dropduplicates_vs_window_row_number.py) | 🟡 `Intermediate` | An e-commerce order ingestion pipeline receives duplicate HTTP webhook payloads. Some duplicates are exact identical records, while ... | `distinct()` hashes all columns across the dataset, triggering a shuffle. |
| `010` | [`Transformations vs Actions, Lazy Evaluation & Lineage Graph`](notebooks/01_core_dataframe_api/010_transformations_vs_actions_lazy_evaluation_and_lineage_graph.py) | 🟡 `Intermediate` | A performance engineering lead must diagnose why a data pipeline fails after running for 40 minutes, only to crash on a lightweight ... | Transformations build the logical plan; actions trigger physical job execution and DAG scheduling. |
| `011` | [`Custom PySpark UDFs vs Vectorized Pandas UDFs (PyArrow)`](notebooks/01_core_dataframe_api/011_custom_pyspark_udfs_vs_vectorized_pandas_udfs_pyarrow.py) | 🟡 `Intermediate` | A credit scoring model applies complex non-linear mathematical risk adjustments to customer accounts. The current implementation use... | Always favor native Spark SQL functions first before writing custom functions. |
| `012` | [`Emulating Databricks DBUtils, Widgets & Secret Scopes Locally`](notebooks/01_core_dataframe_api/012_emulating_databricks_dbutils_widgets_and_secret_scopes_locally.py) | 🟡 `Intermediate` | An enterprise migration team develops Databricks pipelines locally inside VS Code or CI/CD test runners before deploying to producti... | Never hardcode credentials in notebooks—use secret scopes via `dbutils.secrets.get()` in Databricks or... |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>

<a id="module-02"></a>
<details open>
<summary><h3>🧩 Module 02: Complex Data Types & Nested Structures Handling (013 – 022)</h3></summary>

> 💡 **Focus:** Nested StructTypes, Arrays, Maps, Higher-Order Functions (transform, filter, aggregate), JSON parsing, XML shredding, and schema evolution.

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `013` | [`Array Creation, Element Extraction and Array Search Functions`](notebooks/02_complex_data_types/013_array_creation_element_extraction_and_array_search_functions.py) | 🟡 `Intermediate` | A digital streaming platform logs user engagement tags, genre preferences, and content playlists as arrays. Marketing analysts need ... | `element_at(col, 1)` uses 1-based indexing for positive numbers and -1 for the last element. |
| `014` | [`Spark Higher-Order Functions: Transform, Filter and Aggregate`](notebooks/02_complex_data_types/014_spark_higher_order_functions_transform_filter_and_aggregate.py) | 🟡 `Intermediate` | A fintech loyalty platform tracks customer reward points earned across multiple merchant transactions in an array. The team needs to... | Higher-order functions eliminate the need for `explode()` and `groupBy()` when manipulating array elem... |
| `015` | [`Exploding and Flattening Arrays: explode vs explode_outer and posexplode`](notebooks/02_complex_data_types/015_exploding_and_flattening_arrays_explode_vs_explode_outer_and_posexplode.py) | 🟡 `Intermediate` | A flight booking aggregator normalizes JSON payloads of airline bookings. Bookings contain lists of passengers and stops. Some booki... | Always use `explode_outer()` if the array column can contain nulls or empty lists to avoid losing pare... |
| `016` | [`Struct Creation, Nested Field Access and Updating Sub-Fields`](notebooks/02_complex_data_types/016_struct_creation_nested_field_access_and_updating_sub_fields.py) | 🟡 `Intermediate` | A customer identity platform groups personal attributes, contact points, and address details into nested structs for clean schema or... | Use `struct(*cols)` to group related columns into a compound object without network shuffle. |
| `017` | [`Recursive Schema Traversal: Flattening Deeply Nested JSON/Structs`](notebooks/02_complex_data_types/017_recursive_schema_traversal_flattening_deeply_nested_json_structs.py) | 🔴 `Advanced` | An enterprise integration hub ingests complex payloads from ERP and CRM systems containing 5+ levels of nested structs and arrays. A... | Programmatic schema inspection via `df.schema.fields` enables dynamic metadata-driven pipelines. |
| `018` | [`MapType Creation, Key Extraction and Dynamic Properties`](notebooks/02_complex_data_types/018_maptype_creation_key_extraction_and_dynamic_properties.py) | 🟡 `Intermediate` | An advertising technology platform captures custom user ad-tracking attributes (cookies, campaign IDs, UTM tags, experiment variants... | Use `MapType` for semi-structured data where keys are dynamic, variable, or unpredictable. |
| `019` | [`Parsing Raw JSON Payloads: from_json, to_json and schema_of_json`](notebooks/02_complex_data_types/019_parsing_raw_json_payloads_from_json_to_json_and_schema_of_json.py) | 🟡 `Intermediate` | A payment gateway streams raw JSON transaction events via Apache Kafka or cloud storage. Each record arrives as a single raw string ... | Always supply explicit schemas to `from_json()` to avoid driver-side inference bottlenecks. |
| `020` | [`Handling Corrupted & Malformed JSON: Permissive vs DropMalformed vs FailFast`](notebooks/02_complex_data_types/020_handling_corrupted_and_malformed_json_permissive_vs_dropmalformed_vs_failfast.py) | 🟡 `Intermediate` | A public data ingestion pipeline scrapes third-party web endpoints. Up to 5% of incoming payloads contain syntax errors (truncated b... | `PERMISSIVE` mode ensures zero pipeline downtime by capturing unparseable strings into `_corrupt_record`. |
| `021` | [`XML Data Ingestion, Hierarchical Parsing and Tag Structuring`](notebooks/02_complex_data_types/021_xml_data_ingestion_hierarchical_parsing_and_tag_structuring.py) | 🟡 `Intermediate` | A legacy healthcare exchange transmits insurance claims in XML format. Claims contain nested patient information, diagnosis codes, a... | Spark SQL provides built-in XPath expressions (`xpath_string`, `xpath_double`, `xpath_int`) for lightw... |
| `022` | [`Advanced Aggregations: collect_list vs collect_set and Sorted Collections`](notebooks/02_complex_data_types/022_advanced_aggregations_collect_list_vs_collect_set_and_sorted_collections.py) | 🟡 `Intermediate` | A digital customer journey pipeline aggregates all URLs visited by users within a session. Product teams need both: (1) The exact ch... | `collect_list()` retains duplicates and element count; `collect_set()` removes duplicates without guar... |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>

<a id="module-03"></a>
<details open>
<summary><h3>🪟 Module 03: Advanced SQL, Aggregations & Analytical Window Functions (023 – 032)</h3></summary>

> 💡 **Focus:** Ranking, lead/lag, rolling moving averages, cumulative distribution, OLAP cubes, pivots, broadcast hash joins, and range vs rows framing.

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `023` | [`Basic and Multi-Column Aggregations with Alias Management`](notebooks/03_sql_and_window_functions/023_basic_and_multi_column_aggregations_with_alias_management.py) | 🟢 `Beginner` | A retail analytics engine calculates daily regional performance metrics across store departments: total gross revenue, average trans... | Always alias aggregated expressions immediately using `.alias()` to prevent messy default column names. |
| `024` | [`Advanced Multidimensional OLAP: Rollup, Cube and Grouping Sets`](notebooks/03_sql_and_window_functions/024_advanced_multidimensional_olap_rollup_cube_and_grouping_sets.py) | 🟡 `Intermediate` | A financial controller needs a single unified sales reporting view that includes: (1) Detailed breakdown by Year, Quarter, and Produ... | `rollup()` computes hierarchical subtotals along a defined drill-down path (Year -> Quarter -> Product). |
| `025` | [`Pivoting and Unpivoting Data: pivot and stack Expression Mastery`](notebooks/03_sql_and_window_functions/025_pivoting_and_unpivoting_data_pivot_and_stack_expression_mastery.py) | 🟡 `Intermediate` | A business intelligence dashboard requires monthly revenue pivoted into wide columns (Jan, Feb, Mar...) for tabular display. Convers... | Always provide an explicit list of pivot values to `pivot(col, values)` to eliminate an expensive full... |
| `026` | [`Ranking Window Functions: row_number, rank, dense_rank and percent_rank`](notebooks/03_sql_and_window_functions/026_ranking_window_functions_row_number_rank_dense_rank_and_percent_rank.py) | 🟡 `Intermediate` | A national sales management board ranks sales representatives within each geographic territory based on quarterly quota attainment. ... | `row_number()` assigns strictly unique sequential integers regardless of ties. |
| `027` | [`Aggregate & Cumulative Window Functions: Running Totals and Moving Averages`](notebooks/03_sql_and_window_functions/027_aggregate_and_cumulative_window_functions_running_totals_and_moving_averages.py) | 🟡 `Intermediate` | A fintech wealth management app displays account balance trajectories and 7-day smoothed moving averages of daily expenditures to he... | `ROWS BETWEEN` defines physical row count boundaries (e.g. 2 preceding rows). |
| `028` | [`Offset Window Functions: lead, lag and Period-over-Period Churn`](notebooks/03_sql_and_window_functions/028_offset_window_functions_lead_lag_and_period_over_period_churn.py) | 🟡 `Intermediate` | A SaaS subscription billing service analyzes customer MRR (Monthly Recurring Revenue). Finance requires month-over-month revenue var... | `lag(col, offset, default)` accesses preceding rows without requiring self-joins. |
| `029` | [`Sessionization and the Gaps & Islands Problem in PySpark`](notebooks/03_sql_and_window_functions/029_sessionization_and_the_gaps_and_islands_problem_in_pyspark.py) | 🔴 `Advanced` | A digital analytics platform tracks web page views. An active browsing session is defined as consecutive clicks separated by no more... | The Gaps and Islands problem is solved using a two-window approach: flag boundaries with `lag`, then g... |
| `030` | [`Relational Joins Masterclass: Inner, Outer, Semi and Anti Joins`](notebooks/03_sql_and_window_functions/030_relational_joins_masterclass_inner_outer_semi_and_anti_joins.py) | 🟡 `Intermediate` | An enterprise CRM data reconciler syncs accounts between Salesforce and a billing database. The reconciliation team needs to find: (... | Use `on='col_name'` instead of `df1['col'] == df2['col']` to avoid producing duplicate join column names. |
| `031` | [`Broadcast Hash Joins, Threshold Tuning and Memory Limits`](notebooks/03_sql_and_window_functions/031_broadcast_hash_joins_threshold_tuning_and_memory_limits.py) | 🟡 `Intermediate` | A point-of-sale stream joins billions of transaction rows with a small 2MB store metadata lookup table. Default join configuration c... | Broadcast joins completely eliminate shuffle exchange, providing order-of-magnitude speedups for asymm... |
| `032` | [`Spark SQL Views, Temp Tables and Catalyst SQL Optimization`](notebooks/03_sql_and_window_functions/032_spark_sql_views_temp_tables_and_catalyst_sql_optimization.py) | 🟢 `Beginner` | A hybrid analytics team consists of Python data engineers and SQL business analysts. The data engineering team prepares sanitized Da... | Temporary views are lightweight metadata aliases that require zero disk storage or computation until q... |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>

<a id="module-04"></a>
<details open>
<summary><h3>🧱 Module 04: Delta Lake Architecture, ACID Operations & Time Travel (033 – 043)</h3></summary>

> 💡 **Focus:** Delta transaction log (_delta_log JSON & checkpoints), ACID CRUD, high-throughput MERGE INTO, time travel, VACUUM retention, and Liquid Clustering.

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `033` | [`Delta Table Creation, Metadata Inspection and Transaction Log Internals`](notebooks/04_delta_lake_and_acid/033_delta_table_creation_metadata_inspection_and_transaction_log_internals.py) | 🟡 `Intermediate` | A lakehouse platform architect requires an immutable, audit-compliant storage layer for financial ledger records. Standard Parquet f... | Delta Lake stores data as standard Parquet files paired with an ACID transaction log in `_delta_log/`. |
| `034` | [`ACID CRUD Operations on Delta: Insert, Update and Delete Mechanics`](notebooks/04_delta_lake_and_acid/034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics.py) | 🟡 `Intermediate` | A GDPR/CCPA privacy compliance pipeline requires modifying existing customer records: deleting individuals exercising 'Right to be F... | Delta Lake brings row-level UPDATE and DELETE capabilities to Apache Spark without rewriting whole par... |
| `035` | [`Delta Upsert and MERGE INTO Mastery with Multiple Conditions`](notebooks/04_delta_lake_and_acid/035_delta_upsert_and_merge_into_mastery_with_multiple_conditions.py) | 🟡 `Intermediate` | A retail inventory management system synchronizes daily inventory updates from 500 physical warehouses. If an item exists in the cen... | `MERGE INTO` executes inserts, updates, and deletes in a single unified atomic transaction. |
| `036` | [`Delta Time Travel, Audit History and Version Rollback`](notebooks/04_delta_lake_and_acid/036_delta_time_travel_audit_history_and_version_rollback.py) | 🟡 `Intermediate` | A bug in an automated ETL pricing script mistakenly discounted all luxury goods by 90% during an overnight run. The operations team ... | Time Travel allows querying historical states via `.option('versionAsOf', v)` or `.option('timestampAs... |
| `037` | [`VACUUM Command, Retention Hours and Safe Tombstone Management`](notebooks/04_delta_lake_and_acid/037_vacuum_command_retention_hours_and_safe_tombstone_management.py) | 🟡 `Intermediate` | A cloud lakehouse table ingests high-frequency updates, accumulating 50,000 unreferenced obsolete Parquet files and generating massi... | Delta `VACUUM` cleans up orphaned data files no longer referenced by the current table snapshot. |
| `038` | [`Delta OPTIMIZE, File Compaction and Auto-Compaction Strategies`](notebooks/04_delta_lake_and_acid/038_delta_optimize_file_compaction_and_auto_compaction_strategies.py) | 🟡 `Intermediate` | A streaming ingestion job writes 1,000 tiny 50KB Parquet files into a Delta table every hour. Query latency has plummeted from 2 sec... | `OPTIMIZE` bin-packs small files into target ~1GB files without altering data semantics. |
| `039` | [`Z-Order Multi-Dimensional Indexing vs Liquid Clustering (CLUSTER BY)`](notebooks/04_delta_lake_and_acid/039_z_order_multi_dimensional_indexing_vs_liquid_clustering_cluster_by.py) | 🔴 `Advanced` | An ad-tech analytics platform runs ad-hoc queries filtering on combinations of `customer_id`, `event_date`, and `campaign_id`. Tradi... | Z-Ordering arranges multi-dimensional data along a Morton space-filling curve for optimal file skipping. |
| `040` | [`Delta Change Data Feed (CDF): Enabling and Consuming Row-Level Changes`](notebooks/04_delta_lake_and_acid/040_delta_change_data_feed_cdf_enabling_and_consuming_row_level_changes.py) | 🔴 `Advanced` | An enterprise financial lakehouse architecture maintains a real-time risk dashboard that must react to downstream row-level modifica... | Delta Change Data Feed captures row-level mutations: `insert`, `update_preimage`, `update_postimage`, ... |
| `041` | [`Delta Schema Evolution (mergeSchema) and Schema Enforcement`](notebooks/04_delta_lake_and_acid/041_delta_schema_evolution_mergeschema_and_schema_enforcement.py) | 🟡 `Intermediate` | A product catalog service frequently releases new product attributes (e.g. `warranty_months`, `energy_rating`). When downstream batc... | Delta Lake enforces strict schema on write by default, preventing accidental schema corruption. |
| `042` | [`Delta Table Constraints: CHECK and NOT NULL Data Quality Rules`](notebooks/04_delta_lake_and_acid/042_delta_table_constraints_check_and_not_null_data_quality_rules.py) | 🟡 `Intermediate` | A banking compliance application requires strict data quality invariants on account records: account balances must never drop below ... | Delta CHECK constraints enforce data quality at write time directly inside the storage engine. |
| `043` | [`In-Place Parquet to Delta Lake Migration with CONVERT TO DELTA`](notebooks/04_delta_lake_and_acid/043_in_place_parquet_to_delta_lake_migration_with_convert_to_delta.py) | 🟡 `Intermediate` | An enterprise data platform has 100TB of historical data stored across partitioned Parquet directories in Amazon S3 or Azure ADLS. R... | `CONVERT TO DELTA` is a zero-copy metadata-only operation that constructs `_delta_log` instantly. |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>

<a id="module-05"></a>
<details open>
<summary><h3>📥 Module 05: Data Ingestion Patterns, Auto Loader & File Formats (044 – 053)</h3></summary>

> 💡 **Focus:** Databricks Auto Loader (cloudFiles), notification modes, rescued data column, parallel JDBC partitioning, and multi-format lakehouse ingestion.

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `044` | [`CSV Ingestion Edge Cases: Multiline, Escaped Quotes and Custom Delimiters`](notebooks/05_data_ingestion_autoloader/044_csv_ingestion_edge_cases_multiline_escaped_quotes_and_custom_delimiters.py) | 🟡 `Intermediate` | A financial auditing pipeline imports legacy bank statement extracts. The files contain multiline transaction descriptions with embe... | `multiLine=true` is required when fields contain embedded newlines; note that it disables file splitting. |
| `045` | [`Parquet and ORC Columnar Ingestion: Predicate Pushdown and Dictionary Encoding`](notebooks/05_data_ingestion_autoloader/045_parquet_and_orc_columnar_ingestion_predicate_pushdown_and_dictionary_encoding.py) | 🟡 `Intermediate` | A petabyte-scale telemetry lakehouse queries 500 million vehicle diagnostic events daily. Analyst queries typically filter on single... | Column Projection reads only required columns from disk, dramatically reducing I/O and network transfer. |
| `046` | [`Avro Binary Ingestion and Schema Registry Compatibility`](notebooks/05_data_ingestion_autoloader/046_avro_binary_ingestion_and_schema_registry_compatibility.py) | 🟡 `Intermediate` | An enterprise Apache Kafka event bus publishes high-throughput customer account updates serialized in Apache Avro format. Data engin... | Avro is the industry standard format for streaming message buses (Kafka, Event Hubs) due to compact bi... |
| `047` | [`JDBC Parallel Ingestion: Partitioning by Column, fetchsize and Connection Sizing`](notebooks/05_data_ingestion_autoloader/047_jdbc_parallel_ingestion_partitioning_by_column_fetchsize_and_connection_sizing.py) | 🟡 `Intermediate` | A night batch pipeline extracts 50 million sales transaction records from an operational PostgreSQL production database into the Lak... | Always supply `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` when reading large JD... |
| `048` | [`Databricks Auto Loader Directory Mode: cloudFiles, Schema Inference and Checkpointing`](notebooks/05_data_ingestion_autoloader/048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.py) | 🟡 `Intermediate` | A cloud storage bucket receives thousands of landing files (JSON and CSV) every hour from IoT devices. Traditional batch file listin... | Databricks Auto Loader (`format('cloudFiles')`) incrementally ingests millions of files with low laten... |
| `049` | [`Auto Loader Schema Evolution and Rescued Data Column (_rescued_data)`](notebooks/05_data_ingestion_autoloader/049_auto_loader_schema_evolution_and_rescued_data_column_rescued_data.py) | 🟡 `Intermediate` | Third-party vendors send telemetry files where new columns appear without prior warning, or column types change unexpectedly. If a p... | `cloudFiles.schemaEvolutionMode = 'addNewColumns'` automatically alters target Delta tables to include... |
| `050` | [`Building a Metadata-Driven Multi-Format Ingestion Engine`](notebooks/05_data_ingestion_autoloader/050_building_a_metadata_driven_multi_format_ingestion_engine.py) | 🔴 `Advanced` | A centralized lakehouse ingestion platform must onboard 200+ distinct source datasets daily spanning CSV, Parquet, JSON, and ORC. Wr... | Metadata-driven engines decouple data engineering transformation rules from execution infrastructure. |
| `051` | [`Incremental Batch File Discovery: Watermarking and Modification Timestamps`](notebooks/05_data_ingestion_autoloader/051_incremental_batch_file_discovery_watermarking_and_modification_timestamps.py) | 🟡 `Intermediate` | A legacy data pipeline runs hourly batch jobs that read from an S3/ADLS bucket where external partners drop new files. The pipeline ... | `modifiedAfter` filters cloud storage files at the source based on object modification timestamps. |
| `052` | [`Handling Compressed Archives (.zip, .tar.gz, .bz2) in Distributed PySpark`](notebooks/05_data_ingestion_autoloader/052_handling_compressed_archives_zip_tar_gz_bz2_in_distributed_pyspark.py) | 🔴 `Advanced` | A regulatory agency deposits quarterly compliance archives as massive 5GB ZIP and TAR.GZ files. Standard Spark readers cannot split ... | Standard ZIP and raw GZIP files cannot be split across multiple tasks—one worker core must decompress ... |
| `053` | [`Distributed REST API and Webhook Ingestion without Driver OOM`](notebooks/05_data_ingestion_autoloader/053_distributed_rest_api_and_webhook_ingestion_without_driver_oom.py) | 🔴 `Advanced` | A financial data vendor exposes historical equity prices via an authenticated REST API with rate limits. The engineering team must f... | `mapPartitions` allows initializing heavyweight objects (database connections, HTTP sessions) once per... |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>

<a id="module-06"></a>
<details open>
<summary><h3>⚡ Module 06: Streaming & Near Real-Time Processing (054 – 063)</h3></summary>

> 💡 **Focus:** Structured Streaming, AvailableNow triggers, event-time watermarking, stateful stream-stream joins, deduplication, and Dead Letter Queues (DLQ).

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `054` | [`Structured Streaming Foundations: Sources, Execution Model and Query Progress`](notebooks/06_streaming_realtime/054_structured_streaming_foundations_sources_execution_model_and_query_progress.py) | 🟡 `Intermediate` | A payments platform monitors live transaction throughput. As card swipes occur across the globe, an unbounded stream must be parsed,... | Structured Streaming abstracts streaming into an unbounded table queried using standard DataFrame API ... |
| `055` | [`Streaming Triggers: ProcessingTime, Once and AvailableNow for Cost Optimization`](notebooks/06_streaming_realtime/055_streaming_triggers_processingtime_once_and_availablenow_for_cost_optimization.py) | 🟡 `Intermediate` | A cloud financial platform runs streaming pipelines. Running clusters 24/7 costs $50,000/month. Because source data arrives in hourl... | `Trigger.AvailableNow` processes all outstanding backlog across multiple micro-batches and terminates ... |
| `056` | [`Streaming Window Aggregations and Watermarking for Late-Arriving Events`](notebooks/06_streaming_realtime/056_streaming_window_aggregations_and_watermarking_for_late_arriving_events.py) | 🔴 `Advanced` | A mobile gaming platform aggregates player score events over 10-minute tumbling windows. Due to intermittent mobile connectivity and... | `withWatermark(time_col, delay)` informs Spark how long to retain intermediate aggregation state for l... |
| `057` | [`Stream-Static Joins with Slowly Changing Dimension Lookups`](notebooks/06_streaming_realtime/057_stream_static_joins_with_slowly_changing_dimension_lookups.py) | 🟡 `Intermediate` | A financial fraud detection system processes streaming credit card swipe authorizations. Each incoming transaction contains only a `... | Stream-Static joins do NOT require state tracking or watermarks because the static table is fully boun... |
| `058` | [`Stream-Stream Dual Watermark Joins with Interval Conditions`](notebooks/06_streaming_realtime/058_stream_stream_dual_watermark_joins_with_interval_conditions.py) | 🔴 `Advanced` | An online retail logistics platform matches checkout order events with payment confirmation events arriving from Stripe/PayPal acros... | Stream-Stream joins require both watermarks AND time interval boundaries to limit state store retention. |
| `059` | [`Streaming Deduplication Across Time Windows with Watermarks`](notebooks/06_streaming_realtime/059_streaming_deduplication_across_time_windows_with_watermarks.py) | 🟡 `Intermediate` | An IoT gateway ingests sensor readings from connected vehicles over cellular networks. When vehicles enter tunnels or experience net... | Always combine `dropDuplicates([keys, timestamp_col])` with `withWatermark()` in streaming jobs. |
| `060` | [`Idempotent ACID Delta Upserts using writeStream.foreachBatch`](notebooks/06_streaming_realtime/060_idempotent_acid_delta_upserts_using_writestream_foreachbatch.py) | 🔴 `Advanced` | A payments streaming pipeline consumes live financial transactions that include both new purchases and refund updates to existing tr... | `foreachBatch()` bridges streaming DataFrames with batch APIs like Delta Lake `MERGE INTO`. |
| `061` | [`Arbitrary Stateful Streaming Operations with FlatMapGroupsWithState Concepts`](notebooks/06_streaming_realtime/061_arbitrary_stateful_streaming_operations_with_flatmapgroupswithstate_concepts.py) | 🔴 `Advanced` | A telecommunications security monitor detects SIM-swap and fraud patterns across customer cell towers. A fraud flag must trigger onl... | `mapGroupsWithState` enables custom complex state machines beyond simple tumbling/sliding aggregations. |
| `062` | [`Dead-Letter Queue (DLQ) Quarantine Streaming Pattern in PySpark`](notebooks/06_streaming_realtime/062_dead_letter_queue_dlq_quarantine_streaming_pattern_in_pyspark.py) | 🟡 `Intermediate` | A real-time clickstream ingestion pipeline processes 10,000 events/second. A software release on the web client introduces malformed... | The Dead-Letter Queue (DLQ) pattern isolates malformed data without stopping production streaming pipe... |
| `063` | [`Streaming Observability: StreamingQueryListener and SLA Latency Alerting`](notebooks/06_streaming_realtime/063_streaming_observability_streamingquerylistener_and_sla_latency_alerting.py) | 🔴 `Advanced` | An enterprise fintech streaming pipeline must maintain an SLA of processing events within 60 seconds of origination. If a backlog bu... | `StreamingQueryListener` provides non-invasive, asynchronous observability into all streaming queries. |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>

<a id="module-07"></a>
<details open>
<summary><h3>🏛️ Module 07: Lakehouse Data Modeling, CDC & SCD Implementation (064 – 073)</h3></summary>

> 💡 **Focus:** Medallion Architecture (Bronze/Silver/Gold), Slowly Changing Dimensions (SCD Type 1, 2, 3, 4), Debezium CDC replay, and Data Vault 2.0.

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `064` | [`Medallion Architecture: Bronze Ingestion Layer Standards and Audit Trails`](notebooks/07_lakehouse_modeling_scd/064_medallion_architecture_bronze_ingestion_layer_standards_and_audit_trails.py) | 🟡 `Intermediate` | An enterprise data lakehouse ingests raw transactional data from ERP, CRM, and partner APIs. Data must land in an append-only Bronze... | Bronze layer is an immutable, append-only copy of raw incoming source data. |
| `065` | [`Medallion Architecture: Silver Transformation, Cleansing and Conformance`](notebooks/07_lakehouse_modeling_scd/065_medallion_architecture_silver_transformation_cleansing_and_conformance.py) | 🟡 `Intermediate` | Raw invoices from the Bronze layer contain duplicate entries, unstandardized status codes ('appr', 'APPROVED', 'approved'), missing ... | Silver layer provides conformed, cleansed, deduplicated data for cross-functional consumption. |
| `066` | [`Medallion Architecture: Gold Business Aggregations and Dimensional Serving`](notebooks/07_lakehouse_modeling_scd/066_medallion_architecture_gold_business_aggregations_and_dimensional_serving.py) | 🟡 `Intermediate` | Executive leadership and Power BI dashboards require pre-aggregated quarterly vendor performance scorecards: total invoiced volume, ... | Gold tables represent domain-specific business aggregates and dimensional models designed for end-user... |
| `067` | [`Distributed Surrogate Key Generation: monotonically_increasing_id vs xxhash64 vs UUIDs`](notebooks/07_lakehouse_modeling_scd/067_distributed_surrogate_key_generation_monotonically_increasing_id_vs_xxhash64_vs_uuids.py) | 🟡 `Intermediate` | A lakehouse dimensional model builds customer and account dimension tables. Business analysts require synthetic integer or hash surr... | `xxhash64(concat_ws('&#124;&#124;', *keys))` is the recommended surrogate key strategy for distributed Lakehouse... |
| `068` | [`Slowly Changing Dimension Type 1 (SCD Type 1) In-Place Overwrites via Delta Merge`](notebooks/07_lakehouse_modeling_scd/068_slowly_changing_dimension_type_1_scd_type_1_in_place_overwrites_via_delta_merge.py) | 🟡 `Intermediate` | A master customer dimension stores current contact email, phone number, and address. Under business policy, when a customer moves or... | SCD Type 1 overwrites old attributes in-place, keeping only the latest current state. |
| `069` | [`Slowly Changing Dimension Type 2 (SCD Type 2) Historical Tracking with Effective Dates`](notebooks/07_lakehouse_modeling_scd/069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.py) | 🔴 `Advanced` | An insurance underwriting platform tracks policyholder residential address history. Premium risk calculations depend strictly on the... | SCD Type 2 preserves complete historical lineage using effective start date, end date, and `is_current... |
| `070` | [`Slowly Changing Dimension Type 3 and Type 4 Multi-Tier Implementation`](notebooks/07_lakehouse_modeling_scd/070_slowly_changing_dimension_type_3_and_type_4_multi_tier_implementation.py) | 🟡 `Intermediate` | A sales commission system tracks territory reassignments. Sales reps occasionally switch regions. Compensation analysts only need to... | SCD Type 3 tracks immediate prior state by adding a `previous_value` column to the same row. |
| `071` | [`Change Data Capture (CDC) Ingestion from Debezium/Kafka into Lakehouse`](notebooks/07_lakehouse_modeling_scd/071_change_data_capture_cdc_ingestion_from_debezium_kafka_into_lakehouse.py) | 🔴 `Advanced` | A transactional MySQL inventory database streams row-level changes into Kafka via Debezium CDC. Debezium publishes events containing... | Debezium CDC envelopes provide `before`, `after`, `op`, and commit sequence timestamps. |
| `072` | [`Star Schema Implementation: Fact and Conformed Dimension Modeling in Spark`](notebooks/07_lakehouse_modeling_scd/072_star_schema_implementation_fact_and_conformed_dimension_modeling_in_spark.py) | 🟡 `Intermediate` | A retail enterprise transitions from a monolithic relational data warehouse to a Databricks Lakehouse. BI analysts need classic Star... | Star Schemas feature a central numeric Fact table surrounded by denormalized Dimension tables. |
| `073` | [`Data Vault 2.0 in the Lakehouse: Hubs, Links and Satellites Modeling`](notebooks/07_lakehouse_modeling_scd/073_data_vault_2_0_in_the_lakehouse_hubs_links_and_satellites_modeling.py) | 🔴 `Advanced` | A large banking institution integrates 30 disparate core banking, loan, and credit systems. Source systems change schemas frequently... | Data Vault 2.0 decomposes data into Hubs (business keys), Links (associations), and Satellites (context). |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>

<a id="module-08"></a>
<details open>
<summary><h3>🏎️ Module 08: Performance Tuning, Query Optimization & Skew Mitigation (074 – 085)</h3></summary>

> 💡 **Focus:** Catalyst Logical & Physical Plans, Adaptive Query Execution (AQE), Salting join skew, Hive-style bucketing, OOM diagnosis, and Photon.

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `074` | [`Catalyst Optimizer Deep Dive: Parsed, Analyzed, Optimized Plans and Code Generation`](notebooks/08_performance_and_tuning/074_catalyst_optimizer_deep_dive_parsed_analyzed_optimized_plans_and_code_generation.py) | 🔴 `Advanced` | A lead data architect conducts code reviews for data pipelines. Junior engineers write complex nested SQL and DataFrame code without... | Catalyst translates declarative logic into optimized Java bytecode through 4 formal compilation phases. |
| `075` | [`Adaptive Query Execution (AQE): Dynamic Coalescing, Join Switching and Skew Joins`](notebooks/08_performance_and_tuning/075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins.py) | 🔴 `Advanced` | A 2TB batch ETL pipeline runs with `spark.sql.shuffle.partitions = 200`. During filtering stages, data volume drops from 2TB to 50MB... | AQE uses runtime statistics collected at shuffle stage boundaries to re-optimize execution plans on th... |
| `076` | [`Data Skew Detection and Mitigation Using Salting and Two-Stage Joins`](notebooks/08_performance_and_tuning/076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins.py) | 🔴 `Advanced` | A national e-commerce platform joins website clicks with user accounts. Out of 100 million clicks, 40% belong to guest user ID `GUES... | Data skew causes the notorious '99% task stuck' problem where one executor handles massive key volume. |
| `077` | [`Partitioning Strategies: Avoiding Over-Partitioning and Directory Pruning`](notebooks/08_performance_and_tuning/077_partitioning_strategies_avoiding_over_partitioning_and_directory_pruning.py) | 🟡 `Intermediate` | A telemetry pipeline partitioned data by `year/month/day/hour/device_id`. After 6 months, the table contained 2 million partition di... | Partition pruning skips scanning entire filesystem directories when partition columns are filtered in ... |
| `078` | [`Bucketing for Pre-Sorted Join Optimization and Shuffle Elimination`](notebooks/08_performance_and_tuning/078_bucketing_for_pre_sorted_join_optimization_and_shuffle_elimination.py) | 🔴 `Advanced` | Two multi-billion-row fact tables (Orders and Shipments) are joined hourly in a critical financial pipeline. Because both tables are... | Bucketing pre-sorts and pre-distributes data on write, eliminating network shuffle during downstream j... |
| `079` | [`Caching and Persistence: StorageLevel (MEMORY_AND_DISK) vs Cache Traps`](notebooks/08_performance_and_tuning/079_caching_and_persistence_storagelevel_memory_and_disk_vs_cache_traps.py) | 🟡 `Intermediate` | A machine learning feature engineering notebook calls `df.cache()` on a 50GB dataset. Later in the pipeline, iterative actions cause... | Only cache DataFrames that are accessed multiple times across distinct actions or iterative ML algorit... |
| `080` | [`Repartition vs Coalesce Internals: Full Shuffle vs Partition Minimization`](notebooks/08_performance_and_tuning/080_repartition_vs_coalesce_internals_full_shuffle_vs_partition_minimization.py) | 🟡 `Intermediate` | A data pipeline writes daily reports to cloud storage. An engineer notices that writing a 5MB aggregated table produces 200 tiny fil... | Use `coalesce(n)` to decrease partition count before writing to disk—it eliminates network shuffles. |
| `081` | [`Shuffle Partition Sizing and Tuning for Small vs Terabyte-Scale Jobs`](notebooks/08_performance_and_tuning/081_shuffle_partition_sizing_and_tuning_for_small_vs_terabyte_scale_jobs.py) | 🟡 `Intermediate` | A 100MB dataset query takes 45 seconds because Spark defaults to 200 shuffle partitions (`spark.sql.shuffle.partitions = 200`), laun... | Target between 100MB and 200MB of compressed data per shuffle partition task. |
| `082` | [`Diagnosing and Fixing OutOfMemory (OOM) Errors: Driver vs Executor Heap`](notebooks/08_performance_and_tuning/082_diagnosing_and_fixing_outofmemory_oom_errors_driver_vs_executor_heap.py) | 🔴 `Advanced` | A production ETL job intermittently crashes with `java.lang.OutOfMemoryError: Java heap space`. On-call engineers arbitrarily double... | Driver OOM is caused by `.collect()`, `.toPandas()`, oversized broadcast tables, or deep Catalyst plans. |
| `083` | [`Spark UI, EventLog Metrics, Task Skew and Shuffle Spill Analysis`](notebooks/08_performance_and_tuning/083_spark_ui_eventlog_metrics_task_skew_and_shuffle_spill_analysis.py) | 🔴 `Advanced` | A mission-critical nightly reconciliation job slows down by 3x on weekends. The senior engineer must inspect Spark execution metrics... | Shuffle Spill (Memory/Disk) in the Spark UI is the #1 indicator of undersized shuffle partitions. |
| `084` | [`Vectorized Execution and Photon Engine Acceleration Mechanics`](notebooks/08_performance_and_tuning/084_vectorized_execution_and_photon_engine_acceleration_mechanics.py) | 🔴 `Advanced` | An enterprise evaluates migrating high-throughput SQL analytics from classic Apache Spark clusters to Databricks clusters with Photo... | Photon is Databricks' vectorized query engine written in C++ for maximum CPU cache locality and SIMD e... |
| `085` | [`Fixing Accidental Cartesian Products and BroadcastNestedLoopJoins`](notebooks/08_performance_and_tuning/085_fixing_accidental_cartesian_products_and_broadcastnestedloopjoins.py) | 🔴 `Advanced` | A junior engineer submits a join query where the join condition accidentally uses an OR operator across two conditions: `ON (a.id = ... | Joining on non-equi conditions or OR predicates forces Spark into `CartesianProduct` or `BroadcastNest... |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>

<a id="module-09"></a>
<details open>
<summary><h3>🛡️ Module 09: Databricks Platform, Unity Catalog, Security & FinOps (086 – 095)</h3></summary>

> 💡 **Focus:** Unity Catalog 3-tier namespace, Managed & External Volumes, Row-Level Security, Dynamic Column Masking, Secret Scopes, and Databricks Asset Bundles.

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `086` | [`Unity Catalog Three-Tier Namespace: Catalogs, Schemas and Managed vs External Tables`](notebooks/09_unity_catalog_and_security/086_unity_catalog_three_tier_namespace_catalogs_schemas_and_managed_vs_external_tables.py) | 🟡 `Intermediate` | An enterprise migrates from the legacy Hive Metastore (`hive_metastore.default`) to Databricks Unity Catalog. Platform administrator... | Unity Catalog replaces the legacy 2-tier Hive Metastore with a structured 3-tier namespace (`catalog.s... |
| `087` | [`Unity Catalog Volumes for Unstructured and Semi-Structured Data Governance`](notebooks/09_unity_catalog_and_security/087_unity_catalog_volumes_for_unstructured_and_semi_structured_data_governance.py) | 🟡 `Intermediate` | A machine learning team manages non-tabular files (PDF documents, medical images, audio files, and model weights) in cloud storage. ... | Unity Catalog Volumes extend data governance to non-tabular files (PDFs, images, CSVs, models). |
| `088` | [`Dynamic Row-Level Security (RLS) Using Row Filters and User Functions`](notebooks/09_unity_catalog_and_security/088_dynamic_row_level_security_rls_using_row_filters_and_user_functions.py) | 🔴 `Advanced` | A multinational sales organization maintains a single centralized `sales_pipeline` table. Regional sales reps must only see deals wi... | Unity Catalog Row Filters enforce data isolation on single conformed tables without creating fragmente... |
| `089` | [`Dynamic Column Masking for PII Protection and Compliance`](notebooks/09_unity_catalog_and_security/089_dynamic_column_masking_for_pii_protection_and_compliance.py) | 🟡 `Intermediate` | A medical insurance database stores member Social Security Numbers (SSN), full names, and email addresses. Customer service represen... | Column Masking anonymizes sensitive attributes dynamically without mutating underlying storage files. |
| `090` | [`PII Encryption, Salted Hashing and AES Decryption (aes_encrypt, sha2)`](notebooks/09_unity_catalog_and_security/090_pii_encryption_salted_hashing_and_aes_decryption_aes_encrypt_sha2.py) | 🔴 `Advanced` | A payment processing platform stores credit card Primary Account Numbers (PANs). PCI-DSS regulatory standards mandate that card numb... | `aes_encrypt` and `aes_decrypt` provide native, high-speed cryptographic protection directly in Spark ... |
| `091` | [`Databricks Secret Scopes and Secure Credential Injection via dbutils.secrets`](notebooks/09_unity_catalog_and_security/091_databricks_secret_scopes_and_secure_credential_injection_via_dbutils_secrets.py) | 🟡 `Intermediate` | A data integration pipeline connects to cloud databases, external APIs (Stripe, Salesforce), and message queues. Developers must nev... | Never hardcode passwords or API keys in notebooks or Git repositories. |
| `092` | [`FinOps and DBU Cost Calculation Using system.billing.usage System Tables`](notebooks/09_unity_catalog_and_security/092_finops_and_dbu_cost_calculation_using_system_billing_usage_system_tables.py) | 🔴 `Advanced` | A Director of Data Engineering needs to track monthly Databricks DBU (Databricks Unit) consumption across business units (Finance, M... | `system.billing.usage` provides granular operational visibility into DBU spend across workspaces. |
| `093` | [`Lakeflow Declarative Pipelines and Delta Live Tables (DLT) Expectations`](notebooks/09_unity_catalog_and_security/093_lakeflow_declarative_pipelines_and_delta_live_tables_dlt_expectations.py) | 🔴 `Advanced` | An enterprise financial lakehouse migrates manual notebook pipelines to Databricks Lakeflow Declarative Pipelines (Delta Live Tables... | Delta Live Tables (DLT) provides a declarative framework for building reliable Medallion lakehouses. |
| `094` | [`Databricks Asset Bundles (DABs) for Multi-Environment CI/CD Automation`](notebooks/09_unity_catalog_and_security/094_databricks_asset_bundles_dabs_for_multi_environment_ci_cd_automation.py) | 🔴 `Advanced` | An enterprise development team deploys 50 Databricks workflows and pipelines across development, staging, and production workspaces.... | Databricks Asset Bundles (DABs) implement Infrastructure-as-Code (IaC) for Databricks projects. |
| `095` | [`Semantic Layer Modeling and Databricks Metric Views for BI and AI Agents`](notebooks/09_unity_catalog_and_security/095_semantic_layer_modeling_and_databricks_metric_views_for_bi_and_ai_agents.py) | 🔴 `Advanced` | Different business teams calculate 'Customer Churn' and 'Net Revenue' using conflicting SQL formulas, producing discrepancies betwee... | A Semantic Layer standardizes business metrics (Net Revenue, Churn, Margin) across the entire company. |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>

<a id="module-10"></a>
<details open>
<summary><h3>🚀 Module 10: End-to-End Enterprise Projects & Production Workflows (096 – 105)</h3></summary>

> 💡 **Focus:** Full capstones: FinTech Fraud Detection, Omnichannel Retail Lakehouse, Healthcare FHIR Ingestion, IoT Telemetry, RAG Embeddings, and Workflows.

| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
| `096` | [`Project Finance: Real-Time Credit Card Fraud Detection and Customer 360`](notebooks/10_end_to_end_projects/096_project_finance_real_time_credit_card_fraud_detection_and_customer_360.py) | 🔴 `Advanced` | A tier-1 retail bank processes 50 million credit card swipe transactions daily. Risk engineering must build an end-to-end Medallion ... | Medallion architecture provides clear separation between raw append (Bronze), enriched logic (Silver),... |
| `097` | [`Project Retail: Omnichannel Multi-Source Lakehouse ETL`](notebooks/10_end_to_end_projects/097_project_retail_omnichannel_multi_source_lakehouse_etl.py) | 🔴 `Advanced` | A major omnichannel retailer operates physical retail stores, an e-commerce website, and customer support via Salesforce. Data resid... | Omnichannel data pipelines reconcile disparate source systems into a unified Lakehouse schema. |
| `098` | [`Project Healthcare: HIPAA-Compliant Clinical Records De-Identification`](notebooks/10_end_to_end_projects/098_project_healthcare_hipaa_compliant_clinical_records_de_identification.py) | 🔴 `Advanced` | A hospital research network prepares electronic health records (EHR) for clinical cancer research. HIPAA Safe Harbor compliance mand... | HIPAA Safe Harbor requires de-identifying 18 categories of protected health information (PHI). |
| `099` | [`Project Telecom: Customer Churn Prediction Feature Store and Window Features`](notebooks/10_end_to_end_projects/099_project_telecom_customer_churn_prediction_feature_store_and_window_features.py) | 🔴 `Advanced` | A major mobile carrier experiences 2% monthly customer churn. Data scientists need a centralized Lakehouse Feature Store containing ... | Feature Stores decouple ML feature engineering from real-time model serving. |
| `100` | [`Project Cybersecurity: Web Server Access Log Analytics and Threat Detection`](notebooks/10_end_to_end_projects/100_project_cybersecurity_web_server_access_log_analytics_and_threat_detection.py) | 🔴 `Advanced` | A cyber defense operations center monitors 100 million web server access logs daily. Security analysts must parse unformatted Nginx/... | High-throughput log analytics leverages regex pattern extraction inside Tungsten binary representations. |
| `101` | [`Project IoT: Industrial Telemetry Anomaly Detection via Z-Score Statistics`](notebooks/10_end_to_end_projects/101_project_iot_industrial_telemetry_anomaly_detection_via_z_score_statistics.py) | 🔴 `Advanced` | A smart manufacturing plant operates 5,000 robotic welding arms. Bearings experience mechanical fatigue before catastrophic failure.... | Statistical Z-score detection identifies anomalous values relative to a dynamic, per-machine baseline. |
| `102` | [`Project Supply Chain: Inventory Replenishment Forecasting and Reorder Triggers`](notebooks/10_end_to_end_projects/102_project_supply_chain_inventory_replenishment_forecasting_and_reorder_triggers.py) | 🔴 `Advanced` | A national grocery chain manages 10,000 SKUs across 20 regional distribution centers. Supply chain disruptions cause frequent stocko... | Reorder Point (ROP) equals Lead Time Demand plus statistical Safety Stock. |
| `103` | [`Project E-Commerce: Collaborative Filtering Recommendation Engine with Spark MLlib`](notebooks/10_end_to_end_projects/103_project_e_commerce_collaborative_filtering_recommendation_engine_with_spark_mllib.py) | 🔴 `Advanced` | An online retail store has 1 million products and 5 million shoppers. Product managers want personalized 'Recommended For You' item ... | Spark MLlib ALS performs distributed matrix factorization for collaborative filtering recommendations. |
| `104` | [`Project Agentic AI & RAG: Lakehouse Document Chunking and Vector Search`](notebooks/10_end_to_end_projects/104_project_agentic_ai_and_rag_lakehouse_document_chunking_and_vector_search.py) | 🔴 `Advanced` | An enterprise customer service platform deploys an Agentic AI chatbot powered by Databricks Genie and LLMs. The chatbot needs to ans... | RAG pipelines bridge unstructured enterprise documents with LLM reasoning via Vector Search. |
| `105` | [`Master Enterprise Production Orchestration, Workflow Parameters and Alerting`](notebooks/10_end_to_end_projects/105_master_enterprise_production_orchestration_workflow_parameters_and_alerting.py) | 🔴 `Advanced` | A mission-critical enterprise financial pipeline executes 15 dependent tasks every night. The workflow must accept dynamic run param... | Production pipelines require parameterized orchestration across environments (dev, test, prod). |

<p align="right"><a href="#-interactive-curriculum-navigation">⬆️ Back to Top</a></p>

</details>


---

## 📂 Repository Structure

```
pyspark-handbook/
├── .github/
│   └── workflows/
│       └── ci.yml                     # Automated GitHub Actions CI workflow
├── notebooks/
│   ├── 01_core_dataframe_api/         # 001 - 012: Core DataFrame API, Schemas, Arrow Pandas UDFs, DBUtils
│   ├── 02_complex_data_types/         # 013 - 022: Arrays, Higher-Order Functions, Nested Structs, JSON, XML
│   ├── 03_sql_and_window_functions/   # 023 - 032: OLAP Cubes, Pivots, Window Functions, Joins, Catalyst SQL
│   ├── 04_delta_lake_and_acid/        # 033 - 043: Transaction Log, ACID CRUD, MERGE, Time Travel, Liquid Clustering
│   ├── 05_data_ingestion_autoloader/  # 044 - 053: Auto Loader, Rescued Data, Parallel JDBC, Parquet, Archives
│   ├── 06_streaming_realtime/         # 054 - 063: Structured Streaming, Triggers, Watermarks, Dual-Stream Joins, DLQ
│   ├── 07_lakehouse_modeling_scd/     # 064 - 073: Medallion Architecture, SCD 1/2/3/4, Debezium CDC, Data Vault 2.0
│   ├── 08_performance_and_tuning/     # 074 - 085: Catalyst Optimizer, AQE, Salting Skew, Bucketing, Photon, OOM Fixes
│   ├── 09_unity_catalog_and_security/ # 086 - 095: 3-Tier Namespace, Volumes, RLS, Column Masking, FinOps, DABs
│   └── 10_end_to_end_projects/        # 096 - 105: Finance Fraud, Omnichannel Retail, Healthcare, IoT, RAG, Alerts
├── scripts/
│   ├── notebook_builder.py            # Core engine for generating clean Databricks .py notebooks
│   ├── generate_all.py                # Master CLI tool to rebuild all 105 notebooks
│   ├── generate_readme.py             # Rebuilds the GitHub master catalog and documentation
│   ├── validate_notebooks.py          # Structural integrity, syntax, and Databricks compliance checker
│   └── export_to_ipynb.py             # Converts .py notebooks to Jupyter .ipynb format
├── tests/
│   └── test_notebook_integrity.py     # Automated integrity test suite (count, format, no video traces)
├── .gitignore
├── LICENSE                            # MIT Open Source License
├── pyproject.toml
├── requirements.txt                   # Production and local PySpark dependencies
└── README.md                          # Master documentation and use case catalog
```

---

## ⚡ Quick Cheat Sheet & TL;DR

### 1. Salting Skewed Keys in PySpark
```python
import pyspark.sql.functions as F

SALT_BUCKETS = 8

# Step 1: Add salt to skewed DataFrame
df_salted = df_large.withColumn("salt", F.concat(F.col("skewed_key"), F.lit("_"), F.floor(F.rand() * SALT_BUCKETS)))

# Step 2: Replicate the lookup dimension table to match salt keys
df_lookup_replicated = df_small.withColumn("salt_array", F.array([F.lit(i) for i in range(SALT_BUCKETS)])) \
                               .withColumn("salt_id", F.explode("salt_array")) \
                               .withColumn("salt", F.concat(F.col("join_key"), F.lit("_"), F.col("salt_id")))

# Step 3: Perform balanced join
df_result = df_salted.join(df_lookup_replicated, on="salt", how="inner")
```

### 2. Enabling Adaptive Query Execution (AQE) Best Practices
```python
# Essential production configurations for Apache Spark 3.5+
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "67108864")  # 64MB
```

---

## 🧪 Automated Testing & Continuous Integration

This repository enforces strict code quality and structural contracts on every commit via GitHub Actions:
- **Notebook File Contract:** Exactly 105 `.py` Databricks notebooks must exist.
- **Databricks Formatting:** Every notebook begins with `# Databricks notebook source` without empty prepended cells, uses valid `# COMMAND ----------` delimiters, and includes `# MAGIC %md` documentation.
- **Zero Video / Extraneous References:** Verified completely free of video timestamps, video links, or external author references.
- **AST Syntax Check:** All code blocks pass standard Python AST validation.

To run the verification suite locally:
```bash
# Validate notebook format and AST compliance
python scripts/validate_notebooks.py

# Run integrity tests
python tests/test_notebook_integrity.py
```

---

## 🤝 Contributing

Contributions, feedback, and optimization recipes are warmly welcomed!
1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/awesome-pyspark-recipe`)
3. Commit your Changes (`git commit -m 'Add SCD2 PySpark implementation'`)
4. Push to the Branch (`git push origin feature/awesome-pyspark-recipe`)
5. Open a Pull Request

---

## ⭐ Star History

If this handbook helped you ace an interview or optimize a production lakehouse pipeline, **please give it a star!** ⭐

[![Star History Chart](https://api.star-history.com/svg?repos=santoshjammi29/pyspark-handbook&type=Date)](https://star-history.com/#santoshjammi29/pyspark-handbook&Date)

---

## 📜 License & Acknowledgments

This project is licensed under the **[MIT License](LICENSE)** — feel free to use it for personal study, enterprise lakehouse development, internal team training, and technical interview preparation.

<div align="center">

Maintained with ❤️ by [Santosh Jammi](https://github.com/santoshjammi29)

</div>
