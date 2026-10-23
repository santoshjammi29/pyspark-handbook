# ⚡ Apache Spark & Databricks Lakehouse: 105 Coding Use Cases Masterclass

[![CI Notebook Validation](https://github.com/santoshjammi29/spark-coding-usecases-masterclass/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshjammi29/spark-coding-usecases-masterclass/actions)
[![PySpark](https://img.shields.io/badge/PySpark-3.4%2B-FDEE21?logo=apachespark&logoColor=black)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Databricks Unity Catalog](https://img.shields.io/badge/Databricks-Unity_Catalog-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Notebook Count](https://img.shields.io/badge/Notebooks-105_Complete-success.svg)](#-master-catalog-of-105-use-cases)

Welcome to the **Spark Coding Use Cases Masterclass**! This repository is a comprehensive, production-grade collection of **105 fully executable Jupyter Notebooks**, each solving a concrete real-world engineering challenge across Apache Spark, PySpark, Delta Lake, and the Databricks Lakehouse platform.

Every single notebook is **100% self-contained**—with realistic synthetic mock datasets, architectural explanations, production best practices, error-handling patterns, and senior data engineering interview questions.

---

## 📺 Masterclass Reference Alignment
This curriculum directly expands and codifies the architectural principles from the **[Databricks All in One Free Masterclass Playlist](https://www.youtube.com/playlist?list=PLlHuhl-cAAnH30NbAzagvT4FXSWu94uQS)** by *DataBeli*, mapping each video concept to hands-on notebooks:

| Masterclass Playlist Theme | Videos | Covered In Modules |
|---|---|---|
| **Databricks Workspaces, UI & Magic Commands** | Videos 02, 04, 06 | Module 01 (Notebooks 001 - 012) |
| **Orchestration, %run vs dbutils.notebook.run** | Video 07, 10 | Module 01, 10 (Notebooks 012, 105) |
| **Secret Scopes & DBUtils File Management** | Videos 08, 09 | Module 01, 09 (Notebooks 012, 087, 091) |
| **Auto Loader (cloudFiles) & Ingestion Engine** | Video 11 | Module 05 (Notebooks 048, 049, 050) |
| **Debugging, Execution Plans & Performance Tuning** | Video 12, 13 | Module 08 (Notebooks 074 - 085) |
| **Cluster Architecture, Photon & Sizing** | Video 13, 29 | Module 08 (Notebooks 081, 084) |
| **Unity Catalog Governance (3-Tier, Volumes, ACLs)** | Video 03, 14 | Module 09 (Notebooks 086 - 089) |
| **FinOps & Job Cost Calculation with SQL** | Video 15 | Module 09 (Notebook 092) |
| **LakeFlow Connect & CDC Ingestion** | Video 16 | Module 05, 07 (Notebooks 047, 071) |
| **Lakeflow Declarative Pipelines & DLT Expectations** | Video 17 | Module 09 (Notebook 093) |
| **Genie AI Agents, Dashboards & Semantic Layer** | Videos 18, 19, 24, 25, 31 | Module 03, 09, 10 (Notebooks 025, 095, 104) |
| **Finance Domain End-to-End Medallion Project** | Video 20 | Module 04, 07, 10 (Notebooks 035, 069, 096) |
| **CI/CD with Databricks Asset Bundles (DABs)** | Video 21 | Module 09 (Notebook 094) |
| **PII Data Masterclass (Masking, AES Encryption)** | Video 22 | Module 01, 09 (Notebooks 005, 089, 090) |
| **Data Architect Interview Questions & Scenarios** | Video 23 | Embedded across all 105 Notebooks! |
| **Retail Multi-Source Data Engineering Project** | Video 26 | Module 07, 10 (Notebooks 072, 097) |
| **Agentic AI & Generative AI on Lakehouse** | Video 27 | Module 10 (Notebook 104) |
| **Data Modeling (Star Schema, SCD 1/2/3/4, Data Vault)** | Video 28 | Module 07 (Notebooks 064 - 073) |

---

## 🗂️ Project Repository Structure

```
spark-coding-usecases-masterclass/
├── .github/
│   └── workflows/
│       └── ci.yml                 # Automated CI running test suite & notebook validation
├── notebooks/
│   ├── 01_core_dataframe_api/     # Notebooks 001 - 012: PySpark session, schema, UDFs, dbutils
│   ├── 02_complex_data_types/     # Notebooks 013 - 022: Arrays, higher-order functions, JSON, XML
│   ├── 03_sql_and_window_functions/# Notebooks 023 - 032: Grouping sets, pivots, ranking, gaps & islands
│   ├── 04_delta_lake_and_acid/    # Notebooks 033 - 043: ACID, MERGE, Time Travel, VACUUM, Z-Order
│   ├── 05_data_ingestion_autoloader/# Notebooks 044 - 053: Auto Loader, CSV, Parquet, Avro, JDBC, APIs
│   ├── 06_streaming_realtime/     # Notebooks 054 - 063: Structured streaming, watermarks, joins, DLQ
│   ├── 07_lakehouse_modeling_scd/ # Notebooks 064 - 073: Medallion architecture, SCD 1/2/3/4, Data Vault
│   ├── 08_performance_and_tuning/ # Notebooks 074 - 085: Catalyst, AQE, Salting, Skew, Bucketing, Photon
│   ├── 09_unity_catalog_and_security/# Notebooks 086 - 095: 3-tier namespace, RLS, Column masking, FinOps
│   └── 10_end_to_end_projects/    # Notebooks 096 - 105: Finance, Retail, Healthcare, IoT, RAG, Alerts
├── scripts/
│   ├── generate_all.py            # Regenerates all 105 notebooks programmatically
│   ├── validate_notebooks.py      # Verifies JSON format, cell count & AST syntax
│   └── export_to_databricks.py    # Converts .ipynb to Databricks native .py source format
├── tests/
│   └── test_notebook_integrity.py # Automated pytest test suite
├── .gitignore
├── LICENSE                        # MIT License
├── pyproject.toml
├── requirements.txt
└── README.md                      # Master documentation & 105-notebook index
```

---

## 🚀 Quick Start Guide

### 1. Run Locally (VS Code / Jupyter / Standard PySpark)

```bash
# Clone the repository
git clone https://github.com/santoshjammi29/spark-coding-usecases-masterclass.git
cd spark-coding-usecases-masterclass

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies (PySpark, Delta Spark, PyArrow, Pandas, PyTest)
pip install -r requirements.txt

# Launch Jupyter Lab
jupyter lab
```

### 2. Import into Databricks Workspace
- **Via Databricks Repos / Git Folders**:
  1. Open your Databricks workspace.
  2. Navigate to **Workspace** > **Repos** (or **Git Folders**).
  3. Click **Add Repo** and paste your GitHub repository URL:
     `https://github.com/santoshjammi29/spark-coding-usecases-masterclass.git`.
  4. All 105 notebooks will open natively in your workspace!

- **Convert to Native Databricks `.py` Format**:
  If you prefer `# Databricks notebook source` format:
  ```bash
  python scripts/export_to_databricks.py
  ```

---

## 📚 Master Catalog of 105 Use Cases


### Module 01: Core PySpark Foundations & DataFrame API (001 - 012)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 001 | [Sparksession Initialization And Dataframe Creation Strategies](notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.ipynb) | 🟢 `Beginner` | A digital commerce platform needs to ingest customer profile extracts from multiple legacy sources (in-memo... |
| 002 | [Schema Enforcement Safe Type Casting And Try Cast Handling](notebooks/01_core_dataframe_api/002_schema_enforcement_safe_type_casting_and_try_cast_handling.ipynb) | 🟢 `Beginner` | A payments processor receives raw transaction dumps where numeric amounts contain currency symbols, timesta... |
| 003 | [Column Manipulation Batch Addition And Projection Pruning](notebooks/01_core_dataframe_api/003_column_manipulation_batch_addition_and_projection_pruning.ipynb) | 🟢 `Beginner` | A SaaS telemetry service needs to enrich raw session events with calculated duration, derived flags, normal... |
| 004 | [Filtering Multi Condition Predicates And Pushdown Analysis](notebooks/01_core_dataframe_api/004_filtering_multi_condition_predicates_and_pushdown_analysis.ipynb) | 🟢 `Beginner` | A logistics fleet management company needs to identify critical alerts: vehicles with engine temperatures e... |
| 005 | [String Operations Regex Extraction And Data Cleansing](notebooks/01_core_dataframe_api/005_string_operations_regex_extraction_and_data_cleansing.ipynb) | 🟢 `Beginner` | A marketing automation system ingests unstructured customer lead notes containing messy phone numbers, emai... |
| 006 | [Date And Timestamp Mastery Parsing Arithmetic And Timezones](notebooks/01_core_dataframe_api/006_date_and_timestamp_mastery_parsing_arithmetic_and_timezones.ipynb) | 🟢 `Beginner` | A global ride-sharing platform records ride dispatch, pickup, and dropoff events across multiple internatio... |
| 007 | [Handling Nulls Nans And Imputation Strategies](notebooks/01_core_dataframe_api/007_handling_nulls_nans_and_imputation_strategies.ipynb) | 🟢 `Beginner` | A medical diagnostics platform aggregates clinical lab test results. Missing biomarker readings occur due t... |
| 008 | [Sorting Ordering Semantics And Stratified Sampling](notebooks/01_core_dataframe_api/008_sorting_ordering_semantics_and_stratified_sampling.ipynb) | 🟢 `Beginner` | A financial auditing team must sample transaction logs for regulatory compliance. The sample must be strati... |
| 009 | [Deduplication Strategies Dropduplicates Vs Window Row Number](notebooks/01_core_dataframe_api/009_deduplication_strategies_dropduplicates_vs_window_row_number.ipynb) | 🟡 `Intermediate` | An e-commerce order ingestion pipeline receives duplicate HTTP webhook payloads. Some duplicates are exact ... |
| 010 | [Transformations Vs Actions Lazy Evaluation And Lineage Graph](notebooks/01_core_dataframe_api/010_transformations_vs_actions_lazy_evaluation_and_lineage_graph.ipynb) | 🟡 `Intermediate` | A performance engineering lead must diagnose why a data pipeline fails after running for 40 minutes, only t... |
| 011 | [Custom Pyspark Udfs Vs Vectorized Pandas Udfs Pyarrow](notebooks/01_core_dataframe_api/011_custom_pyspark_udfs_vs_vectorized_pandas_udfs_pyarrow.ipynb) | 🟡 `Intermediate` | A credit scoring model applies complex non-linear mathematical risk adjustments to customer accounts. The c... |
| 012 | [Emulating Databricks Dbutils Widgets And Secret Scopes Locally](notebooks/01_core_dataframe_api/012_emulating_databricks_dbutils_widgets_and_secret_scopes_locally.ipynb) | 🟡 `Intermediate` | An enterprise migration team develops Databricks pipelines locally inside VS Code or CI/CD test runners bef... |

### Module 02: Complex Data Types & Nested Structures Handling (013 - 022)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 013 | [Array Creation Element Extraction And Array Search Functions](notebooks/02_complex_data_types/013_array_creation_element_extraction_and_array_search_functions.ipynb) | 🟡 `Intermediate` | A digital streaming platform logs user engagement tags, genre preferences, and content playlists as arrays.... |
| 014 | [Spark Higher Order Functions Transform Filter And Aggregate](notebooks/02_complex_data_types/014_spark_higher_order_functions_transform_filter_and_aggregate.ipynb) | 🟡 `Intermediate` | A fintech loyalty platform tracks customer reward points earned across multiple merchant transactions in an... |
| 015 | [Exploding And Flattening Arrays Explode Vs Explode Outer And Posexplode](notebooks/02_complex_data_types/015_exploding_and_flattening_arrays_explode_vs_explode_outer_and_posexplode.ipynb) | 🟡 `Intermediate` | A flight booking aggregator normalizes JSON payloads of airline bookings. Bookings contain lists of passeng... |
| 016 | [Struct Creation Nested Field Access And Updating Sub Fields](notebooks/02_complex_data_types/016_struct_creation_nested_field_access_and_updating_sub_fields.ipynb) | 🟡 `Intermediate` | A customer identity platform groups personal attributes, contact points, and address details into nested st... |
| 017 | [Recursive Schema Traversal Flattening Deeply Nested Json Structs](notebooks/02_complex_data_types/017_recursive_schema_traversal_flattening_deeply_nested_json_structs.ipynb) | 🔴 `Advanced` | An enterprise integration hub ingests complex payloads from ERP and CRM systems containing 5+ levels of nes... |
| 018 | [Maptype Creation Key Extraction And Dynamic Properties](notebooks/02_complex_data_types/018_maptype_creation_key_extraction_and_dynamic_properties.ipynb) | 🟡 `Intermediate` | An advertising technology platform captures custom user ad-tracking attributes (cookies, campaign IDs, UTM ... |
| 019 | [Parsing Raw Json Payloads From Json To Json And Schema Of Json](notebooks/02_complex_data_types/019_parsing_raw_json_payloads_from_json_to_json_and_schema_of_json.ipynb) | 🟡 `Intermediate` | A payment gateway streams raw JSON transaction events via Apache Kafka or cloud storage. Each record arrive... |
| 020 | [Handling Corrupted And Malformed Json Permissive Vs Dropmalformed Vs Failfast](notebooks/02_complex_data_types/020_handling_corrupted_and_malformed_json_permissive_vs_dropmalformed_vs_failfast.ipynb) | 🟡 `Intermediate` | A public data ingestion pipeline scrapes third-party web endpoints. Up to 5% of incoming payloads contain s... |
| 021 | [Xml Data Ingestion Hierarchical Parsing And Tag Structuring](notebooks/02_complex_data_types/021_xml_data_ingestion_hierarchical_parsing_and_tag_structuring.ipynb) | 🟡 `Intermediate` | A legacy healthcare exchange transmits insurance claims in XML format. Claims contain nested patient inform... |
| 022 | [Advanced Aggregations Collect List Vs Collect Set And Sorted Collections](notebooks/02_complex_data_types/022_advanced_aggregations_collect_list_vs_collect_set_and_sorted_collections.ipynb) | 🟡 `Intermediate` | A digital customer journey pipeline aggregates all URLs visited by users within a session. Product teams ne... |

### Module 03: Advanced SQL, Aggregations & Analytical Window Functions (023 - 032)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 023 | [Basic And Multi Column Aggregations With Alias Management](notebooks/03_sql_and_window_functions/023_basic_and_multi_column_aggregations_with_alias_management.ipynb) | 🟢 `Beginner` | A retail analytics engine calculates daily regional performance metrics across store departments: total gro... |
| 024 | [Advanced Multidimensional Olap Rollup Cube And Grouping Sets](notebooks/03_sql_and_window_functions/024_advanced_multidimensional_olap_rollup_cube_and_grouping_sets.ipynb) | 🟡 `Intermediate` | A financial controller needs a single unified sales reporting view that includes: (1) Detailed breakdown by... |
| 025 | [Pivoting And Unpivoting Data Pivot And Stack Expression Mastery](notebooks/03_sql_and_window_functions/025_pivoting_and_unpivoting_data_pivot_and_stack_expression_mastery.ipynb) | 🟡 `Intermediate` | A business intelligence dashboard requires monthly revenue pivoted into wide columns (Jan, Feb, Mar...) for... |
| 026 | [Ranking Window Functions Row Number Rank Dense Rank And Percent Rank](notebooks/03_sql_and_window_functions/026_ranking_window_functions_row_number_rank_dense_rank_and_percent_rank.ipynb) | 🟡 `Intermediate` | A national sales management board ranks sales representatives within each geographic territory based on qua... |
| 027 | [Aggregate And Cumulative Window Functions Running Totals And Moving Averages](notebooks/03_sql_and_window_functions/027_aggregate_and_cumulative_window_functions_running_totals_and_moving_averages.ipynb) | 🟡 `Intermediate` | A fintech wealth management app displays account balance trajectories and 7-day smoothed moving averages of... |
| 028 | [Offset Window Functions Lead Lag And Period Over Period Churn](notebooks/03_sql_and_window_functions/028_offset_window_functions_lead_lag_and_period_over_period_churn.ipynb) | 🟡 `Intermediate` | A SaaS subscription billing service analyzes customer MRR (Monthly Recurring Revenue). Finance requires mon... |
| 029 | [Sessionization And The Gaps And Islands Problem In Pyspark](notebooks/03_sql_and_window_functions/029_sessionization_and_the_gaps_and_islands_problem_in_pyspark.ipynb) | 🔴 `Advanced` | A digital analytics platform tracks web page views. An active browsing session is defined as consecutive cl... |
| 030 | [Relational Joins Masterclass Inner Outer Semi And Anti Joins](notebooks/03_sql_and_window_functions/030_relational_joins_masterclass_inner_outer_semi_and_anti_joins.ipynb) | 🟡 `Intermediate` | An enterprise CRM data reconciler syncs accounts between Salesforce and a billing database. The reconciliat... |
| 031 | [Broadcast Hash Joins Threshold Tuning And Memory Limits](notebooks/03_sql_and_window_functions/031_broadcast_hash_joins_threshold_tuning_and_memory_limits.ipynb) | 🟡 `Intermediate` | A point-of-sale stream joins billions of transaction rows with a small 2MB store metadata lookup table. Def... |
| 032 | [Spark Sql Views Temp Tables And Catalyst Sql Optimization](notebooks/03_sql_and_window_functions/032_spark_sql_views_temp_tables_and_catalyst_sql_optimization.ipynb) | 🟢 `Beginner` | A hybrid analytics team consists of Python data engineers and SQL business analysts. The data engineering t... |

### Module 04: Delta Lake Architecture, ACID Operations & Time Travel (033 - 043)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 033 | [Delta Table Creation Metadata Inspection And Transaction Log Internals](notebooks/04_delta_lake_and_acid/033_delta_table_creation_metadata_inspection_and_transaction_log_internals.ipynb) | 🟡 `Intermediate` | A lakehouse platform architect requires an immutable, audit-compliant storage layer for financial ledger re... |
| 034 | [Acid Crud Operations On Delta Insert Update And Delete Mechanics](notebooks/04_delta_lake_and_acid/034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics.ipynb) | 🟡 `Intermediate` | A GDPR/CCPA privacy compliance pipeline requires modifying existing customer records: deleting individuals ... |
| 035 | [Delta Upsert And Merge Into Mastery With Multiple Conditions](notebooks/04_delta_lake_and_acid/035_delta_upsert_and_merge_into_mastery_with_multiple_conditions.ipynb) | 🟡 `Intermediate` | A retail inventory management system synchronizes daily inventory updates from 500 physical warehouses. If ... |
| 036 | [Delta Time Travel Audit History And Version Rollback](notebooks/04_delta_lake_and_acid/036_delta_time_travel_audit_history_and_version_rollback.ipynb) | 🟡 `Intermediate` | A bug in an automated ETL pricing script mistakenly discounted all luxury goods by 90% during an overnight ... |
| 037 | [Vacuum Command Retention Hours And Safe Tombstone Management](notebooks/04_delta_lake_and_acid/037_vacuum_command_retention_hours_and_safe_tombstone_management.ipynb) | 🟡 `Intermediate` | A cloud lakehouse table ingests high-frequency updates, accumulating 50,000 unreferenced obsolete Parquet f... |
| 038 | [Delta Optimize File Compaction And Auto Compaction Strategies](notebooks/04_delta_lake_and_acid/038_delta_optimize_file_compaction_and_auto_compaction_strategies.ipynb) | 🟡 `Intermediate` | A streaming ingestion job writes 1,000 tiny 50KB Parquet files into a Delta table every hour. Query latency... |
| 039 | [Z Order Multi Dimensional Indexing Vs Liquid Clustering Cluster By](notebooks/04_delta_lake_and_acid/039_z_order_multi_dimensional_indexing_vs_liquid_clustering_cluster_by.ipynb) | 🔴 `Advanced` | An ad-tech analytics platform runs ad-hoc queries filtering on combinations of `customer_id`, `event_date`,... |
| 040 | [Delta Change Data Feed Cdf Enabling And Consuming Row Level Changes](notebooks/04_delta_lake_and_acid/040_delta_change_data_feed_cdf_enabling_and_consuming_row_level_changes.ipynb) | 🔴 `Advanced` | An enterprise financial lakehouse architecture maintains a real-time risk dashboard that must react to down... |
| 041 | [Delta Schema Evolution Mergeschema And Schema Enforcement](notebooks/04_delta_lake_and_acid/041_delta_schema_evolution_mergeschema_and_schema_enforcement.ipynb) | 🟡 `Intermediate` | A product catalog service frequently releases new product attributes (e.g. `warranty_months`, `energy_ratin... |
| 042 | [Delta Table Constraints Check And Not Null Data Quality Rules](notebooks/04_delta_lake_and_acid/042_delta_table_constraints_check_and_not_null_data_quality_rules.ipynb) | 🟡 `Intermediate` | A banking compliance application requires strict data quality invariants on account records: account balanc... |
| 043 | [In Place Parquet To Delta Lake Migration With Convert To Delta](notebooks/04_delta_lake_and_acid/043_in_place_parquet_to_delta_lake_migration_with_convert_to_delta.ipynb) | 🟡 `Intermediate` | An enterprise data platform has 100TB of historical data stored across partitioned Parquet directories in A... |

### Module 05: Data Ingestion Patterns, Auto Loader & File Formats (044 - 053)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 044 | [Csv Ingestion Edge Cases Multiline Escaped Quotes And Custom Delimiters](notebooks/05_data_ingestion_autoloader/044_csv_ingestion_edge_cases_multiline_escaped_quotes_and_custom_delimiters.ipynb) | 🟡 `Intermediate` | A financial auditing pipeline imports legacy bank statement extracts. The files contain multiline transacti... |
| 045 | [Parquet And Orc Columnar Ingestion Predicate Pushdown And Dictionary Encoding](notebooks/05_data_ingestion_autoloader/045_parquet_and_orc_columnar_ingestion_predicate_pushdown_and_dictionary_encoding.ipynb) | 🟡 `Intermediate` | A petabyte-scale telemetry lakehouse queries 500 million vehicle diagnostic events daily. Analyst queries t... |
| 046 | [Avro Binary Ingestion And Schema Registry Compatibility](notebooks/05_data_ingestion_autoloader/046_avro_binary_ingestion_and_schema_registry_compatibility.ipynb) | 🟡 `Intermediate` | An enterprise Apache Kafka event bus publishes high-throughput customer account updates serialized in Apach... |
| 047 | [Jdbc Parallel Ingestion Partitioning By Column Fetchsize And Connection Sizing](notebooks/05_data_ingestion_autoloader/047_jdbc_parallel_ingestion_partitioning_by_column_fetchsize_and_connection_sizing.ipynb) | 🟡 `Intermediate` | A night batch pipeline extracts 50 million sales transaction records from an operational PostgreSQL product... |
| 048 | [Databricks Auto Loader Directory Mode Cloudfiles Schema Inference And Checkpointing](notebooks/05_data_ingestion_autoloader/048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.ipynb) | 🟡 `Intermediate` | A cloud storage bucket receives thousands of landing files (JSON and CSV) every hour from IoT devices. Trad... |
| 049 | [Auto Loader Schema Evolution And Rescued Data Column Rescued Data](notebooks/05_data_ingestion_autoloader/049_auto_loader_schema_evolution_and_rescued_data_column_rescued_data.ipynb) | 🟡 `Intermediate` | Third-party vendors send telemetry files where new columns appear without prior warning, or column types ch... |
| 050 | [Building A Metadata Driven Multi Format Ingestion Engine](notebooks/05_data_ingestion_autoloader/050_building_a_metadata_driven_multi_format_ingestion_engine.ipynb) | 🔴 `Advanced` | A centralized lakehouse ingestion platform must onboard 200+ distinct source datasets daily spanning CSV, P... |
| 051 | [Incremental Batch File Discovery Watermarking And Modification Timestamps](notebooks/05_data_ingestion_autoloader/051_incremental_batch_file_discovery_watermarking_and_modification_timestamps.ipynb) | 🟡 `Intermediate` | A legacy data pipeline runs hourly batch jobs that read from an S3/ADLS bucket where external partners drop... |
| 052 | [Handling Compressed Archives Zip Tar Gz Bz2 In Distributed Pyspark](notebooks/05_data_ingestion_autoloader/052_handling_compressed_archives_zip_tar_gz_bz2_in_distributed_pyspark.ipynb) | 🔴 `Advanced` | A regulatory agency deposits quarterly compliance archives as massive 5GB ZIP and TAR.GZ files. Standard Sp... |
| 053 | [Distributed Rest Api And Webhook Ingestion Without Driver Oom](notebooks/05_data_ingestion_autoloader/053_distributed_rest_api_and_webhook_ingestion_without_driver_oom.ipynb) | 🔴 `Advanced` | A financial data vendor exposes historical equity prices via an authenticated REST API with rate limits. Th... |

### Module 06: Streaming & Near Real-Time Processing (054 - 063)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 054 | [Structured Streaming Foundations Sources Execution Model And Query Progress](notebooks/06_streaming_realtime/054_structured_streaming_foundations_sources_execution_model_and_query_progress.ipynb) | 🟡 `Intermediate` | A payments platform monitors live transaction throughput. As card swipes occur across the globe, an unbound... |
| 055 | [Streaming Triggers Processingtime Once And Availablenow For Cost Optimization](notebooks/06_streaming_realtime/055_streaming_triggers_processingtime_once_and_availablenow_for_cost_optimization.ipynb) | 🟡 `Intermediate` | A cloud financial platform runs streaming pipelines. Running clusters 24/7 costs $50,000/month. Because sou... |
| 056 | [Streaming Window Aggregations And Watermarking For Late Arriving Events](notebooks/06_streaming_realtime/056_streaming_window_aggregations_and_watermarking_for_late_arriving_events.ipynb) | 🔴 `Advanced` | A mobile gaming platform aggregates player score events over 10-minute tumbling windows. Due to intermitten... |
| 057 | [Stream Static Joins With Slowly Changing Dimension Lookups](notebooks/06_streaming_realtime/057_stream_static_joins_with_slowly_changing_dimension_lookups.ipynb) | 🟡 `Intermediate` | A financial fraud detection system processes streaming credit card swipe authorizations. Each incoming tran... |
| 058 | [Stream Stream Dual Watermark Joins With Interval Conditions](notebooks/06_streaming_realtime/058_stream_stream_dual_watermark_joins_with_interval_conditions.ipynb) | 🔴 `Advanced` | An online retail logistics platform matches checkout order events with payment confirmation events arriving... |
| 059 | [Streaming Deduplication Across Time Windows With Watermarks](notebooks/06_streaming_realtime/059_streaming_deduplication_across_time_windows_with_watermarks.ipynb) | 🟡 `Intermediate` | An IoT gateway ingests sensor readings from connected vehicles over cellular networks. When vehicles enter ... |
| 060 | [Idempotent Acid Delta Upserts Using Writestream Foreachbatch](notebooks/06_streaming_realtime/060_idempotent_acid_delta_upserts_using_writestream_foreachbatch.ipynb) | 🔴 `Advanced` | A payments streaming pipeline consumes live financial transactions that include both new purchases and refu... |
| 061 | [Arbitrary Stateful Streaming Operations With Flatmapgroupswithstate Concepts](notebooks/06_streaming_realtime/061_arbitrary_stateful_streaming_operations_with_flatmapgroupswithstate_concepts.ipynb) | 🔴 `Advanced` | A telecommunications security monitor detects SIM-swap and fraud patterns across customer cell towers. A fr... |
| 062 | [Dead Letter Queue Dlq Quarantine Streaming Pattern In Pyspark](notebooks/06_streaming_realtime/062_dead_letter_queue_dlq_quarantine_streaming_pattern_in_pyspark.ipynb) | 🟡 `Intermediate` | A real-time clickstream ingestion pipeline processes 10,000 events/second. A software release on the web cl... |
| 063 | [Streaming Observability Streamingquerylistener And Sla Latency Alerting](notebooks/06_streaming_realtime/063_streaming_observability_streamingquerylistener_and_sla_latency_alerting.ipynb) | 🔴 `Advanced` | An enterprise fintech streaming pipeline must maintain an SLA of processing events within 60 seconds of ori... |

### Module 07: Lakehouse Data Modeling, CDC & SCD Implementation (064 - 073)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 064 | [Medallion Architecture Bronze Ingestion Layer Standards And Audit Trails](notebooks/07_lakehouse_modeling_scd/064_medallion_architecture_bronze_ingestion_layer_standards_and_audit_trails.ipynb) | 🟡 `Intermediate` | An enterprise data lakehouse ingests raw transactional data from ERP, CRM, and partner APIs. Data must land... |
| 065 | [Medallion Architecture Silver Transformation Cleansing And Conformance](notebooks/07_lakehouse_modeling_scd/065_medallion_architecture_silver_transformation_cleansing_and_conformance.ipynb) | 🟡 `Intermediate` | Raw invoices from the Bronze layer contain duplicate entries, unstandardized status codes ('appr', 'APPROVE... |
| 066 | [Medallion Architecture Gold Business Aggregations And Dimensional Serving](notebooks/07_lakehouse_modeling_scd/066_medallion_architecture_gold_business_aggregations_and_dimensional_serving.ipynb) | 🟡 `Intermediate` | Executive leadership and Power BI dashboards require pre-aggregated quarterly vendor performance scorecards... |
| 067 | [Distributed Surrogate Key Generation Monotonically Increasing Id Vs Xxhash64 Vs Uuids](notebooks/07_lakehouse_modeling_scd/067_distributed_surrogate_key_generation_monotonically_increasing_id_vs_xxhash64_vs_uuids.ipynb) | 🟡 `Intermediate` | A lakehouse dimensional model builds customer and account dimension tables. Business analysts require synth... |
| 068 | [Slowly Changing Dimension Type 1 Scd Type 1 In Place Overwrites Via Delta Merge](notebooks/07_lakehouse_modeling_scd/068_slowly_changing_dimension_type_1_scd_type_1_in_place_overwrites_via_delta_merge.ipynb) | 🟡 `Intermediate` | A master customer dimension stores current contact email, phone number, and address. Under business policy,... |
| 069 | [Slowly Changing Dimension Type 2 Scd Type 2 Historical Tracking With Effective Dates](notebooks/07_lakehouse_modeling_scd/069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.ipynb) | 🔴 `Advanced` | An insurance underwriting platform tracks policyholder residential address history. Premium risk calculatio... |
| 070 | [Slowly Changing Dimension Type 3 And Type 4 Multi Tier Implementation](notebooks/07_lakehouse_modeling_scd/070_slowly_changing_dimension_type_3_and_type_4_multi_tier_implementation.ipynb) | 🟡 `Intermediate` | A sales commission system tracks territory reassignments. Sales reps occasionally switch regions. Compensat... |
| 071 | [Change Data Capture Cdc Ingestion From Debezium Kafka Into Lakehouse](notebooks/07_lakehouse_modeling_scd/071_change_data_capture_cdc_ingestion_from_debezium_kafka_into_lakehouse.ipynb) | 🔴 `Advanced` | A transactional MySQL inventory database streams row-level changes into Kafka via Debezium CDC. Debezium pu... |
| 072 | [Star Schema Implementation Fact And Conformed Dimension Modeling In Spark](notebooks/07_lakehouse_modeling_scd/072_star_schema_implementation_fact_and_conformed_dimension_modeling_in_spark.ipynb) | 🟡 `Intermediate` | A retail enterprise transitions from a monolithic relational data warehouse to a Databricks Lakehouse. BI a... |
| 073 | [Data Vault 2 0 In The Lakehouse Hubs Links And Satellites Modeling](notebooks/07_lakehouse_modeling_scd/073_data_vault_2_0_in_the_lakehouse_hubs_links_and_satellites_modeling.ipynb) | 🔴 `Advanced` | A large banking institution integrates 30 disparate core banking, loan, and credit systems. Source systems ... |

### Module 08: Performance Tuning, Query Optimization & Skew Mitigation (074 - 085)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 074 | [Catalyst Optimizer Deep Dive Parsed Analyzed Optimized Plans And Code Generation](notebooks/08_performance_and_tuning/074_catalyst_optimizer_deep_dive_parsed_analyzed_optimized_plans_and_code_generation.ipynb) | 🔴 `Advanced` | A lead data architect conducts code reviews for data pipelines. Junior engineers write complex nested SQL a... |
| 075 | [Adaptive Query Execution Aqe Dynamic Coalescing Join Switching And Skew Joins](notebooks/08_performance_and_tuning/075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins.ipynb) | 🔴 `Advanced` | A 2TB batch ETL pipeline runs with `spark.sql.shuffle.partitions = 200`. During filtering stages, data volu... |
| 076 | [Data Skew Detection And Mitigation Using Salting And Two Stage Joins](notebooks/08_performance_and_tuning/076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins.ipynb) | 🔴 `Advanced` | A national e-commerce platform joins website clicks with user accounts. Out of 100 million clicks, 40% belo... |
| 077 | [Partitioning Strategies Avoiding Over Partitioning And Directory Pruning](notebooks/08_performance_and_tuning/077_partitioning_strategies_avoiding_over_partitioning_and_directory_pruning.ipynb) | 🟡 `Intermediate` | A telemetry pipeline partitioned data by `year/month/day/hour/device_id`. After 6 months, the table contain... |
| 078 | [Bucketing For Pre Sorted Join Optimization And Shuffle Elimination](notebooks/08_performance_and_tuning/078_bucketing_for_pre_sorted_join_optimization_and_shuffle_elimination.ipynb) | 🔴 `Advanced` | Two multi-billion-row fact tables (Orders and Shipments) are joined hourly in a critical financial pipeline... |
| 079 | [Caching And Persistence Storagelevel Memory And Disk Vs Cache Traps](notebooks/08_performance_and_tuning/079_caching_and_persistence_storagelevel_memory_and_disk_vs_cache_traps.ipynb) | 🟡 `Intermediate` | A machine learning feature engineering notebook calls `df.cache()` on a 50GB dataset. Later in the pipeline... |
| 080 | [Repartition Vs Coalesce Internals Full Shuffle Vs Partition Minimization](notebooks/08_performance_and_tuning/080_repartition_vs_coalesce_internals_full_shuffle_vs_partition_minimization.ipynb) | 🟡 `Intermediate` | A data pipeline writes daily reports to cloud storage. An engineer notices that writing a 5MB aggregated ta... |
| 081 | [Shuffle Partition Sizing And Tuning For Small Vs Terabyte Scale Jobs](notebooks/08_performance_and_tuning/081_shuffle_partition_sizing_and_tuning_for_small_vs_terabyte_scale_jobs.ipynb) | 🟡 `Intermediate` | A 100MB dataset query takes 45 seconds because Spark defaults to 200 shuffle partitions (`spark.sql.shuffle... |
| 082 | [Diagnosing And Fixing Outofmemory Oom Errors Driver Vs Executor Heap](notebooks/08_performance_and_tuning/082_diagnosing_and_fixing_outofmemory_oom_errors_driver_vs_executor_heap.ipynb) | 🔴 `Advanced` | A production ETL job intermittently crashes with `java.lang.OutOfMemoryError: Java heap space`. On-call eng... |
| 083 | [Spark Ui Eventlog Metrics Task Skew And Shuffle Spill Analysis](notebooks/08_performance_and_tuning/083_spark_ui_eventlog_metrics_task_skew_and_shuffle_spill_analysis.ipynb) | 🔴 `Advanced` | A mission-critical nightly reconciliation job slows down by 3x on weekends. The senior engineer must inspec... |
| 084 | [Vectorized Execution And Photon Engine Acceleration Mechanics](notebooks/08_performance_and_tuning/084_vectorized_execution_and_photon_engine_acceleration_mechanics.ipynb) | 🔴 `Advanced` | An enterprise evaluates migrating high-throughput SQL analytics from classic Apache Spark clusters to Datab... |
| 085 | [Fixing Accidental Cartesian Products And Broadcastnestedloopjoins](notebooks/08_performance_and_tuning/085_fixing_accidental_cartesian_products_and_broadcastnestedloopjoins.ipynb) | 🔴 `Advanced` | A junior engineer submits a join query where the join condition accidentally uses an OR operator across two... |

### Module 09: Databricks Platform, Unity Catalog, Security & FinOps (086 - 095)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 086 | [Unity Catalog Three Tier Namespace Catalogs Schemas And Managed Vs External Tables](notebooks/09_unity_catalog_and_security/086_unity_catalog_three_tier_namespace_catalogs_schemas_and_managed_vs_external_tables.ipynb) | 🟡 `Intermediate` | An enterprise migrates from the legacy Hive Metastore (`hive_metastore.default`) to Databricks Unity Catalo... |
| 087 | [Unity Catalog Volumes For Unstructured And Semi Structured Data Governance](notebooks/09_unity_catalog_and_security/087_unity_catalog_volumes_for_unstructured_and_semi_structured_data_governance.ipynb) | 🟡 `Intermediate` | A machine learning team manages non-tabular files (PDF documents, medical images, audio files, and model we... |
| 088 | [Dynamic Row Level Security Rls Using Row Filters And User Functions](notebooks/09_unity_catalog_and_security/088_dynamic_row_level_security_rls_using_row_filters_and_user_functions.ipynb) | 🔴 `Advanced` | A multinational sales organization maintains a single centralized `sales_pipeline` table. Regional sales re... |
| 089 | [Dynamic Column Masking For Pii Protection And Compliance](notebooks/09_unity_catalog_and_security/089_dynamic_column_masking_for_pii_protection_and_compliance.ipynb) | 🟡 `Intermediate` | A medical insurance database stores member Social Security Numbers (SSN), full names, and email addresses. ... |
| 090 | [Pii Encryption Salted Hashing And Aes Decryption Aes Encrypt Sha2](notebooks/09_unity_catalog_and_security/090_pii_encryption_salted_hashing_and_aes_decryption_aes_encrypt_sha2.ipynb) | 🔴 `Advanced` | A payment processing platform stores credit card Primary Account Numbers (PANs). PCI-DSS regulatory standar... |
| 091 | [Databricks Secret Scopes And Secure Credential Injection Via Dbutils Secrets](notebooks/09_unity_catalog_and_security/091_databricks_secret_scopes_and_secure_credential_injection_via_dbutils_secrets.ipynb) | 🟡 `Intermediate` | A data integration pipeline connects to cloud databases, external APIs (Stripe, Salesforce), and message qu... |
| 092 | [Finops And Dbu Cost Calculation Using System Billing Usage System Tables](notebooks/09_unity_catalog_and_security/092_finops_and_dbu_cost_calculation_using_system_billing_usage_system_tables.ipynb) | 🔴 `Advanced` | A Director of Data Engineering needs to track monthly Databricks DBU (Databricks Unit) consumption across b... |
| 093 | [Lakeflow Declarative Pipelines And Delta Live Tables Dlt Expectations](notebooks/09_unity_catalog_and_security/093_lakeflow_declarative_pipelines_and_delta_live_tables_dlt_expectations.ipynb) | 🔴 `Advanced` | An enterprise financial lakehouse migrates manual notebook pipelines to Databricks Lakeflow Declarative Pip... |
| 094 | [Databricks Asset Bundles Dabs For Multi Environment Ci Cd Automation](notebooks/09_unity_catalog_and_security/094_databricks_asset_bundles_dabs_for_multi_environment_ci_cd_automation.ipynb) | 🔴 `Advanced` | An enterprise development team deploys 50 Databricks workflows and pipelines across development, staging, a... |
| 095 | [Semantic Layer Modeling And Databricks Metric Views For Bi And Ai Agents](notebooks/09_unity_catalog_and_security/095_semantic_layer_modeling_and_databricks_metric_views_for_bi_and_ai_agents.ipynb) | 🔴 `Advanced` | Different business teams calculate 'Customer Churn' and 'Net Revenue' using conflicting SQL formulas, produ... |

### Module 10: End-to-End Enterprise Projects & Production Workflows (096 - 105)

| # | Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 096 | [Project Finance Real Time Credit Card Fraud Detection And Customer 360](notebooks/10_end_to_end_projects/096_project_finance_real_time_credit_card_fraud_detection_and_customer_360.ipynb) | 🔴 `Advanced` | A tier-1 retail bank processes 50 million credit card swipe transactions daily. Risk engineering must build... |
| 097 | [Project Retail Omnichannel Multi Source Lakehouse Etl](notebooks/10_end_to_end_projects/097_project_retail_omnichannel_multi_source_lakehouse_etl.ipynb) | 🔴 `Advanced` | A major omnichannel retailer operates physical retail stores, an e-commerce website, and customer support v... |
| 098 | [Project Healthcare Hipaa Compliant Clinical Records De Identification](notebooks/10_end_to_end_projects/098_project_healthcare_hipaa_compliant_clinical_records_de_identification.ipynb) | 🔴 `Advanced` | A hospital research network prepares electronic health records (EHR) for clinical cancer research. HIPAA Sa... |
| 099 | [Project Telecom Customer Churn Prediction Feature Store And Window Features](notebooks/10_end_to_end_projects/099_project_telecom_customer_churn_prediction_feature_store_and_window_features.ipynb) | 🔴 `Advanced` | A major mobile carrier experiences 2% monthly customer churn. Data scientists need a centralized Lakehouse ... |
| 100 | [Project Cybersecurity Web Server Access Log Analytics And Threat Detection](notebooks/10_end_to_end_projects/100_project_cybersecurity_web_server_access_log_analytics_and_threat_detection.ipynb) | 🔴 `Advanced` | A cyber defense operations center monitors 100 million web server access logs daily. Security analysts must... |
| 101 | [Project Iot Industrial Telemetry Anomaly Detection Via Z Score Statistics](notebooks/10_end_to_end_projects/101_project_iot_industrial_telemetry_anomaly_detection_via_z_score_statistics.ipynb) | 🔴 `Advanced` | A smart manufacturing plant operates 5,000 robotic welding arms. Bearings experience mechanical fatigue bef... |
| 102 | [Project Supply Chain Inventory Replenishment Forecasting And Reorder Triggers](notebooks/10_end_to_end_projects/102_project_supply_chain_inventory_replenishment_forecasting_and_reorder_triggers.ipynb) | 🔴 `Advanced` | A national grocery chain manages 10,000 SKUs across 20 regional distribution centers. Supply chain disrupti... |
| 103 | [Project E Commerce Collaborative Filtering Recommendation Engine With Spark Mllib](notebooks/10_end_to_end_projects/103_project_e_commerce_collaborative_filtering_recommendation_engine_with_spark_mllib.ipynb) | 🔴 `Advanced` | An online retail store has 1 million products and 5 million shoppers. Product managers want personalized 'R... |
| 104 | [Project Agentic Ai And Rag Lakehouse Document Chunking And Vector Search](notebooks/10_end_to_end_projects/104_project_agentic_ai_and_rag_lakehouse_document_chunking_and_vector_search.ipynb) | 🔴 `Advanced` | An enterprise customer service platform deploys an Agentic AI chatbot powered by Databricks Genie and LLMs.... |
| 105 | [Master Enterprise Production Orchestration Workflow Parameters And Alerting](notebooks/10_end_to_end_projects/105_master_enterprise_production_orchestration_workflow_parameters_and_alerting.ipynb) | 🔴 `Advanced` | A mission-critical enterprise financial pipeline executes 15 dependent tasks every night. The workflow must... |

---

## ⚙️ CI/CD & Quality Assurance

This repository includes continuous integration tests to ensure that every single notebook is valid, well-structured, and executable:
- **`python scripts/validate_notebooks.py`**: Ensures all 105 `.ipynb` files have valid JSON syntax, proper metadata (`nbformat: 4`), complete problem statements, code cells, and valid Python syntax.
- **`pytest tests/`**: Automated test suite asserting module coverage, cell counts, and data contracts.

---

## 🤝 Contributing & License
Contributions, feedback, and additional use cases are welcome! Please feel free to open a Pull Request or Issue.

Licensed under the [MIT License](LICENSE).
