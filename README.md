# ⚡ Apache Spark & Databricks Lakehouse: 105 Coding Use Cases Masterclass

[![CI Notebook Validation](https://github.com/santoshjammi29/spark-coding-usecases-masterclass/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshjammi29/spark-coding-usecases-masterclass/actions)
[![Databricks](https://img.shields.io/badge/Databricks-Native_.py_Notebooks-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4%2B-FDEE21?logo=apachespark&logoColor=black)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-008080?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Notebook Count](https://img.shields.io/badge/Notebooks-105_Complete-success.svg)](#-master-catalog-of-105-use-cases)

Welcome to the **Spark & Databricks Coding Use Cases Masterclass**! This repository is a production-grade enterprise library of **105 native Databricks Python notebooks (`.py`)**, covering essential data engineering patterns, optimization strategies, and architectural designs across Apache Spark, Delta Lake, and the Databricks Lakehouse platform.

Every notebook is structured in the official **Databricks Source Notebook format** (`# Databricks notebook source` and `# COMMAND ----------` cell delimiters) so it syncs directly into **Databricks Repos / Git Folders**, **Databricks Asset Bundles (DABs)**, **VS Code**, or standard Python runtimes.

---

## 🏗️ Curriculum Architecture Overview

The 105 use cases are organized across 10 progressive, enterprise modules:

```
spark-coding-usecases-masterclass/
├── .github/
│   └── workflows/
│       └── ci.yml                 # Automated CI running test suite & notebook validation
├── notebooks/
│   ├── 01_core_dataframe_api/     # 001 - 012: Core DataFrame API, Schemas, Arrow Pandas UDFs, DBUtils
│   ├── 02_complex_data_types/     # 013 - 022: Arrays, Higher-Order Functions, Nested Structs, JSON, XML
│   ├── 03_sql_and_window_functions/# 023 - 032: OLAP Cubes, Pivots, Window Functions, Joins, Catalyst SQL
│   ├── 04_delta_lake_and_acid/    # 033 - 043: Transaction Log, ACID CRUD, MERGE, Time Travel, Liquid Clustering
│   ├── 05_data_ingestion_autoloader/# 044 - 053: Auto Loader, Rescued Data, Parallel JDBC, Parquet, Archives
│   ├── 06_streaming_realtime/     # 054 - 063: Structured Streaming, Triggers, Watermarks, Dual-Stream Joins, DLQ
│   ├── 07_lakehouse_modeling_scd/ # 064 - 073: Medallion Architecture, SCD 1/2/3/4, Debezium CDC, Data Vault 2.0
│   ├── 08_performance_and_tuning/ # 074 - 085: Catalyst Optimizer, AQE, Salting Skew, Bucketing, Photon, OOM Fixes
│   ├── 09_unity_catalog_and_security/# 086 - 095: 3-Tier Namespace, Volumes, RLS, Column Masking, FinOps, DABs
│   └── 10_end_to_end_projects/    # 096 - 105: Finance Fraud, Omnichannel Retail, Healthcare, IoT, RAG, Alerts
├── scripts/
│   ├── generate_all.py            # Regenerates all 105 notebooks programmatically
│   ├── validate_notebooks.py      # Verifies Databricks source structure, commands & syntax
│   ├── export_to_ipynb.py         # Converts .py notebooks to Jupyter .ipynb format
│   └── generate_readme.py         # Rebuilds master catalog index
├── tests/
│   └── test_notebook_integrity.py # Pytest test suite asserting notebook contracts
├── .gitignore
├── LICENSE                        # MIT License
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 🚀 Deployment & Usage

### 1. Direct Sync into Databricks Repos / Git Folders (Recommended)
Because all notebooks are native Databricks `.py` source files, they sync seamlessly with Databricks Workspaces:
1. Open your **Databricks Workspace**.
2. Click **Workspace** > **Repos** (or **Git Folders**).
3. Click **Add Repo** and paste:
   ```
   https://github.com/santoshjammi29/spark-coding-usecases-masterclass.git
   ```
4. All 105 notebooks will immediately render with native Databricks cells, markdown headings, widgets, and interactive execution!

### 2. Run Locally in VS Code or Terminal
```bash
# Clone the repository
git clone https://github.com/santoshjammi29/spark-coding-usecases-masterclass.git
cd spark-coding-usecases-masterclass

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run any notebook directly as a Python script
python notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.py
```

### 3. Convert to Jupyter `.ipynb` (Optional)
If you prefer viewing in Jupyter Lab:
```bash
python scripts/export_to_ipynb.py
```
This generates standard `.ipynb` files under `jupyter_exports/`.

---

## 📚 Master Catalog of 105 Use Cases


### Module 01: Core PySpark Foundations & DataFrame API (001 - 012)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 001 | [`001_sparksession_initialization_and_dataframe_creation_strategies.py`](notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.py) | 🟢 `Beginner` | A digital commerce platform needs to ingest customer profile extracts from multiple legacy sources (in-memory li... |
| 002 | [`002_schema_enforcement_safe_type_casting_and_try_cast_handling.py`](notebooks/01_core_dataframe_api/002_schema_enforcement_safe_type_casting_and_try_cast_handling.py) | 🟢 `Beginner` | A payments processor receives raw transaction dumps where numeric amounts contain currency symbols, timestamps a... |
| 003 | [`003_column_manipulation_batch_addition_and_projection_pruning.py`](notebooks/01_core_dataframe_api/003_column_manipulation_batch_addition_and_projection_pruning.py) | 🟢 `Beginner` | A SaaS telemetry service needs to enrich raw session events with calculated duration, derived flags, normalized ... |
| 004 | [`004_filtering_multi_condition_predicates_and_pushdown_analysis.py`](notebooks/01_core_dataframe_api/004_filtering_multi_condition_predicates_and_pushdown_analysis.py) | 🟢 `Beginner` | A logistics fleet management company needs to identify critical alerts: vehicles with engine temperatures exceed... |
| 005 | [`005_string_operations_regex_extraction_and_data_cleansing.py`](notebooks/01_core_dataframe_api/005_string_operations_regex_extraction_and_data_cleansing.py) | 🟢 `Beginner` | A marketing automation system ingests unstructured customer lead notes containing messy phone numbers, email add... |
| 006 | [`006_date_and_timestamp_mastery_parsing_arithmetic_and_timezones.py`](notebooks/01_core_dataframe_api/006_date_and_timestamp_mastery_parsing_arithmetic_and_timezones.py) | 🟢 `Beginner` | A global ride-sharing platform records ride dispatch, pickup, and dropoff events across multiple international t... |
| 007 | [`007_handling_nulls_nans_and_imputation_strategies.py`](notebooks/01_core_dataframe_api/007_handling_nulls_nans_and_imputation_strategies.py) | 🟢 `Beginner` | A medical diagnostics platform aggregates clinical lab test results. Missing biomarker readings occur due to mac... |
| 008 | [`008_sorting_ordering_semantics_and_stratified_sampling.py`](notebooks/01_core_dataframe_api/008_sorting_ordering_semantics_and_stratified_sampling.py) | 🟢 `Beginner` | A financial auditing team must sample transaction logs for regulatory compliance. The sample must be stratified ... |
| 009 | [`009_deduplication_strategies_dropduplicates_vs_window_row_number.py`](notebooks/01_core_dataframe_api/009_deduplication_strategies_dropduplicates_vs_window_row_number.py) | 🟡 `Intermediate` | An e-commerce order ingestion pipeline receives duplicate HTTP webhook payloads. Some duplicates are exact ident... |
| 010 | [`010_transformations_vs_actions_lazy_evaluation_and_lineage_graph.py`](notebooks/01_core_dataframe_api/010_transformations_vs_actions_lazy_evaluation_and_lineage_graph.py) | 🟡 `Intermediate` | A performance engineering lead must diagnose why a data pipeline fails after running for 40 minutes, only to cra... |
| 011 | [`011_custom_pyspark_udfs_vs_vectorized_pandas_udfs_pyarrow.py`](notebooks/01_core_dataframe_api/011_custom_pyspark_udfs_vs_vectorized_pandas_udfs_pyarrow.py) | 🟡 `Intermediate` | A credit scoring model applies complex non-linear mathematical risk adjustments to customer accounts. The curren... |
| 012 | [`012_emulating_databricks_dbutils_widgets_and_secret_scopes_locally.py`](notebooks/01_core_dataframe_api/012_emulating_databricks_dbutils_widgets_and_secret_scopes_locally.py) | 🟡 `Intermediate` | An enterprise migration team develops Databricks pipelines locally inside VS Code or CI/CD test runners before d... |

### Module 02: Complex Data Types & Nested Structures Handling (013 - 022)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 013 | [`013_array_creation_element_extraction_and_array_search_functions.py`](notebooks/02_complex_data_types/013_array_creation_element_extraction_and_array_search_functions.py) | 🟡 `Intermediate` | A digital streaming platform logs user engagement tags, genre preferences, and content playlists as arrays. Mark... |
| 014 | [`014_spark_higher_order_functions_transform_filter_and_aggregate.py`](notebooks/02_complex_data_types/014_spark_higher_order_functions_transform_filter_and_aggregate.py) | 🟡 `Intermediate` | A fintech loyalty platform tracks customer reward points earned across multiple merchant transactions in an arra... |
| 015 | [`015_exploding_and_flattening_arrays_explode_vs_explode_outer_and_posexplode.py`](notebooks/02_complex_data_types/015_exploding_and_flattening_arrays_explode_vs_explode_outer_and_posexplode.py) | 🟡 `Intermediate` | A flight booking aggregator normalizes JSON payloads of airline bookings. Bookings contain lists of passengers a... |
| 016 | [`016_struct_creation_nested_field_access_and_updating_sub_fields.py`](notebooks/02_complex_data_types/016_struct_creation_nested_field_access_and_updating_sub_fields.py) | 🟡 `Intermediate` | A customer identity platform groups personal attributes, contact points, and address details into nested structs... |
| 017 | [`017_recursive_schema_traversal_flattening_deeply_nested_json_structs.py`](notebooks/02_complex_data_types/017_recursive_schema_traversal_flattening_deeply_nested_json_structs.py) | 🔴 `Advanced` | An enterprise integration hub ingests complex payloads from ERP and CRM systems containing 5+ levels of nested s... |
| 018 | [`018_maptype_creation_key_extraction_and_dynamic_properties.py`](notebooks/02_complex_data_types/018_maptype_creation_key_extraction_and_dynamic_properties.py) | 🟡 `Intermediate` | An advertising technology platform captures custom user ad-tracking attributes (cookies, campaign IDs, UTM tags,... |
| 019 | [`019_parsing_raw_json_payloads_from_json_to_json_and_schema_of_json.py`](notebooks/02_complex_data_types/019_parsing_raw_json_payloads_from_json_to_json_and_schema_of_json.py) | 🟡 `Intermediate` | A payment gateway streams raw JSON transaction events via Apache Kafka or cloud storage. Each record arrives as ... |
| 020 | [`020_handling_corrupted_and_malformed_json_permissive_vs_dropmalformed_vs_failfast.py`](notebooks/02_complex_data_types/020_handling_corrupted_and_malformed_json_permissive_vs_dropmalformed_vs_failfast.py) | 🟡 `Intermediate` | A public data ingestion pipeline scrapes third-party web endpoints. Up to 5% of incoming payloads contain syntax... |
| 021 | [`021_xml_data_ingestion_hierarchical_parsing_and_tag_structuring.py`](notebooks/02_complex_data_types/021_xml_data_ingestion_hierarchical_parsing_and_tag_structuring.py) | 🟡 `Intermediate` | A legacy healthcare exchange transmits insurance claims in XML format. Claims contain nested patient information... |
| 022 | [`022_advanced_aggregations_collect_list_vs_collect_set_and_sorted_collections.py`](notebooks/02_complex_data_types/022_advanced_aggregations_collect_list_vs_collect_set_and_sorted_collections.py) | 🟡 `Intermediate` | A digital customer journey pipeline aggregates all URLs visited by users within a session. Product teams need bo... |

### Module 03: Advanced SQL, Aggregations & Analytical Window Functions (023 - 032)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 023 | [`023_basic_and_multi_column_aggregations_with_alias_management.py`](notebooks/03_sql_and_window_functions/023_basic_and_multi_column_aggregations_with_alias_management.py) | 🟢 `Beginner` | A retail analytics engine calculates daily regional performance metrics across store departments: total gross re... |
| 024 | [`024_advanced_multidimensional_olap_rollup_cube_and_grouping_sets.py`](notebooks/03_sql_and_window_functions/024_advanced_multidimensional_olap_rollup_cube_and_grouping_sets.py) | 🟡 `Intermediate` | A financial controller needs a single unified sales reporting view that includes: (1) Detailed breakdown by Year... |
| 025 | [`025_pivoting_and_unpivoting_data_pivot_and_stack_expression_mastery.py`](notebooks/03_sql_and_window_functions/025_pivoting_and_unpivoting_data_pivot_and_stack_expression_mastery.py) | 🟡 `Intermediate` | A business intelligence dashboard requires monthly revenue pivoted into wide columns (Jan, Feb, Mar...) for tabu... |
| 026 | [`026_ranking_window_functions_row_number_rank_dense_rank_and_percent_rank.py`](notebooks/03_sql_and_window_functions/026_ranking_window_functions_row_number_rank_dense_rank_and_percent_rank.py) | 🟡 `Intermediate` | A national sales management board ranks sales representatives within each geographic territory based on quarterl... |
| 027 | [`027_aggregate_and_cumulative_window_functions_running_totals_and_moving_averages.py`](notebooks/03_sql_and_window_functions/027_aggregate_and_cumulative_window_functions_running_totals_and_moving_averages.py) | 🟡 `Intermediate` | A fintech wealth management app displays account balance trajectories and 7-day smoothed moving averages of dail... |
| 028 | [`028_offset_window_functions_lead_lag_and_period_over_period_churn.py`](notebooks/03_sql_and_window_functions/028_offset_window_functions_lead_lag_and_period_over_period_churn.py) | 🟡 `Intermediate` | A SaaS subscription billing service analyzes customer MRR (Monthly Recurring Revenue). Finance requires month-ov... |
| 029 | [`029_sessionization_and_the_gaps_and_islands_problem_in_pyspark.py`](notebooks/03_sql_and_window_functions/029_sessionization_and_the_gaps_and_islands_problem_in_pyspark.py) | 🔴 `Advanced` | A digital analytics platform tracks web page views. An active browsing session is defined as consecutive clicks ... |
| 030 | [`030_relational_joins_masterclass_inner_outer_semi_and_anti_joins.py`](notebooks/03_sql_and_window_functions/030_relational_joins_masterclass_inner_outer_semi_and_anti_joins.py) | 🟡 `Intermediate` | An enterprise CRM data reconciler syncs accounts between Salesforce and a billing database. The reconciliation t... |
| 031 | [`031_broadcast_hash_joins_threshold_tuning_and_memory_limits.py`](notebooks/03_sql_and_window_functions/031_broadcast_hash_joins_threshold_tuning_and_memory_limits.py) | 🟡 `Intermediate` | A point-of-sale stream joins billions of transaction rows with a small 2MB store metadata lookup table. Default ... |
| 032 | [`032_spark_sql_views_temp_tables_and_catalyst_sql_optimization.py`](notebooks/03_sql_and_window_functions/032_spark_sql_views_temp_tables_and_catalyst_sql_optimization.py) | 🟢 `Beginner` | A hybrid analytics team consists of Python data engineers and SQL business analysts. The data engineering team p... |

### Module 04: Delta Lake Architecture, ACID Operations & Time Travel (033 - 043)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 033 | [`033_delta_table_creation_metadata_inspection_and_transaction_log_internals.py`](notebooks/04_delta_lake_and_acid/033_delta_table_creation_metadata_inspection_and_transaction_log_internals.py) | 🟡 `Intermediate` | A lakehouse platform architect requires an immutable, audit-compliant storage layer for financial ledger records... |
| 034 | [`034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics.py`](notebooks/04_delta_lake_and_acid/034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics.py) | 🟡 `Intermediate` | A GDPR/CCPA privacy compliance pipeline requires modifying existing customer records: deleting individuals exerc... |
| 035 | [`035_delta_upsert_and_merge_into_mastery_with_multiple_conditions.py`](notebooks/04_delta_lake_and_acid/035_delta_upsert_and_merge_into_mastery_with_multiple_conditions.py) | 🟡 `Intermediate` | A retail inventory management system synchronizes daily inventory updates from 500 physical warehouses. If an it... |
| 036 | [`036_delta_time_travel_audit_history_and_version_rollback.py`](notebooks/04_delta_lake_and_acid/036_delta_time_travel_audit_history_and_version_rollback.py) | 🟡 `Intermediate` | A bug in an automated ETL pricing script mistakenly discounted all luxury goods by 90% during an overnight run. ... |
| 037 | [`037_vacuum_command_retention_hours_and_safe_tombstone_management.py`](notebooks/04_delta_lake_and_acid/037_vacuum_command_retention_hours_and_safe_tombstone_management.py) | 🟡 `Intermediate` | A cloud lakehouse table ingests high-frequency updates, accumulating 50,000 unreferenced obsolete Parquet files ... |
| 038 | [`038_delta_optimize_file_compaction_and_auto_compaction_strategies.py`](notebooks/04_delta_lake_and_acid/038_delta_optimize_file_compaction_and_auto_compaction_strategies.py) | 🟡 `Intermediate` | A streaming ingestion job writes 1,000 tiny 50KB Parquet files into a Delta table every hour. Query latency has ... |
| 039 | [`039_z_order_multi_dimensional_indexing_vs_liquid_clustering_cluster_by.py`](notebooks/04_delta_lake_and_acid/039_z_order_multi_dimensional_indexing_vs_liquid_clustering_cluster_by.py) | 🔴 `Advanced` | An ad-tech analytics platform runs ad-hoc queries filtering on combinations of `customer_id`, `event_date`, and ... |
| 040 | [`040_delta_change_data_feed_cdf_enabling_and_consuming_row_level_changes.py`](notebooks/04_delta_lake_and_acid/040_delta_change_data_feed_cdf_enabling_and_consuming_row_level_changes.py) | 🔴 `Advanced` | An enterprise financial lakehouse architecture maintains a real-time risk dashboard that must react to downstrea... |
| 041 | [`041_delta_schema_evolution_mergeschema_and_schema_enforcement.py`](notebooks/04_delta_lake_and_acid/041_delta_schema_evolution_mergeschema_and_schema_enforcement.py) | 🟡 `Intermediate` | A product catalog service frequently releases new product attributes (e.g. `warranty_months`, `energy_rating`). ... |
| 042 | [`042_delta_table_constraints_check_and_not_null_data_quality_rules.py`](notebooks/04_delta_lake_and_acid/042_delta_table_constraints_check_and_not_null_data_quality_rules.py) | 🟡 `Intermediate` | A banking compliance application requires strict data quality invariants on account records: account balances mu... |
| 043 | [`043_in_place_parquet_to_delta_lake_migration_with_convert_to_delta.py`](notebooks/04_delta_lake_and_acid/043_in_place_parquet_to_delta_lake_migration_with_convert_to_delta.py) | 🟡 `Intermediate` | An enterprise data platform has 100TB of historical data stored across partitioned Parquet directories in Amazon... |

### Module 05: Data Ingestion Patterns, Auto Loader & File Formats (044 - 053)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 044 | [`044_csv_ingestion_edge_cases_multiline_escaped_quotes_and_custom_delimiters.py`](notebooks/05_data_ingestion_autoloader/044_csv_ingestion_edge_cases_multiline_escaped_quotes_and_custom_delimiters.py) | 🟡 `Intermediate` | A financial auditing pipeline imports legacy bank statement extracts. The files contain multiline transaction de... |
| 045 | [`045_parquet_and_orc_columnar_ingestion_predicate_pushdown_and_dictionary_encoding.py`](notebooks/05_data_ingestion_autoloader/045_parquet_and_orc_columnar_ingestion_predicate_pushdown_and_dictionary_encoding.py) | 🟡 `Intermediate` | A petabyte-scale telemetry lakehouse queries 500 million vehicle diagnostic events daily. Analyst queries typica... |
| 046 | [`046_avro_binary_ingestion_and_schema_registry_compatibility.py`](notebooks/05_data_ingestion_autoloader/046_avro_binary_ingestion_and_schema_registry_compatibility.py) | 🟡 `Intermediate` | An enterprise Apache Kafka event bus publishes high-throughput customer account updates serialized in Apache Avr... |
| 047 | [`047_jdbc_parallel_ingestion_partitioning_by_column_fetchsize_and_connection_sizing.py`](notebooks/05_data_ingestion_autoloader/047_jdbc_parallel_ingestion_partitioning_by_column_fetchsize_and_connection_sizing.py) | 🟡 `Intermediate` | A night batch pipeline extracts 50 million sales transaction records from an operational PostgreSQL production d... |
| 048 | [`048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.py`](notebooks/05_data_ingestion_autoloader/048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.py) | 🟡 `Intermediate` | A cloud storage bucket receives thousands of landing files (JSON and CSV) every hour from IoT devices. Tradition... |
| 049 | [`049_auto_loader_schema_evolution_and_rescued_data_column_rescued_data.py`](notebooks/05_data_ingestion_autoloader/049_auto_loader_schema_evolution_and_rescued_data_column_rescued_data.py) | 🟡 `Intermediate` | Third-party vendors send telemetry files where new columns appear without prior warning, or column types change ... |
| 050 | [`050_building_a_metadata_driven_multi_format_ingestion_engine.py`](notebooks/05_data_ingestion_autoloader/050_building_a_metadata_driven_multi_format_ingestion_engine.py) | 🔴 `Advanced` | A centralized lakehouse ingestion platform must onboard 200+ distinct source datasets daily spanning CSV, Parque... |
| 051 | [`051_incremental_batch_file_discovery_watermarking_and_modification_timestamps.py`](notebooks/05_data_ingestion_autoloader/051_incremental_batch_file_discovery_watermarking_and_modification_timestamps.py) | 🟡 `Intermediate` | A legacy data pipeline runs hourly batch jobs that read from an S3/ADLS bucket where external partners drop new ... |
| 052 | [`052_handling_compressed_archives_zip_tar_gz_bz2_in_distributed_pyspark.py`](notebooks/05_data_ingestion_autoloader/052_handling_compressed_archives_zip_tar_gz_bz2_in_distributed_pyspark.py) | 🔴 `Advanced` | A regulatory agency deposits quarterly compliance archives as massive 5GB ZIP and TAR.GZ files. Standard Spark r... |
| 053 | [`053_distributed_rest_api_and_webhook_ingestion_without_driver_oom.py`](notebooks/05_data_ingestion_autoloader/053_distributed_rest_api_and_webhook_ingestion_without_driver_oom.py) | 🔴 `Advanced` | A financial data vendor exposes historical equity prices via an authenticated REST API with rate limits. The eng... |

### Module 06: Streaming & Near Real-Time Processing (054 - 063)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 054 | [`054_structured_streaming_foundations_sources_execution_model_and_query_progress.py`](notebooks/06_streaming_realtime/054_structured_streaming_foundations_sources_execution_model_and_query_progress.py) | 🟡 `Intermediate` | A payments platform monitors live transaction throughput. As card swipes occur across the globe, an unbounded st... |
| 055 | [`055_streaming_triggers_processingtime_once_and_availablenow_for_cost_optimization.py`](notebooks/06_streaming_realtime/055_streaming_triggers_processingtime_once_and_availablenow_for_cost_optimization.py) | 🟡 `Intermediate` | A cloud financial platform runs streaming pipelines. Running clusters 24/7 costs $50,000/month. Because source d... |
| 056 | [`056_streaming_window_aggregations_and_watermarking_for_late_arriving_events.py`](notebooks/06_streaming_realtime/056_streaming_window_aggregations_and_watermarking_for_late_arriving_events.py) | 🔴 `Advanced` | A mobile gaming platform aggregates player score events over 10-minute tumbling windows. Due to intermittent mob... |
| 057 | [`057_stream_static_joins_with_slowly_changing_dimension_lookups.py`](notebooks/06_streaming_realtime/057_stream_static_joins_with_slowly_changing_dimension_lookups.py) | 🟡 `Intermediate` | A financial fraud detection system processes streaming credit card swipe authorizations. Each incoming transacti... |
| 058 | [`058_stream_stream_dual_watermark_joins_with_interval_conditions.py`](notebooks/06_streaming_realtime/058_stream_stream_dual_watermark_joins_with_interval_conditions.py) | 🔴 `Advanced` | An online retail logistics platform matches checkout order events with payment confirmation events arriving from... |
| 059 | [`059_streaming_deduplication_across_time_windows_with_watermarks.py`](notebooks/06_streaming_realtime/059_streaming_deduplication_across_time_windows_with_watermarks.py) | 🟡 `Intermediate` | An IoT gateway ingests sensor readings from connected vehicles over cellular networks. When vehicles enter tunne... |
| 060 | [`060_idempotent_acid_delta_upserts_using_writestream_foreachbatch.py`](notebooks/06_streaming_realtime/060_idempotent_acid_delta_upserts_using_writestream_foreachbatch.py) | 🔴 `Advanced` | A payments streaming pipeline consumes live financial transactions that include both new purchases and refund up... |
| 061 | [`061_arbitrary_stateful_streaming_operations_with_flatmapgroupswithstate_concepts.py`](notebooks/06_streaming_realtime/061_arbitrary_stateful_streaming_operations_with_flatmapgroupswithstate_concepts.py) | 🔴 `Advanced` | A telecommunications security monitor detects SIM-swap and fraud patterns across customer cell towers. A fraud f... |
| 062 | [`062_dead_letter_queue_dlq_quarantine_streaming_pattern_in_pyspark.py`](notebooks/06_streaming_realtime/062_dead_letter_queue_dlq_quarantine_streaming_pattern_in_pyspark.py) | 🟡 `Intermediate` | A real-time clickstream ingestion pipeline processes 10,000 events/second. A software release on the web client ... |
| 063 | [`063_streaming_observability_streamingquerylistener_and_sla_latency_alerting.py`](notebooks/06_streaming_realtime/063_streaming_observability_streamingquerylistener_and_sla_latency_alerting.py) | 🔴 `Advanced` | An enterprise fintech streaming pipeline must maintain an SLA of processing events within 60 seconds of originat... |

### Module 07: Lakehouse Data Modeling, CDC & SCD Implementation (064 - 073)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 064 | [`064_medallion_architecture_bronze_ingestion_layer_standards_and_audit_trails.py`](notebooks/07_lakehouse_modeling_scd/064_medallion_architecture_bronze_ingestion_layer_standards_and_audit_trails.py) | 🟡 `Intermediate` | An enterprise data lakehouse ingests raw transactional data from ERP, CRM, and partner APIs. Data must land in a... |
| 065 | [`065_medallion_architecture_silver_transformation_cleansing_and_conformance.py`](notebooks/07_lakehouse_modeling_scd/065_medallion_architecture_silver_transformation_cleansing_and_conformance.py) | 🟡 `Intermediate` | Raw invoices from the Bronze layer contain duplicate entries, unstandardized status codes ('appr', 'APPROVED', '... |
| 066 | [`066_medallion_architecture_gold_business_aggregations_and_dimensional_serving.py`](notebooks/07_lakehouse_modeling_scd/066_medallion_architecture_gold_business_aggregations_and_dimensional_serving.py) | 🟡 `Intermediate` | Executive leadership and Power BI dashboards require pre-aggregated quarterly vendor performance scorecards: tot... |
| 067 | [`067_distributed_surrogate_key_generation_monotonically_increasing_id_vs_xxhash64_vs_uuids.py`](notebooks/07_lakehouse_modeling_scd/067_distributed_surrogate_key_generation_monotonically_increasing_id_vs_xxhash64_vs_uuids.py) | 🟡 `Intermediate` | A lakehouse dimensional model builds customer and account dimension tables. Business analysts require synthetic ... |
| 068 | [`068_slowly_changing_dimension_type_1_scd_type_1_in_place_overwrites_via_delta_merge.py`](notebooks/07_lakehouse_modeling_scd/068_slowly_changing_dimension_type_1_scd_type_1_in_place_overwrites_via_delta_merge.py) | 🟡 `Intermediate` | A master customer dimension stores current contact email, phone number, and address. Under business policy, when... |
| 069 | [`069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.py`](notebooks/07_lakehouse_modeling_scd/069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.py) | 🔴 `Advanced` | An insurance underwriting platform tracks policyholder residential address history. Premium risk calculations de... |
| 070 | [`070_slowly_changing_dimension_type_3_and_type_4_multi_tier_implementation.py`](notebooks/07_lakehouse_modeling_scd/070_slowly_changing_dimension_type_3_and_type_4_multi_tier_implementation.py) | 🟡 `Intermediate` | A sales commission system tracks territory reassignments. Sales reps occasionally switch regions. Compensation a... |
| 071 | [`071_change_data_capture_cdc_ingestion_from_debezium_kafka_into_lakehouse.py`](notebooks/07_lakehouse_modeling_scd/071_change_data_capture_cdc_ingestion_from_debezium_kafka_into_lakehouse.py) | 🔴 `Advanced` | A transactional MySQL inventory database streams row-level changes into Kafka via Debezium CDC. Debezium publish... |
| 072 | [`072_star_schema_implementation_fact_and_conformed_dimension_modeling_in_spark.py`](notebooks/07_lakehouse_modeling_scd/072_star_schema_implementation_fact_and_conformed_dimension_modeling_in_spark.py) | 🟡 `Intermediate` | A retail enterprise transitions from a monolithic relational data warehouse to a Databricks Lakehouse. BI analys... |
| 073 | [`073_data_vault_2_0_in_the_lakehouse_hubs_links_and_satellites_modeling.py`](notebooks/07_lakehouse_modeling_scd/073_data_vault_2_0_in_the_lakehouse_hubs_links_and_satellites_modeling.py) | 🔴 `Advanced` | A large banking institution integrates 30 disparate core banking, loan, and credit systems. Source systems chang... |

### Module 08: Performance Tuning, Query Optimization & Skew Mitigation (074 - 085)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 074 | [`074_catalyst_optimizer_deep_dive_parsed_analyzed_optimized_plans_and_code_generation.py`](notebooks/08_performance_and_tuning/074_catalyst_optimizer_deep_dive_parsed_analyzed_optimized_plans_and_code_generation.py) | 🔴 `Advanced` | A lead data architect conducts code reviews for data pipelines. Junior engineers write complex nested SQL and Da... |
| 075 | [`075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins.py`](notebooks/08_performance_and_tuning/075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins.py) | 🔴 `Advanced` | A 2TB batch ETL pipeline runs with `spark.sql.shuffle.partitions = 200`. During filtering stages, data volume dr... |
| 076 | [`076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins.py`](notebooks/08_performance_and_tuning/076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins.py) | 🔴 `Advanced` | A national e-commerce platform joins website clicks with user accounts. Out of 100 million clicks, 40% belong to... |
| 077 | [`077_partitioning_strategies_avoiding_over_partitioning_and_directory_pruning.py`](notebooks/08_performance_and_tuning/077_partitioning_strategies_avoiding_over_partitioning_and_directory_pruning.py) | 🟡 `Intermediate` | A telemetry pipeline partitioned data by `year/month/day/hour/device_id`. After 6 months, the table contained 2 ... |
| 078 | [`078_bucketing_for_pre_sorted_join_optimization_and_shuffle_elimination.py`](notebooks/08_performance_and_tuning/078_bucketing_for_pre_sorted_join_optimization_and_shuffle_elimination.py) | 🔴 `Advanced` | Two multi-billion-row fact tables (Orders and Shipments) are joined hourly in a critical financial pipeline. Bec... |
| 079 | [`079_caching_and_persistence_storagelevel_memory_and_disk_vs_cache_traps.py`](notebooks/08_performance_and_tuning/079_caching_and_persistence_storagelevel_memory_and_disk_vs_cache_traps.py) | 🟡 `Intermediate` | A machine learning feature engineering notebook calls `df.cache()` on a 50GB dataset. Later in the pipeline, ite... |
| 080 | [`080_repartition_vs_coalesce_internals_full_shuffle_vs_partition_minimization.py`](notebooks/08_performance_and_tuning/080_repartition_vs_coalesce_internals_full_shuffle_vs_partition_minimization.py) | 🟡 `Intermediate` | A data pipeline writes daily reports to cloud storage. An engineer notices that writing a 5MB aggregated table p... |
| 081 | [`081_shuffle_partition_sizing_and_tuning_for_small_vs_terabyte_scale_jobs.py`](notebooks/08_performance_and_tuning/081_shuffle_partition_sizing_and_tuning_for_small_vs_terabyte_scale_jobs.py) | 🟡 `Intermediate` | A 100MB dataset query takes 45 seconds because Spark defaults to 200 shuffle partitions (`spark.sql.shuffle.part... |
| 082 | [`082_diagnosing_and_fixing_outofmemory_oom_errors_driver_vs_executor_heap.py`](notebooks/08_performance_and_tuning/082_diagnosing_and_fixing_outofmemory_oom_errors_driver_vs_executor_heap.py) | 🔴 `Advanced` | A production ETL job intermittently crashes with `java.lang.OutOfMemoryError: Java heap space`. On-call engineer... |
| 083 | [`083_spark_ui_eventlog_metrics_task_skew_and_shuffle_spill_analysis.py`](notebooks/08_performance_and_tuning/083_spark_ui_eventlog_metrics_task_skew_and_shuffle_spill_analysis.py) | 🔴 `Advanced` | A mission-critical nightly reconciliation job slows down by 3x on weekends. The senior engineer must inspect Spa... |
| 084 | [`084_vectorized_execution_and_photon_engine_acceleration_mechanics.py`](notebooks/08_performance_and_tuning/084_vectorized_execution_and_photon_engine_acceleration_mechanics.py) | 🔴 `Advanced` | An enterprise evaluates migrating high-throughput SQL analytics from classic Apache Spark clusters to Databricks... |
| 085 | [`085_fixing_accidental_cartesian_products_and_broadcastnestedloopjoins.py`](notebooks/08_performance_and_tuning/085_fixing_accidental_cartesian_products_and_broadcastnestedloopjoins.py) | 🔴 `Advanced` | A junior engineer submits a join query where the join condition accidentally uses an OR operator across two cond... |

### Module 09: Databricks Platform, Unity Catalog, Security & FinOps (086 - 095)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 086 | [`086_unity_catalog_three_tier_namespace_catalogs_schemas_and_managed_vs_external_tables.py`](notebooks/09_unity_catalog_and_security/086_unity_catalog_three_tier_namespace_catalogs_schemas_and_managed_vs_external_tables.py) | 🟡 `Intermediate` | An enterprise migrates from the legacy Hive Metastore (`hive_metastore.default`) to Databricks Unity Catalog. Pl... |
| 087 | [`087_unity_catalog_volumes_for_unstructured_and_semi_structured_data_governance.py`](notebooks/09_unity_catalog_and_security/087_unity_catalog_volumes_for_unstructured_and_semi_structured_data_governance.py) | 🟡 `Intermediate` | A machine learning team manages non-tabular files (PDF documents, medical images, audio files, and model weights... |
| 088 | [`088_dynamic_row_level_security_rls_using_row_filters_and_user_functions.py`](notebooks/09_unity_catalog_and_security/088_dynamic_row_level_security_rls_using_row_filters_and_user_functions.py) | 🔴 `Advanced` | A multinational sales organization maintains a single centralized `sales_pipeline` table. Regional sales reps mu... |
| 089 | [`089_dynamic_column_masking_for_pii_protection_and_compliance.py`](notebooks/09_unity_catalog_and_security/089_dynamic_column_masking_for_pii_protection_and_compliance.py) | 🟡 `Intermediate` | A medical insurance database stores member Social Security Numbers (SSN), full names, and email addresses. Custo... |
| 090 | [`090_pii_encryption_salted_hashing_and_aes_decryption_aes_encrypt_sha2.py`](notebooks/09_unity_catalog_and_security/090_pii_encryption_salted_hashing_and_aes_decryption_aes_encrypt_sha2.py) | 🔴 `Advanced` | A payment processing platform stores credit card Primary Account Numbers (PANs). PCI-DSS regulatory standards ma... |
| 091 | [`091_databricks_secret_scopes_and_secure_credential_injection_via_dbutils_secrets.py`](notebooks/09_unity_catalog_and_security/091_databricks_secret_scopes_and_secure_credential_injection_via_dbutils_secrets.py) | 🟡 `Intermediate` | A data integration pipeline connects to cloud databases, external APIs (Stripe, Salesforce), and message queues.... |
| 092 | [`092_finops_and_dbu_cost_calculation_using_system_billing_usage_system_tables.py`](notebooks/09_unity_catalog_and_security/092_finops_and_dbu_cost_calculation_using_system_billing_usage_system_tables.py) | 🔴 `Advanced` | A Director of Data Engineering needs to track monthly Databricks DBU (Databricks Unit) consumption across busine... |
| 093 | [`093_lakeflow_declarative_pipelines_and_delta_live_tables_dlt_expectations.py`](notebooks/09_unity_catalog_and_security/093_lakeflow_declarative_pipelines_and_delta_live_tables_dlt_expectations.py) | 🔴 `Advanced` | An enterprise financial lakehouse migrates manual notebook pipelines to Databricks Lakeflow Declarative Pipeline... |
| 094 | [`094_databricks_asset_bundles_dabs_for_multi_environment_ci_cd_automation.py`](notebooks/09_unity_catalog_and_security/094_databricks_asset_bundles_dabs_for_multi_environment_ci_cd_automation.py) | 🔴 `Advanced` | An enterprise development team deploys 50 Databricks workflows and pipelines across development, staging, and pr... |
| 095 | [`095_semantic_layer_modeling_and_databricks_metric_views_for_bi_and_ai_agents.py`](notebooks/09_unity_catalog_and_security/095_semantic_layer_modeling_and_databricks_metric_views_for_bi_and_ai_agents.py) | 🔴 `Advanced` | Different business teams calculate 'Customer Churn' and 'Net Revenue' using conflicting SQL formulas, producing ... |

### Module 10: End-to-End Enterprise Projects & Production Workflows (096 - 105)

| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |
|---|---|---|---|
| 096 | [`096_project_finance_real_time_credit_card_fraud_detection_and_customer_360.py`](notebooks/10_end_to_end_projects/096_project_finance_real_time_credit_card_fraud_detection_and_customer_360.py) | 🔴 `Advanced` | A tier-1 retail bank processes 50 million credit card swipe transactions daily. Risk engineering must build an e... |
| 097 | [`097_project_retail_omnichannel_multi_source_lakehouse_etl.py`](notebooks/10_end_to_end_projects/097_project_retail_omnichannel_multi_source_lakehouse_etl.py) | 🔴 `Advanced` | A major omnichannel retailer operates physical retail stores, an e-commerce website, and customer support via Sa... |
| 098 | [`098_project_healthcare_hipaa_compliant_clinical_records_de_identification.py`](notebooks/10_end_to_end_projects/098_project_healthcare_hipaa_compliant_clinical_records_de_identification.py) | 🔴 `Advanced` | A hospital research network prepares electronic health records (EHR) for clinical cancer research. HIPAA Safe Ha... |
| 099 | [`099_project_telecom_customer_churn_prediction_feature_store_and_window_features.py`](notebooks/10_end_to_end_projects/099_project_telecom_customer_churn_prediction_feature_store_and_window_features.py) | 🔴 `Advanced` | A major mobile carrier experiences 2% monthly customer churn. Data scientists need a centralized Lakehouse Featu... |
| 100 | [`100_project_cybersecurity_web_server_access_log_analytics_and_threat_detection.py`](notebooks/10_end_to_end_projects/100_project_cybersecurity_web_server_access_log_analytics_and_threat_detection.py) | 🔴 `Advanced` | A cyber defense operations center monitors 100 million web server access logs daily. Security analysts must pars... |
| 101 | [`101_project_iot_industrial_telemetry_anomaly_detection_via_z_score_statistics.py`](notebooks/10_end_to_end_projects/101_project_iot_industrial_telemetry_anomaly_detection_via_z_score_statistics.py) | 🔴 `Advanced` | A smart manufacturing plant operates 5,000 robotic welding arms. Bearings experience mechanical fatigue before c... |
| 102 | [`102_project_supply_chain_inventory_replenishment_forecasting_and_reorder_triggers.py`](notebooks/10_end_to_end_projects/102_project_supply_chain_inventory_replenishment_forecasting_and_reorder_triggers.py) | 🔴 `Advanced` | A national grocery chain manages 10,000 SKUs across 20 regional distribution centers. Supply chain disruptions c... |
| 103 | [`103_project_e_commerce_collaborative_filtering_recommendation_engine_with_spark_mllib.py`](notebooks/10_end_to_end_projects/103_project_e_commerce_collaborative_filtering_recommendation_engine_with_spark_mllib.py) | 🔴 `Advanced` | An online retail store has 1 million products and 5 million shoppers. Product managers want personalized 'Recomm... |
| 104 | [`104_project_agentic_ai_and_rag_lakehouse_document_chunking_and_vector_search.py`](notebooks/10_end_to_end_projects/104_project_agentic_ai_and_rag_lakehouse_document_chunking_and_vector_search.py) | 🔴 `Advanced` | An enterprise customer service platform deploys an Agentic AI chatbot powered by Databricks Genie and LLMs. The ... |
| 105 | [`105_master_enterprise_production_orchestration_workflow_parameters_and_alerting.py`](notebooks/10_end_to_end_projects/105_master_enterprise_production_orchestration_workflow_parameters_and_alerting.py) | 🔴 `Advanced` | A mission-critical enterprise financial pipeline executes 15 dependent tasks every night. The workflow must acce... |

---

## 🧪 Testing & Validation

All 105 notebooks are verified by automated tests:
```bash
# Validate Databricks source structure and Python AST syntax
python scripts/validate_notebooks.py

# Run test suite
pytest tests/
```

---

## 🤝 License
This repository is open-sourced under the [MIT License](LICENSE).
