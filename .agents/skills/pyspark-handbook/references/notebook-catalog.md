# PySpark Handbook Notebook Catalog

All 105 numbered source notebooks from the pinned PySpark Handbook snapshot are included in this skill. Select the closest scenario, then read its local source file before adapting it.

Pinned upstream commit: `bb56d371a6c1c14b92e3ae0336e15b7784f75107`.

## Module 01: Core PySpark Foundations & DataFrame API (12)

- [001 — SparkSession Initialization & DataFrame Creation Strategies](../notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.py) — Beginner
- [002 — Schema Enforcement, Safe Type Casting & Try-Cast Handling](../notebooks/01_core_dataframe_api/002_schema_enforcement_safe_type_casting_and_try_cast_handling.py) — Beginner
- [003 — Column Manipulation, Batch Addition & Projection Pruning](../notebooks/01_core_dataframe_api/003_column_manipulation_batch_addition_and_projection_pruning.py) — Beginner
- [004 — Filtering, Multi-Condition Predicates & Pushdown Analysis](../notebooks/01_core_dataframe_api/004_filtering_multi_condition_predicates_and_pushdown_analysis.py) — Beginner
- [005 — String Operations, Regex Extraction & Data Cleansing](../notebooks/01_core_dataframe_api/005_string_operations_regex_extraction_and_data_cleansing.py) — Beginner
- [006 — Date & Timestamp Mastery: Parsing, Arithmetic & Timezones](../notebooks/01_core_dataframe_api/006_date_and_timestamp_mastery_parsing_arithmetic_and_timezones.py) — Beginner
- [007 — Handling Nulls, NaNs & Imputation Strategies](../notebooks/01_core_dataframe_api/007_handling_nulls_nans_and_imputation_strategies.py) — Beginner
- [008 — Sorting, Ordering Semantics & Stratified Sampling](../notebooks/01_core_dataframe_api/008_sorting_ordering_semantics_and_stratified_sampling.py) — Beginner
- [009 — Deduplication Strategies: dropDuplicates vs Window Row-Number](../notebooks/01_core_dataframe_api/009_deduplication_strategies_dropduplicates_vs_window_row_number.py) — Intermediate
- [010 — Transformations vs Actions, Lazy Evaluation & Lineage Graph](../notebooks/01_core_dataframe_api/010_transformations_vs_actions_lazy_evaluation_and_lineage_graph.py) — Intermediate
- [011 — Custom PySpark UDFs vs Vectorized Pandas UDFs (PyArrow)](../notebooks/01_core_dataframe_api/011_custom_pyspark_udfs_vs_vectorized_pandas_udfs_pyarrow.py) — Intermediate
- [012 — Emulating Databricks DBUtils, Widgets & Secret Scopes Locally](../notebooks/01_core_dataframe_api/012_emulating_databricks_dbutils_widgets_and_secret_scopes_locally.py) — Intermediate

## Module 02: Complex Data Types & Nested Structures Handling (10)

- [013 — Array Creation, Element Extraction and Array Search Functions](../notebooks/02_complex_data_types/013_array_creation_element_extraction_and_array_search_functions.py) — Intermediate
- [014 — Spark Higher-Order Functions: Transform, Filter and Aggregate](../notebooks/02_complex_data_types/014_spark_higher_order_functions_transform_filter_and_aggregate.py) — Intermediate
- [015 — Exploding and Flattening Arrays: explode vs explode_outer and posexplode](../notebooks/02_complex_data_types/015_exploding_and_flattening_arrays_explode_vs_explode_outer_and_posexplode.py) — Intermediate
- [016 — Struct Creation, Nested Field Access and Updating Sub-Fields](../notebooks/02_complex_data_types/016_struct_creation_nested_field_access_and_updating_sub_fields.py) — Intermediate
- [017 — Recursive Schema Traversal: Flattening Deeply Nested JSON/Structs](../notebooks/02_complex_data_types/017_recursive_schema_traversal_flattening_deeply_nested_json_structs.py) — Advanced
- [018 — MapType Creation, Key Extraction and Dynamic Properties](../notebooks/02_complex_data_types/018_maptype_creation_key_extraction_and_dynamic_properties.py) — Intermediate
- [019 — Parsing Raw JSON Payloads: from_json, to_json and schema_of_json](../notebooks/02_complex_data_types/019_parsing_raw_json_payloads_from_json_to_json_and_schema_of_json.py) — Intermediate
- [020 — Handling Corrupted & Malformed JSON: Permissive vs DropMalformed vs FailFast](../notebooks/02_complex_data_types/020_handling_corrupted_and_malformed_json_permissive_vs_dropmalformed_vs_failfast.py) — Intermediate
- [021 — XML Data Ingestion, Hierarchical Parsing and Tag Structuring](../notebooks/02_complex_data_types/021_xml_data_ingestion_hierarchical_parsing_and_tag_structuring.py) — Intermediate
- [022 — Advanced Aggregations: collect_list vs collect_set and Sorted Collections](../notebooks/02_complex_data_types/022_advanced_aggregations_collect_list_vs_collect_set_and_sorted_collections.py) — Intermediate

## Module 03: Advanced SQL, Aggregations & Analytical Window Functions (10)

- [023 — Basic and Multi-Column Aggregations with Alias Management](../notebooks/03_sql_and_window_functions/023_basic_and_multi_column_aggregations_with_alias_management.py) — Beginner
- [024 — Advanced Multidimensional OLAP: Rollup, Cube and Grouping Sets](../notebooks/03_sql_and_window_functions/024_advanced_multidimensional_olap_rollup_cube_and_grouping_sets.py) — Intermediate
- [025 — Pivoting and Unpivoting Data: pivot and stack Expression Mastery](../notebooks/03_sql_and_window_functions/025_pivoting_and_unpivoting_data_pivot_and_stack_expression_mastery.py) — Intermediate
- [026 — Ranking Window Functions: row_number, rank, dense_rank and percent_rank](../notebooks/03_sql_and_window_functions/026_ranking_window_functions_row_number_rank_dense_rank_and_percent_rank.py) — Intermediate
- [027 — Aggregate & Cumulative Window Functions: Running Totals and Moving Averages](../notebooks/03_sql_and_window_functions/027_aggregate_and_cumulative_window_functions_running_totals_and_moving_averages.py) — Intermediate
- [028 — Offset Window Functions: lead, lag and Period-over-Period Churn](../notebooks/03_sql_and_window_functions/028_offset_window_functions_lead_lag_and_period_over_period_churn.py) — Intermediate
- [029 — Sessionization and the Gaps & Islands Problem in PySpark](../notebooks/03_sql_and_window_functions/029_sessionization_and_the_gaps_and_islands_problem_in_pyspark.py) — Advanced
- [030 — Relational Joins Masterclass: Inner, Outer, Semi and Anti Joins](../notebooks/03_sql_and_window_functions/030_relational_joins_masterclass_inner_outer_semi_and_anti_joins.py) — Intermediate
- [031 — Broadcast Hash Joins, Threshold Tuning and Memory Limits](../notebooks/03_sql_and_window_functions/031_broadcast_hash_joins_threshold_tuning_and_memory_limits.py) — Intermediate
- [032 — Spark SQL Views, Temp Tables and Catalyst SQL Optimization](../notebooks/03_sql_and_window_functions/032_spark_sql_views_temp_tables_and_catalyst_sql_optimization.py) — Beginner

## Module 04: Delta Lake Architecture, ACID Operations & Time Travel (11)

- [033 — Delta Table Creation, Metadata Inspection and Transaction Log Internals](../notebooks/04_delta_lake_and_acid/033_delta_table_creation_metadata_inspection_and_transaction_log_internals.py) — Intermediate
- [034 — ACID CRUD Operations on Delta: Insert, Update and Delete Mechanics](../notebooks/04_delta_lake_and_acid/034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics.py) — Intermediate
- [035 — Delta Upsert and MERGE INTO Mastery with Multiple Conditions](../notebooks/04_delta_lake_and_acid/035_delta_upsert_and_merge_into_mastery_with_multiple_conditions.py) — Intermediate
- [036 — Delta Time Travel, Audit History and Version Rollback](../notebooks/04_delta_lake_and_acid/036_delta_time_travel_audit_history_and_version_rollback.py) — Intermediate
- [037 — VACUUM Command, Retention Hours and Safe Tombstone Management](../notebooks/04_delta_lake_and_acid/037_vacuum_command_retention_hours_and_safe_tombstone_management.py) — Intermediate
- [038 — Delta OPTIMIZE, File Compaction and Auto-Compaction Strategies](../notebooks/04_delta_lake_and_acid/038_delta_optimize_file_compaction_and_auto_compaction_strategies.py) — Intermediate
- [039 — Z-Order Multi-Dimensional Indexing vs Liquid Clustering (CLUSTER BY)](../notebooks/04_delta_lake_and_acid/039_z_order_multi_dimensional_indexing_vs_liquid_clustering_cluster_by.py) — Advanced
- [040 — Delta Change Data Feed (CDF): Enabling and Consuming Row-Level Changes](../notebooks/04_delta_lake_and_acid/040_delta_change_data_feed_cdf_enabling_and_consuming_row_level_changes.py) — Advanced
- [041 — Delta Schema Evolution (mergeSchema) and Schema Enforcement](../notebooks/04_delta_lake_and_acid/041_delta_schema_evolution_mergeschema_and_schema_enforcement.py) — Intermediate
- [042 — Delta Table Constraints: CHECK and NOT NULL Data Quality Rules](../notebooks/04_delta_lake_and_acid/042_delta_table_constraints_check_and_not_null_data_quality_rules.py) — Intermediate
- [043 — In-Place Parquet to Delta Lake Migration with CONVERT TO DELTA](../notebooks/04_delta_lake_and_acid/043_in_place_parquet_to_delta_lake_migration_with_convert_to_delta.py) — Intermediate

## Module 05: Data Ingestion Patterns, Auto Loader & File Formats (10)

- [044 — CSV Ingestion Edge Cases: Multiline, Escaped Quotes and Custom Delimiters](../notebooks/05_data_ingestion_autoloader/044_csv_ingestion_edge_cases_multiline_escaped_quotes_and_custom_delimiters.py) — Intermediate
- [045 — Parquet and ORC Columnar Ingestion: Predicate Pushdown and Dictionary Encoding](../notebooks/05_data_ingestion_autoloader/045_parquet_and_orc_columnar_ingestion_predicate_pushdown_and_dictionary_encoding.py) — Intermediate
- [046 — Avro Binary Ingestion and Schema Registry Compatibility](../notebooks/05_data_ingestion_autoloader/046_avro_binary_ingestion_and_schema_registry_compatibility.py) — Intermediate
- [047 — JDBC Parallel Ingestion: Partitioning by Column, fetchsize and Connection Sizing](../notebooks/05_data_ingestion_autoloader/047_jdbc_parallel_ingestion_partitioning_by_column_fetchsize_and_connection_sizing.py) — Intermediate
- [048 — Databricks Auto Loader Directory Mode: cloudFiles, Schema Inference and Checkpointing](../notebooks/05_data_ingestion_autoloader/048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.py) — Intermediate
- [049 — Auto Loader Schema Evolution and Rescued Data Column (_rescued_data)](../notebooks/05_data_ingestion_autoloader/049_auto_loader_schema_evolution_and_rescued_data_column_rescued_data.py) — Intermediate
- [050 — Building a Metadata-Driven Multi-Format Ingestion Engine](../notebooks/05_data_ingestion_autoloader/050_building_a_metadata_driven_multi_format_ingestion_engine.py) — Advanced
- [051 — Incremental Batch File Discovery: Watermarking and Modification Timestamps](../notebooks/05_data_ingestion_autoloader/051_incremental_batch_file_discovery_watermarking_and_modification_timestamps.py) — Intermediate
- [052 — Handling Compressed Archives (.zip, .tar.gz, .bz2) in Distributed PySpark](../notebooks/05_data_ingestion_autoloader/052_handling_compressed_archives_zip_tar_gz_bz2_in_distributed_pyspark.py) — Advanced
- [053 — Distributed REST API and Webhook Ingestion without Driver OOM](../notebooks/05_data_ingestion_autoloader/053_distributed_rest_api_and_webhook_ingestion_without_driver_oom.py) — Advanced

## Module 06: Streaming & Near Real-Time Processing (10)

- [054 — Structured Streaming Foundations: Sources, Execution Model and Query Progress](../notebooks/06_streaming_realtime/054_structured_streaming_foundations_sources_execution_model_and_query_progress.py) — Intermediate
- [055 — Streaming Triggers: ProcessingTime, Once and AvailableNow for Cost Optimization](../notebooks/06_streaming_realtime/055_streaming_triggers_processingtime_once_and_availablenow_for_cost_optimization.py) — Intermediate
- [056 — Streaming Window Aggregations and Watermarking for Late-Arriving Events](../notebooks/06_streaming_realtime/056_streaming_window_aggregations_and_watermarking_for_late_arriving_events.py) — Advanced
- [057 — Stream-Static Joins with Slowly Changing Dimension Lookups](../notebooks/06_streaming_realtime/057_stream_static_joins_with_slowly_changing_dimension_lookups.py) — Intermediate
- [058 — Stream-Stream Dual Watermark Joins with Interval Conditions](../notebooks/06_streaming_realtime/058_stream_stream_dual_watermark_joins_with_interval_conditions.py) — Advanced
- [059 — Streaming Deduplication Across Time Windows with Watermarks](../notebooks/06_streaming_realtime/059_streaming_deduplication_across_time_windows_with_watermarks.py) — Intermediate
- [060 — Idempotent ACID Delta Upserts using writeStream.foreachBatch](../notebooks/06_streaming_realtime/060_idempotent_acid_delta_upserts_using_writestream_foreachbatch.py) — Advanced
- [061 — Arbitrary Stateful Streaming Operations with FlatMapGroupsWithState Concepts](../notebooks/06_streaming_realtime/061_arbitrary_stateful_streaming_operations_with_flatmapgroupswithstate_concepts.py) — Advanced
- [062 — Dead-Letter Queue (DLQ) Quarantine Streaming Pattern in PySpark](../notebooks/06_streaming_realtime/062_dead_letter_queue_dlq_quarantine_streaming_pattern_in_pyspark.py) — Intermediate
- [063 — Streaming Observability: StreamingQueryListener and SLA Latency Alerting](../notebooks/06_streaming_realtime/063_streaming_observability_streamingquerylistener_and_sla_latency_alerting.py) — Advanced

## Module 07: Lakehouse Data Modeling, CDC & SCD Implementation (10)

- [064 — Medallion Architecture: Bronze Ingestion Layer Standards and Audit Trails](../notebooks/07_lakehouse_modeling_scd/064_medallion_architecture_bronze_ingestion_layer_standards_and_audit_trails.py) — Intermediate
- [065 — Medallion Architecture: Silver Transformation, Cleansing and Conformance](../notebooks/07_lakehouse_modeling_scd/065_medallion_architecture_silver_transformation_cleansing_and_conformance.py) — Intermediate
- [066 — Medallion Architecture: Gold Business Aggregations and Dimensional Serving](../notebooks/07_lakehouse_modeling_scd/066_medallion_architecture_gold_business_aggregations_and_dimensional_serving.py) — Intermediate
- [067 — Distributed Surrogate Key Generation: monotonically_increasing_id vs xxhash64 vs UUIDs](../notebooks/07_lakehouse_modeling_scd/067_distributed_surrogate_key_generation_monotonically_increasing_id_vs_xxhash64_vs_uuids.py) — Intermediate
- [068 — Slowly Changing Dimension Type 1 (SCD Type 1) In-Place Overwrites via Delta Merge](../notebooks/07_lakehouse_modeling_scd/068_slowly_changing_dimension_type_1_scd_type_1_in_place_overwrites_via_delta_merge.py) — Intermediate
- [069 — Slowly Changing Dimension Type 2 (SCD Type 2) Historical Tracking with Effective Dates](../notebooks/07_lakehouse_modeling_scd/069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.py) — Advanced
- [070 — Slowly Changing Dimension Type 3 and Type 4 Multi-Tier Implementation](../notebooks/07_lakehouse_modeling_scd/070_slowly_changing_dimension_type_3_and_type_4_multi_tier_implementation.py) — Intermediate
- [071 — Change Data Capture (CDC) Ingestion from Debezium/Kafka into Lakehouse](../notebooks/07_lakehouse_modeling_scd/071_change_data_capture_cdc_ingestion_from_debezium_kafka_into_lakehouse.py) — Advanced
- [072 — Star Schema Implementation: Fact and Conformed Dimension Modeling in Spark](../notebooks/07_lakehouse_modeling_scd/072_star_schema_implementation_fact_and_conformed_dimension_modeling_in_spark.py) — Intermediate
- [073 — Data Vault 2.0 in the Lakehouse: Hubs, Links and Satellites Modeling](../notebooks/07_lakehouse_modeling_scd/073_data_vault_2_0_in_the_lakehouse_hubs_links_and_satellites_modeling.py) — Advanced

## Module 08: Performance Tuning, Query Optimization & Skew Mitigation (12)

- [074 — Catalyst Optimizer Deep Dive: Parsed, Analyzed, Optimized Plans and Code Generation](../notebooks/08_performance_and_tuning/074_catalyst_optimizer_deep_dive_parsed_analyzed_optimized_plans_and_code_generation.py) — Advanced
- [075 — Adaptive Query Execution (AQE): Dynamic Coalescing, Join Switching and Skew Joins](../notebooks/08_performance_and_tuning/075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins.py) — Advanced
- [076 — Data Skew Detection and Mitigation Using Salting and Two-Stage Joins](../notebooks/08_performance_and_tuning/076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins.py) — Advanced
- [077 — Partitioning Strategies: Avoiding Over-Partitioning and Directory Pruning](../notebooks/08_performance_and_tuning/077_partitioning_strategies_avoiding_over_partitioning_and_directory_pruning.py) — Intermediate
- [078 — Bucketing for Pre-Sorted Join Optimization and Shuffle Elimination](../notebooks/08_performance_and_tuning/078_bucketing_for_pre_sorted_join_optimization_and_shuffle_elimination.py) — Advanced
- [079 — Caching and Persistence: StorageLevel (MEMORY_AND_DISK) vs Cache Traps](../notebooks/08_performance_and_tuning/079_caching_and_persistence_storagelevel_memory_and_disk_vs_cache_traps.py) — Intermediate
- [080 — Repartition vs Coalesce Internals: Full Shuffle vs Partition Minimization](../notebooks/08_performance_and_tuning/080_repartition_vs_coalesce_internals_full_shuffle_vs_partition_minimization.py) — Intermediate
- [081 — Shuffle Partition Sizing and Tuning for Small vs Terabyte-Scale Jobs](../notebooks/08_performance_and_tuning/081_shuffle_partition_sizing_and_tuning_for_small_vs_terabyte_scale_jobs.py) — Intermediate
- [082 — Diagnosing and Fixing OutOfMemory (OOM) Errors: Driver vs Executor Heap](../notebooks/08_performance_and_tuning/082_diagnosing_and_fixing_outofmemory_oom_errors_driver_vs_executor_heap.py) — Advanced
- [083 — Spark UI, EventLog Metrics, Task Skew and Shuffle Spill Analysis](../notebooks/08_performance_and_tuning/083_spark_ui_eventlog_metrics_task_skew_and_shuffle_spill_analysis.py) — Advanced
- [084 — Vectorized Execution and Photon Engine Acceleration Mechanics](../notebooks/08_performance_and_tuning/084_vectorized_execution_and_photon_engine_acceleration_mechanics.py) — Advanced
- [085 — Fixing Accidental Cartesian Products and BroadcastNestedLoopJoins](../notebooks/08_performance_and_tuning/085_fixing_accidental_cartesian_products_and_broadcastnestedloopjoins.py) — Advanced

## Module 09: Databricks Platform, Unity Catalog, Security & FinOps (10)

- [086 — Unity Catalog Three-Tier Namespace: Catalogs, Schemas and Managed vs External Tables](../notebooks/09_unity_catalog_and_security/086_unity_catalog_three_tier_namespace_catalogs_schemas_and_managed_vs_external_tables.py) — Intermediate
- [087 — Unity Catalog Volumes for Unstructured and Semi-Structured Data Governance](../notebooks/09_unity_catalog_and_security/087_unity_catalog_volumes_for_unstructured_and_semi_structured_data_governance.py) — Intermediate
- [088 — Dynamic Row-Level Security (RLS) Using Row Filters and User Functions](../notebooks/09_unity_catalog_and_security/088_dynamic_row_level_security_rls_using_row_filters_and_user_functions.py) — Advanced
- [089 — Dynamic Column Masking for PII Protection and Compliance](../notebooks/09_unity_catalog_and_security/089_dynamic_column_masking_for_pii_protection_and_compliance.py) — Intermediate
- [090 — PII Encryption, Salted Hashing and AES Decryption (aes_encrypt, sha2)](../notebooks/09_unity_catalog_and_security/090_pii_encryption_salted_hashing_and_aes_decryption_aes_encrypt_sha2.py) — Advanced
- [091 — Databricks Secret Scopes and Secure Credential Injection via dbutils.secrets](../notebooks/09_unity_catalog_and_security/091_databricks_secret_scopes_and_secure_credential_injection_via_dbutils_secrets.py) — Intermediate
- [092 — FinOps and DBU Cost Calculation Using system.billing.usage System Tables](../notebooks/09_unity_catalog_and_security/092_finops_and_dbu_cost_calculation_using_system_billing_usage_system_tables.py) — Advanced
- [093 — Lakeflow Declarative Pipelines and Delta Live Tables (DLT) Expectations](../notebooks/09_unity_catalog_and_security/093_lakeflow_declarative_pipelines_and_delta_live_tables_dlt_expectations.py) — Advanced
- [094 — Declarative Automation Bundles (DABs / Asset Bundles) for Multi-Environment CI/CD](../notebooks/09_unity_catalog_and_security/094_databricks_asset_bundles_dabs_for_multi_environment_ci_cd_automation.py) — Advanced
- [095 — Semantic Layer Modeling and Databricks Metric Views for BI and AI Agents](../notebooks/09_unity_catalog_and_security/095_semantic_layer_modeling_and_databricks_metric_views_for_bi_and_ai_agents.py) — Advanced

## Module 10: End-to-End Enterprise Projects & Production Workflows (10)

- [096 — Project Finance: Real-Time Credit Card Fraud Detection and Customer 360](../notebooks/10_end_to_end_projects/096_project_finance_real_time_credit_card_fraud_detection_and_customer_360.py) — Advanced
- [097 — Project Retail: Omnichannel Multi-Source Lakehouse ETL](../notebooks/10_end_to_end_projects/097_project_retail_omnichannel_multi_source_lakehouse_etl.py) — Advanced
- [098 — Project Healthcare: HIPAA-Compliant Clinical Records De-Identification](../notebooks/10_end_to_end_projects/098_project_healthcare_hipaa_compliant_clinical_records_de_identification.py) — Advanced
- [099 — Project Telecom: Customer Churn Prediction Feature Store and Window Features](../notebooks/10_end_to_end_projects/099_project_telecom_customer_churn_prediction_feature_store_and_window_features.py) — Advanced
- [100 — Project Cybersecurity: Web Server Access Log Analytics and Threat Detection](../notebooks/10_end_to_end_projects/100_project_cybersecurity_web_server_access_log_analytics_and_threat_detection.py) — Advanced
- [101 — Project IoT: Industrial Telemetry Anomaly Detection via Z-Score Statistics](../notebooks/10_end_to_end_projects/101_project_iot_industrial_telemetry_anomaly_detection_via_z_score_statistics.py) — Advanced
- [102 — Project Supply Chain: Inventory Replenishment Forecasting and Reorder Triggers](../notebooks/10_end_to_end_projects/102_project_supply_chain_inventory_replenishment_forecasting_and_reorder_triggers.py) — Advanced
- [103 — Project E-Commerce: Collaborative Filtering Recommendation Engine with Spark MLlib](../notebooks/10_end_to_end_projects/103_project_e_commerce_collaborative_filtering_recommendation_engine_with_spark_mllib.py) — Advanced
- [104 — Project Agentic AI & RAG: Lakehouse Document Chunking and Vector Search](../notebooks/10_end_to_end_projects/104_project_agentic_ai_and_rag_lakehouse_document_chunking_and_vector_search.py) — Advanced
- [105 — Master Enterprise Production Orchestration, Workflow Parameters and Alerting](../notebooks/10_end_to_end_projects/105_master_enterprise_production_orchestration_workflow_parameters_and_alerting.py) — Advanced
