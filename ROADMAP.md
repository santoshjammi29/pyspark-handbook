# 🗺️ PySpark Handbook — Interactive Curriculum Roadmap

> **Interactive Checkbox Study Tracker & Mastery Progression**  
> **Canonical Repository**: [santoshjammi29/pyspark-handbook](https://github.com/santoshjammi29/pyspark-handbook) | **Ecosystem**: [moderndataengineer.uk](https://moderndataengineer.uk)  
> **Total Exercises & Drills**: `116` | **Primary Pillar**: `Apache Spark & Distributed Compute`

---

## How to Use This Roadmap

1. **Fork this repository** to your personal GitHub account.
2. Work through the modules in sequence or target specific architectural topics.
3. Check off boxes (`- [x]`) as you complete each exercise, drill, or notebook.
4. Commit and push your progress to your personal branch to track your career journey!

```
Progress: [ ] 0 / 116 Completed (0.0%)
```

---

## Curriculum Structure: The 4 Mastery Tiers

```mermaid
flowchart LR
    T1["Tier 1: Foundations<br/>(Level 100)"] --> T2["Tier 2: Core Mechanics<br/>(Level 200)"]
    T2 --> T3["Tier 3: Complex Scenarios<br/>(Level 300)"]
    T3 --> T4["Tier 4: Staff Architect<br/>(Level 400)"]
```

### 🟡 Tier 2: Core Mechanics & Implementation (Level 200)
> Production pipeline patterns, schema validation, standard transformations, and unit tests.

- [ ] `pyspark-handbook-curriculum-topics` — [Topics Directory: PySpark Handbook](topics/README.md) `Level 200`

### 🟠 Tier 3: Complex Scenarios, Edge Cases & Performance (Level 300)
> Performance tuning, partition skew, memory triage runbooks, and failure recovery.

- [ ] `pyspark-handbook-001_sparksession_initialization_and_dataframe_creation_strategies` — [Sparksession Initialization And Dataframe Creation Strategies](notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.py) `Level 300`
- [ ] `pyspark-handbook-002_schema_enforcement_safe_type_casting_and_try_cast_handling` — [Schema Enforcement Safe Type Casting And Try Cast Handling](notebooks/01_core_dataframe_api/002_schema_enforcement_safe_type_casting_and_try_cast_handling.py) `Level 300`
- [ ] `pyspark-handbook-003_column_manipulation_batch_addition_and_projection_pruning` — [Column Manipulation Batch Addition And Projection Pruning](notebooks/01_core_dataframe_api/003_column_manipulation_batch_addition_and_projection_pruning.py) `Level 300`
- [ ] `pyspark-handbook-004_filtering_multi_condition_predicates_and_pushdown_analysis` — [Filtering Multi Condition Predicates And Pushdown Analysis](notebooks/01_core_dataframe_api/004_filtering_multi_condition_predicates_and_pushdown_analysis.py) `Level 300`
- [ ] `pyspark-handbook-005_string_operations_regex_extraction_and_data_cleansing` — [String Operations Regex Extraction And Data Cleansing](notebooks/01_core_dataframe_api/005_string_operations_regex_extraction_and_data_cleansing.py) `Level 300`
- [ ] `pyspark-handbook-006_date_and_timestamp_mastery_parsing_arithmetic_and_timezones` — [Date And Timestamp Mastery Parsing Arithmetic And Timezones](notebooks/01_core_dataframe_api/006_date_and_timestamp_mastery_parsing_arithmetic_and_timezones.py) `Level 300`
- [ ] `pyspark-handbook-007_handling_nulls_nans_and_imputation_strategies` — [Handling Nulls Nans And Imputation Strategies](notebooks/01_core_dataframe_api/007_handling_nulls_nans_and_imputation_strategies.py) `Level 300`
- [ ] `pyspark-handbook-008_sorting_ordering_semantics_and_stratified_sampling` — [Sorting Ordering Semantics And Stratified Sampling](notebooks/01_core_dataframe_api/008_sorting_ordering_semantics_and_stratified_sampling.py) `Level 300`
- [ ] `pyspark-handbook-009_deduplication_strategies_dropduplicates_vs_window_row_number` — [Deduplication Strategies Dropduplicates Vs Window Row Number](notebooks/01_core_dataframe_api/009_deduplication_strategies_dropduplicates_vs_window_row_number.py) `Level 300`
- [ ] `pyspark-handbook-010_transformations_vs_actions_lazy_evaluation_and_lineage_graph` — [Transformations Vs Actions Lazy Evaluation And Lineage Graph](notebooks/01_core_dataframe_api/010_transformations_vs_actions_lazy_evaluation_and_lineage_graph.py) `Level 300`
- [ ] `pyspark-handbook-011_custom_pyspark_udfs_vs_vectorized_pandas_udfs_pyarrow` — [Custom Pyspark Udfs Vs Vectorized Pandas Udfs Pyarrow](notebooks/01_core_dataframe_api/011_custom_pyspark_udfs_vs_vectorized_pandas_udfs_pyarrow.py) `Level 300`
- [ ] `pyspark-handbook-012_emulating_databricks_dbutils_widgets_and_secret_scopes_locally` — [Emulating Databricks Dbutils Widgets And Secret Scopes Locally](notebooks/01_core_dataframe_api/012_emulating_databricks_dbutils_widgets_and_secret_scopes_locally.py) `Level 300`
- [ ] `pyspark-handbook-013_array_creation_element_extraction_and_array_search_functions` — [Array Creation Element Extraction And Array Search Functions](notebooks/02_complex_data_types/013_array_creation_element_extraction_and_array_search_functions.py) `Level 300`
- [ ] `pyspark-handbook-014_spark_higher_order_functions_transform_filter_and_aggregate` — [Spark Higher Order Functions Transform Filter And Aggregate](notebooks/02_complex_data_types/014_spark_higher_order_functions_transform_filter_and_aggregate.py) `Level 300`
- [ ] `pyspark-handbook-015_exploding_and_flattening_arrays_explode_vs_explode_outer_and_posexplode` — [Exploding And Flattening Arrays Explode Vs Explode Outer And Posexplode](notebooks/02_complex_data_types/015_exploding_and_flattening_arrays_explode_vs_explode_outer_and_posexplode.py) `Level 300`
- [ ] `pyspark-handbook-016_struct_creation_nested_field_access_and_updating_sub_fields` — [Struct Creation Nested Field Access And Updating Sub Fields](notebooks/02_complex_data_types/016_struct_creation_nested_field_access_and_updating_sub_fields.py) `Level 300`
- [ ] `pyspark-handbook-017_recursive_schema_traversal_flattening_deeply_nested_json_structs` — [Recursive Schema Traversal Flattening Deeply Nested Json Structs](notebooks/02_complex_data_types/017_recursive_schema_traversal_flattening_deeply_nested_json_structs.py) `Level 300`
- [ ] `pyspark-handbook-018_maptype_creation_key_extraction_and_dynamic_properties` — [Maptype Creation Key Extraction And Dynamic Properties](notebooks/02_complex_data_types/018_maptype_creation_key_extraction_and_dynamic_properties.py) `Level 300`
- [ ] `pyspark-handbook-019_parsing_raw_json_payloads_from_json_to_json_and_schema_of_json` — [Parsing Raw Json Payloads From Json To Json And Schema Of Json](notebooks/02_complex_data_types/019_parsing_raw_json_payloads_from_json_to_json_and_schema_of_json.py) `Level 300`
- [ ] `pyspark-handbook-020_handling_corrupted_and_malformed_json_permissive_vs_dropmalformed_vs_failfast` — [Handling Corrupted And Malformed Json Permissive Vs Dropmalformed Vs Failfast](notebooks/02_complex_data_types/020_handling_corrupted_and_malformed_json_permissive_vs_dropmalformed_vs_failfast.py) `Level 300`
- [ ] `pyspark-handbook-021_xml_data_ingestion_hierarchical_parsing_and_tag_structuring` — [Xml Data Ingestion Hierarchical Parsing And Tag Structuring](notebooks/02_complex_data_types/021_xml_data_ingestion_hierarchical_parsing_and_tag_structuring.py) `Level 300`
- [ ] `pyspark-handbook-022_advanced_aggregations_collect_list_vs_collect_set_and_sorted_collections` — [Advanced Aggregations Collect List Vs Collect Set And Sorted Collections](notebooks/02_complex_data_types/022_advanced_aggregations_collect_list_vs_collect_set_and_sorted_collections.py) `Level 300`
- [ ] `pyspark-handbook-023_basic_and_multi_column_aggregations_with_alias_management` — [Basic And Multi Column Aggregations With Alias Management](notebooks/03_sql_and_window_functions/023_basic_and_multi_column_aggregations_with_alias_management.py) `Level 300`
- [ ] `pyspark-handbook-024_advanced_multidimensional_olap_rollup_cube_and_grouping_sets` — [Advanced Multidimensional Olap Rollup Cube And Grouping Sets](notebooks/03_sql_and_window_functions/024_advanced_multidimensional_olap_rollup_cube_and_grouping_sets.py) `Level 300`
- [ ] `pyspark-handbook-025_pivoting_and_unpivoting_data_pivot_and_stack_expression_mastery` — [Pivoting And Unpivoting Data Pivot And Stack Expression Mastery](notebooks/03_sql_and_window_functions/025_pivoting_and_unpivoting_data_pivot_and_stack_expression_mastery.py) `Level 300`
- [ ] `pyspark-handbook-026_ranking_window_functions_row_number_rank_dense_rank_and_percent_rank` — [Ranking Window Functions Row Number Rank Dense Rank And Percent Rank](notebooks/03_sql_and_window_functions/026_ranking_window_functions_row_number_rank_dense_rank_and_percent_rank.py) `Level 300`
- [ ] `pyspark-handbook-027_aggregate_and_cumulative_window_functions_running_totals_and_moving_averages` — [Aggregate And Cumulative Window Functions Running Totals And Moving Averages](notebooks/03_sql_and_window_functions/027_aggregate_and_cumulative_window_functions_running_totals_and_moving_averages.py) `Level 300`
- [ ] `pyspark-handbook-028_offset_window_functions_lead_lag_and_period_over_period_churn` — [Offset Window Functions Lead Lag And Period Over Period Churn](notebooks/03_sql_and_window_functions/028_offset_window_functions_lead_lag_and_period_over_period_churn.py) `Level 300`
- [ ] `pyspark-handbook-029_sessionization_and_the_gaps_and_islands_problem_in_pyspark` — [Sessionization And The Gaps And Islands Problem In Pyspark](notebooks/03_sql_and_window_functions/029_sessionization_and_the_gaps_and_islands_problem_in_pyspark.py) `Level 300`
- [ ] `pyspark-handbook-030_relational_joins_masterclass_inner_outer_semi_and_anti_joins` — [Relational Joins Masterclass Inner Outer Semi And Anti Joins](notebooks/03_sql_and_window_functions/030_relational_joins_masterclass_inner_outer_semi_and_anti_joins.py) `Level 300`
- [ ] `pyspark-handbook-031_broadcast_hash_joins_threshold_tuning_and_memory_limits` — [Broadcast Hash Joins Threshold Tuning And Memory Limits](notebooks/03_sql_and_window_functions/031_broadcast_hash_joins_threshold_tuning_and_memory_limits.py) `Level 300`
- [ ] `pyspark-handbook-032_spark_sql_views_temp_tables_and_catalyst_sql_optimization` — [Spark Sql Views Temp Tables And Catalyst Sql Optimization](notebooks/03_sql_and_window_functions/032_spark_sql_views_temp_tables_and_catalyst_sql_optimization.py) `Level 300`
- [ ] `pyspark-handbook-033_delta_table_creation_metadata_inspection_and_transaction_log_internals` — [Delta Table Creation Metadata Inspection And Transaction Log Internals](notebooks/04_delta_lake_and_acid/033_delta_table_creation_metadata_inspection_and_transaction_log_internals.py) `Level 300`
- [ ] `pyspark-handbook-034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics` — [Acid Crud Operations On Delta Insert Update And Delete Mechanics](notebooks/04_delta_lake_and_acid/034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics.py) `Level 300`
- [ ] `pyspark-handbook-035_delta_upsert_and_merge_into_mastery_with_multiple_conditions` — [Delta Upsert And Merge Into Mastery With Multiple Conditions](notebooks/04_delta_lake_and_acid/035_delta_upsert_and_merge_into_mastery_with_multiple_conditions.py) `Level 300`
- [ ] `pyspark-handbook-036_delta_time_travel_audit_history_and_version_rollback` — [Delta Time Travel Audit History And Version Rollback](notebooks/04_delta_lake_and_acid/036_delta_time_travel_audit_history_and_version_rollback.py) `Level 300`
- [ ] `pyspark-handbook-037_vacuum_command_retention_hours_and_safe_tombstone_management` — [Vacuum Command Retention Hours And Safe Tombstone Management](notebooks/04_delta_lake_and_acid/037_vacuum_command_retention_hours_and_safe_tombstone_management.py) `Level 300`
- [ ] `pyspark-handbook-038_delta_optimize_file_compaction_and_auto_compaction_strategies` — [Delta Optimize File Compaction And Auto Compaction Strategies](notebooks/04_delta_lake_and_acid/038_delta_optimize_file_compaction_and_auto_compaction_strategies.py) `Level 300`
- [ ] `pyspark-handbook-039_z_order_multi_dimensional_indexing_vs_liquid_clustering_cluster_by` — [Z Order Multi Dimensional Indexing Vs Liquid Clustering Cluster By](notebooks/04_delta_lake_and_acid/039_z_order_multi_dimensional_indexing_vs_liquid_clustering_cluster_by.py) `Level 300`
- [ ] `pyspark-handbook-040_delta_change_data_feed_cdf_enabling_and_consuming_row_level_changes` — [Delta Change Data Feed Cdf Enabling And Consuming Row Level Changes](notebooks/04_delta_lake_and_acid/040_delta_change_data_feed_cdf_enabling_and_consuming_row_level_changes.py) `Level 300`
- [ ] `pyspark-handbook-041_delta_schema_evolution_mergeschema_and_schema_enforcement` — [Delta Schema Evolution Mergeschema And Schema Enforcement](notebooks/04_delta_lake_and_acid/041_delta_schema_evolution_mergeschema_and_schema_enforcement.py) `Level 300`
- [ ] `pyspark-handbook-042_delta_table_constraints_check_and_not_null_data_quality_rules` — [Delta Table Constraints Check And Not Null Data Quality Rules](notebooks/04_delta_lake_and_acid/042_delta_table_constraints_check_and_not_null_data_quality_rules.py) `Level 300`
- [ ] `pyspark-handbook-043_in_place_parquet_to_delta_lake_migration_with_convert_to_delta` — [In Place Parquet To Delta Lake Migration With Convert To Delta](notebooks/04_delta_lake_and_acid/043_in_place_parquet_to_delta_lake_migration_with_convert_to_delta.py) `Level 300`
- [ ] `pyspark-handbook-044_csv_ingestion_edge_cases_multiline_escaped_quotes_and_custom_delimiters` — [Csv Ingestion Edge Cases Multiline Escaped Quotes And Custom Delimiters](notebooks/05_data_ingestion_autoloader/044_csv_ingestion_edge_cases_multiline_escaped_quotes_and_custom_delimiters.py) `Level 300`
- [ ] `pyspark-handbook-045_parquet_and_orc_columnar_ingestion_predicate_pushdown_and_dictionary_encoding` — [Parquet And Orc Columnar Ingestion Predicate Pushdown And Dictionary Encoding](notebooks/05_data_ingestion_autoloader/045_parquet_and_orc_columnar_ingestion_predicate_pushdown_and_dictionary_encoding.py) `Level 300`
- [ ] `pyspark-handbook-046_avro_binary_ingestion_and_schema_registry_compatibility` — [Avro Binary Ingestion And Schema Registry Compatibility](notebooks/05_data_ingestion_autoloader/046_avro_binary_ingestion_and_schema_registry_compatibility.py) `Level 300`
- [ ] `pyspark-handbook-047_jdbc_parallel_ingestion_partitioning_by_column_fetchsize_and_connection_sizing` — [Jdbc Parallel Ingestion Partitioning By Column Fetchsize And Connection Sizing](notebooks/05_data_ingestion_autoloader/047_jdbc_parallel_ingestion_partitioning_by_column_fetchsize_and_connection_sizing.py) `Level 300`
- [ ] `pyspark-handbook-048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing` — [Databricks Auto Loader Directory Mode Cloudfiles Schema Inference And Checkpointing](notebooks/05_data_ingestion_autoloader/048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.py) `Level 300`
- [ ] `pyspark-handbook-049_auto_loader_schema_evolution_and_rescued_data_column_rescued_data` — [Auto Loader Schema Evolution And Rescued Data Column Rescued Data](notebooks/05_data_ingestion_autoloader/049_auto_loader_schema_evolution_and_rescued_data_column_rescued_data.py) `Level 300`
- [ ] `pyspark-handbook-050_building_a_metadata_driven_multi_format_ingestion_engine` — [Building A Metadata Driven Multi Format Ingestion Engine](notebooks/05_data_ingestion_autoloader/050_building_a_metadata_driven_multi_format_ingestion_engine.py) `Level 300`
- [ ] `pyspark-handbook-051_incremental_batch_file_discovery_watermarking_and_modification_timestamps` — [Incremental Batch File Discovery Watermarking And Modification Timestamps](notebooks/05_data_ingestion_autoloader/051_incremental_batch_file_discovery_watermarking_and_modification_timestamps.py) `Level 300`
- [ ] `pyspark-handbook-052_handling_compressed_archives_zip_tar_gz_bz2_in_distributed_pyspark` — [Handling Compressed Archives Zip Tar Gz Bz2 In Distributed Pyspark](notebooks/05_data_ingestion_autoloader/052_handling_compressed_archives_zip_tar_gz_bz2_in_distributed_pyspark.py) `Level 300`
- [ ] `pyspark-handbook-053_distributed_rest_api_and_webhook_ingestion_without_driver_oom` — [Distributed Rest Api And Webhook Ingestion Without Driver Oom](notebooks/05_data_ingestion_autoloader/053_distributed_rest_api_and_webhook_ingestion_without_driver_oom.py) `Level 300`
- [ ] `pyspark-handbook-054_structured_streaming_foundations_sources_execution_model_and_query_progress` — [Structured Streaming Foundations Sources Execution Model And Query Progress](notebooks/06_streaming_realtime/054_structured_streaming_foundations_sources_execution_model_and_query_progress.py) `Level 300`
- [ ] `pyspark-handbook-055_streaming_triggers_processingtime_once_and_availablenow_for_cost_optimization` — [Streaming Triggers Processingtime Once And Availablenow For Cost Optimization](notebooks/06_streaming_realtime/055_streaming_triggers_processingtime_once_and_availablenow_for_cost_optimization.py) `Level 300`
- [ ] `pyspark-handbook-056_streaming_window_aggregations_and_watermarking_for_late_arriving_events` — [Streaming Window Aggregations And Watermarking For Late Arriving Events](notebooks/06_streaming_realtime/056_streaming_window_aggregations_and_watermarking_for_late_arriving_events.py) `Level 300`
- [ ] `pyspark-handbook-057_stream_static_joins_with_slowly_changing_dimension_lookups` — [Stream Static Joins With Slowly Changing Dimension Lookups](notebooks/06_streaming_realtime/057_stream_static_joins_with_slowly_changing_dimension_lookups.py) `Level 300`
- [ ] `pyspark-handbook-058_stream_stream_dual_watermark_joins_with_interval_conditions` — [Stream Stream Dual Watermark Joins With Interval Conditions](notebooks/06_streaming_realtime/058_stream_stream_dual_watermark_joins_with_interval_conditions.py) `Level 300`
- [ ] `pyspark-handbook-059_streaming_deduplication_across_time_windows_with_watermarks` — [Streaming Deduplication Across Time Windows With Watermarks](notebooks/06_streaming_realtime/059_streaming_deduplication_across_time_windows_with_watermarks.py) `Level 300`
- [ ] `pyspark-handbook-060_idempotent_acid_delta_upserts_using_writestream_foreachbatch` — [Idempotent Acid Delta Upserts Using Writestream Foreachbatch](notebooks/06_streaming_realtime/060_idempotent_acid_delta_upserts_using_writestream_foreachbatch.py) `Level 300`
- [ ] `pyspark-handbook-061_arbitrary_stateful_streaming_operations_with_flatmapgroupswithstate_concepts` — [Arbitrary Stateful Streaming Operations With Flatmapgroupswithstate Concepts](notebooks/06_streaming_realtime/061_arbitrary_stateful_streaming_operations_with_flatmapgroupswithstate_concepts.py) `Level 300`
- [ ] `pyspark-handbook-062_dead_letter_queue_dlq_quarantine_streaming_pattern_in_pyspark` — [Dead Letter Queue Dlq Quarantine Streaming Pattern In Pyspark](notebooks/06_streaming_realtime/062_dead_letter_queue_dlq_quarantine_streaming_pattern_in_pyspark.py) `Level 300`
- [ ] `pyspark-handbook-063_streaming_observability_streamingquerylistener_and_sla_latency_alerting` — [Streaming Observability Streamingquerylistener And Sla Latency Alerting](notebooks/06_streaming_realtime/063_streaming_observability_streamingquerylistener_and_sla_latency_alerting.py) `Level 300`
- [ ] `pyspark-handbook-064_medallion_architecture_bronze_ingestion_layer_standards_and_audit_trails` — [Medallion Architecture Bronze Ingestion Layer Standards And Audit Trails](notebooks/07_lakehouse_modeling_scd/064_medallion_architecture_bronze_ingestion_layer_standards_and_audit_trails.py) `Level 300`
- [ ] `pyspark-handbook-065_medallion_architecture_silver_transformation_cleansing_and_conformance` — [Medallion Architecture Silver Transformation Cleansing And Conformance](notebooks/07_lakehouse_modeling_scd/065_medallion_architecture_silver_transformation_cleansing_and_conformance.py) `Level 300`
- [ ] `pyspark-handbook-066_medallion_architecture_gold_business_aggregations_and_dimensional_serving` — [Medallion Architecture Gold Business Aggregations And Dimensional Serving](notebooks/07_lakehouse_modeling_scd/066_medallion_architecture_gold_business_aggregations_and_dimensional_serving.py) `Level 300`
- [ ] `pyspark-handbook-067_distributed_surrogate_key_generation_monotonically_increasing_id_vs_xxhash64_vs_uuids` — [Distributed Surrogate Key Generation Monotonically Increasing Id Vs Xxhash64 Vs Uuids](notebooks/07_lakehouse_modeling_scd/067_distributed_surrogate_key_generation_monotonically_increasing_id_vs_xxhash64_vs_uuids.py) `Level 300`
- [ ] `pyspark-handbook-068_slowly_changing_dimension_type_1_scd_type_1_in_place_overwrites_via_delta_merge` — [Slowly Changing Dimension Type 1 Scd Type 1 In Place Overwrites Via Delta Merge](notebooks/07_lakehouse_modeling_scd/068_slowly_changing_dimension_type_1_scd_type_1_in_place_overwrites_via_delta_merge.py) `Level 300`
- [ ] `pyspark-handbook-069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates` — [Slowly Changing Dimension Type 2 Scd Type 2 Historical Tracking With Effective Dates](notebooks/07_lakehouse_modeling_scd/069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.py) `Level 300`
- [ ] `pyspark-handbook-070_slowly_changing_dimension_type_3_and_type_4_multi_tier_implementation` — [Slowly Changing Dimension Type 3 And Type 4 Multi Tier Implementation](notebooks/07_lakehouse_modeling_scd/070_slowly_changing_dimension_type_3_and_type_4_multi_tier_implementation.py) `Level 300`
- [ ] `pyspark-handbook-071_change_data_capture_cdc_ingestion_from_debezium_kafka_into_lakehouse` — [Change Data Capture Cdc Ingestion From Debezium Kafka Into Lakehouse](notebooks/07_lakehouse_modeling_scd/071_change_data_capture_cdc_ingestion_from_debezium_kafka_into_lakehouse.py) `Level 300`
- [ ] `pyspark-handbook-072_star_schema_implementation_fact_and_conformed_dimension_modeling_in_spark` — [Star Schema Implementation Fact And Conformed Dimension Modeling In Spark](notebooks/07_lakehouse_modeling_scd/072_star_schema_implementation_fact_and_conformed_dimension_modeling_in_spark.py) `Level 300`
- [ ] `pyspark-handbook-073_data_vault_2_0_in_the_lakehouse_hubs_links_and_satellites_modeling` — [Data Vault 2 0 In The Lakehouse Hubs Links And Satellites Modeling](notebooks/07_lakehouse_modeling_scd/073_data_vault_2_0_in_the_lakehouse_hubs_links_and_satellites_modeling.py) `Level 300`
- [ ] `pyspark-handbook-074_catalyst_optimizer_deep_dive_parsed_analyzed_optimized_plans_and_code_generation` — [Catalyst Optimizer Deep Dive Parsed Analyzed Optimized Plans And Code Generation](notebooks/08_performance_and_tuning/074_catalyst_optimizer_deep_dive_parsed_analyzed_optimized_plans_and_code_generation.py) `Level 300`
- [ ] `pyspark-handbook-075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins` — [Adaptive Query Execution Aqe Dynamic Coalescing Join Switching And Skew Joins](notebooks/08_performance_and_tuning/075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins.py) `Level 300`
- [ ] `pyspark-handbook-076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins` — [Data Skew Detection And Mitigation Using Salting And Two Stage Joins](notebooks/08_performance_and_tuning/076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins.py) `Level 300`
- [ ] `pyspark-handbook-077_partitioning_strategies_avoiding_over_partitioning_and_directory_pruning` — [Partitioning Strategies Avoiding Over Partitioning And Directory Pruning](notebooks/08_performance_and_tuning/077_partitioning_strategies_avoiding_over_partitioning_and_directory_pruning.py) `Level 300`
- [ ] `pyspark-handbook-078_bucketing_for_pre_sorted_join_optimization_and_shuffle_elimination` — [Bucketing For Pre Sorted Join Optimization And Shuffle Elimination](notebooks/08_performance_and_tuning/078_bucketing_for_pre_sorted_join_optimization_and_shuffle_elimination.py) `Level 300`
- [ ] `pyspark-handbook-079_caching_and_persistence_storagelevel_memory_and_disk_vs_cache_traps` — [Caching And Persistence Storagelevel Memory And Disk Vs Cache Traps](notebooks/08_performance_and_tuning/079_caching_and_persistence_storagelevel_memory_and_disk_vs_cache_traps.py) `Level 300`
- [ ] `pyspark-handbook-080_repartition_vs_coalesce_internals_full_shuffle_vs_partition_minimization` — [Repartition Vs Coalesce Internals Full Shuffle Vs Partition Minimization](notebooks/08_performance_and_tuning/080_repartition_vs_coalesce_internals_full_shuffle_vs_partition_minimization.py) `Level 300`
- [ ] `pyspark-handbook-081_shuffle_partition_sizing_and_tuning_for_small_vs_terabyte_scale_jobs` — [Shuffle Partition Sizing And Tuning For Small Vs Terabyte Scale Jobs](notebooks/08_performance_and_tuning/081_shuffle_partition_sizing_and_tuning_for_small_vs_terabyte_scale_jobs.py) `Level 300`
- [ ] `pyspark-handbook-082_diagnosing_and_fixing_outofmemory_oom_errors_driver_vs_executor_heap` — [Diagnosing And Fixing Outofmemory Oom Errors Driver Vs Executor Heap](notebooks/08_performance_and_tuning/082_diagnosing_and_fixing_outofmemory_oom_errors_driver_vs_executor_heap.py) `Level 300`
- [ ] `pyspark-handbook-083_spark_ui_eventlog_metrics_task_skew_and_shuffle_spill_analysis` — [Spark Ui Eventlog Metrics Task Skew And Shuffle Spill Analysis](notebooks/08_performance_and_tuning/083_spark_ui_eventlog_metrics_task_skew_and_shuffle_spill_analysis.py) `Level 300`
- [ ] `pyspark-handbook-084_vectorized_execution_and_photon_engine_acceleration_mechanics` — [Vectorized Execution And Photon Engine Acceleration Mechanics](notebooks/08_performance_and_tuning/084_vectorized_execution_and_photon_engine_acceleration_mechanics.py) `Level 300`
- [ ] `pyspark-handbook-085_fixing_accidental_cartesian_products_and_broadcastnestedloopjoins` — [Fixing Accidental Cartesian Products And Broadcastnestedloopjoins](notebooks/08_performance_and_tuning/085_fixing_accidental_cartesian_products_and_broadcastnestedloopjoins.py) `Level 300`
- [ ] `pyspark-handbook-086_unity_catalog_three_tier_namespace_catalogs_schemas_and_managed_vs_external_tables` — [Unity Catalog Three Tier Namespace Catalogs Schemas And Managed Vs External Tables](notebooks/09_unity_catalog_and_security/086_unity_catalog_three_tier_namespace_catalogs_schemas_and_managed_vs_external_tables.py) `Level 300`
- [ ] `pyspark-handbook-087_unity_catalog_volumes_for_unstructured_and_semi_structured_data_governance` — [Unity Catalog Volumes For Unstructured And Semi Structured Data Governance](notebooks/09_unity_catalog_and_security/087_unity_catalog_volumes_for_unstructured_and_semi_structured_data_governance.py) `Level 300`
- [ ] `pyspark-handbook-088_dynamic_row_level_security_rls_using_row_filters_and_user_functions` — [Dynamic Row Level Security Rls Using Row Filters And User Functions](notebooks/09_unity_catalog_and_security/088_dynamic_row_level_security_rls_using_row_filters_and_user_functions.py) `Level 300`
- [ ] `pyspark-handbook-089_dynamic_column_masking_for_pii_protection_and_compliance` — [Dynamic Column Masking For Pii Protection And Compliance](notebooks/09_unity_catalog_and_security/089_dynamic_column_masking_for_pii_protection_and_compliance.py) `Level 300`
- [ ] `pyspark-handbook-090_pii_encryption_salted_hashing_and_aes_decryption_aes_encrypt_sha2` — [Pii Encryption Salted Hashing And Aes Decryption Aes Encrypt Sha2](notebooks/09_unity_catalog_and_security/090_pii_encryption_salted_hashing_and_aes_decryption_aes_encrypt_sha2.py) `Level 300`
- [ ] `pyspark-handbook-091_databricks_secret_scopes_and_secure_credential_injection_via_dbutils_secrets` — [Databricks Secret Scopes And Secure Credential Injection Via Dbutils Secrets](notebooks/09_unity_catalog_and_security/091_databricks_secret_scopes_and_secure_credential_injection_via_dbutils_secrets.py) `Level 300`
- [ ] `pyspark-handbook-092_finops_and_dbu_cost_calculation_using_system_billing_usage_system_tables` — [Finops And Dbu Cost Calculation Using System Billing Usage System Tables](notebooks/09_unity_catalog_and_security/092_finops_and_dbu_cost_calculation_using_system_billing_usage_system_tables.py) `Level 300`
- [ ] `pyspark-handbook-093_lakeflow_declarative_pipelines_and_delta_live_tables_dlt_expectations` — [Lakeflow Declarative Pipelines And Delta Live Tables Dlt Expectations](notebooks/09_unity_catalog_and_security/093_lakeflow_declarative_pipelines_and_delta_live_tables_dlt_expectations.py) `Level 300`
- [ ] `pyspark-handbook-094_databricks_asset_bundles_dabs_for_multi_environment_ci_cd_automation` — [Databricks Asset Bundles Dabs For Multi Environment Ci Cd Automation](notebooks/09_unity_catalog_and_security/094_databricks_asset_bundles_dabs_for_multi_environment_ci_cd_automation.py) `Level 300`
- [ ] `pyspark-handbook-095_semantic_layer_modeling_and_databricks_metric_views_for_bi_and_ai_agents` — [Semantic Layer Modeling And Databricks Metric Views For Bi And Ai Agents](notebooks/09_unity_catalog_and_security/095_semantic_layer_modeling_and_databricks_metric_views_for_bi_and_ai_agents.py) `Level 300`
- [ ] `pyspark-handbook-096_project_finance_real_time_credit_card_fraud_detection_and_customer_360` — [Project Finance Real Time Credit Card Fraud Detection And Customer 360](notebooks/10_end_to_end_projects/096_project_finance_real_time_credit_card_fraud_detection_and_customer_360.py) `Level 300`
- [ ] `pyspark-handbook-097_project_retail_omnichannel_multi_source_lakehouse_etl` — [Project Retail Omnichannel Multi Source Lakehouse Etl](notebooks/10_end_to_end_projects/097_project_retail_omnichannel_multi_source_lakehouse_etl.py) `Level 300`
- [ ] `pyspark-handbook-098_project_healthcare_hipaa_compliant_clinical_records_de_identification` — [Project Healthcare Hipaa Compliant Clinical Records De Identification](notebooks/10_end_to_end_projects/098_project_healthcare_hipaa_compliant_clinical_records_de_identification.py) `Level 300`
- [ ] `pyspark-handbook-099_project_telecom_customer_churn_prediction_feature_store_and_window_features` — [Project Telecom Customer Churn Prediction Feature Store And Window Features](notebooks/10_end_to_end_projects/099_project_telecom_customer_churn_prediction_feature_store_and_window_features.py) `Level 300`
- [ ] `pyspark-handbook-100_project_cybersecurity_web_server_access_log_analytics_and_threat_detection` — [Project Cybersecurity Web Server Access Log Analytics And Threat Detection](notebooks/10_end_to_end_projects/100_project_cybersecurity_web_server_access_log_analytics_and_threat_detection.py) `Level 300`
- [ ] `pyspark-handbook-101_project_iot_industrial_telemetry_anomaly_detection_via_z_score_statistics` — [Project Iot Industrial Telemetry Anomaly Detection Via Z Score Statistics](notebooks/10_end_to_end_projects/101_project_iot_industrial_telemetry_anomaly_detection_via_z_score_statistics.py) `Level 300`
- [ ] `pyspark-handbook-102_project_supply_chain_inventory_replenishment_forecasting_and_reorder_triggers` — [Project Supply Chain Inventory Replenishment Forecasting And Reorder Triggers](notebooks/10_end_to_end_projects/102_project_supply_chain_inventory_replenishment_forecasting_and_reorder_triggers.py) `Level 300`
- [ ] `pyspark-handbook-103_project_e_commerce_collaborative_filtering_recommendation_engine_with_spark_mllib` — [Project E Commerce Collaborative Filtering Recommendation Engine With Spark Mllib](notebooks/10_end_to_end_projects/103_project_e_commerce_collaborative_filtering_recommendation_engine_with_spark_mllib.py) `Level 300`
- [ ] `pyspark-handbook-104_project_agentic_ai_and_rag_lakehouse_document_chunking_and_vector_search` — [Project Agentic Ai And Rag Lakehouse Document Chunking And Vector Search](notebooks/10_end_to_end_projects/104_project_agentic_ai_and_rag_lakehouse_document_chunking_and_vector_search.py) `Level 300`
- [ ] `pyspark-handbook-105_master_enterprise_production_orchestration_workflow_parameters_and_alerting` — [Master Enterprise Production Orchestration Workflow Parameters And Alerting](notebooks/10_end_to_end_projects/105_master_enterprise_production_orchestration_workflow_parameters_and_alerting.py) `Level 300`

### 🔴 Tier 4: Production Hardening, FinOps & Staff Architecture (Level 400)
> Enterprise trade-offs, cross-cloud DR, zero-trust security, and FinOps cost optimization.

- [ ] `pyspark-handbook-rb-01_driver_and_executor_oom_triage` — [Incident Runbook: Driver And Executor Oom Triage](runbooks/01_driver_and_executor_oom_triage.md) `Level 400`
- [ ] `pyspark-handbook-rb-02_shuffle_spill_and_partition_sizing` — [Incident Runbook: Shuffle Spill And Partition Sizing](runbooks/02_shuffle_spill_and_partition_sizing.md) `Level 400`
- [ ] `pyspark-handbook-rb-03_data_skew_and_two_phase_salting` — [Incident Runbook: Data Skew And Two Phase Salting](runbooks/03_data_skew_and_two_phase_salting.md) `Level 400`
- [ ] `pyspark-handbook-rb-04_streaming_state_store_and_watermark_drops` — [Incident Runbook: Streaming State Store And Watermark Drops](runbooks/04_streaming_state_store_and_watermark_drops.md) `Level 400`
- [ ] `pyspark-handbook-rb-05_delta_acid_concurrency_and_metadata_bottlenecks` — [Incident Runbook: Delta Acid Concurrency And Metadata Bottlenecks](runbooks/05_delta_acid_concurrency_and_metadata_bottlenecks.md) `Level 400`
- [ ] `pyspark-handbook-rb-06_pandas_udf_arrow_serialization_and_gc_pauses` — [Incident Runbook: Pandas Udf Arrow Serialization And Gc Pauses](runbooks/06_pandas_udf_arrow_serialization_and_gc_pauses.md) `Level 400`
- [ ] `pyspark-handbook-doc-01_catalyst_optimizer_and_tungsten_engine` — [Architecture: 01 Catalyst Optimizer And Tungsten Engine](architectures/01_catalyst_optimizer_and_tungsten_engine.md) `Level 400`
- [ ] `pyspark-handbook-doc-02_spark_unified_memory_management_and_spill_mechanics` — [Architecture: 02 Spark Unified Memory Management And Spill Mechanics](architectures/02_spark_unified_memory_management_and_spill_mechanics.md) `Level 400`
- [ ] `pyspark-handbook-doc-03_adaptive_query_execution_aqe_deep_dive` — [Architecture: 03 Adaptive Query Execution Aqe Deep Dive](architectures/03_adaptive_query_execution_aqe_deep_dive.md) `Level 400`
- [ ] `pyspark-handbook-doc-04_structured_streaming_and_stateful_stream_processing` — [Architecture: 04 Structured Streaming And Stateful Stream Processing](architectures/04_structured_streaming_and_stateful_stream_processing.md) `Level 400`

---

## Suggested Study Schedules

| Schedule | Pace | Target Audience | Focus |
|---|---|---|---|
| **4-Week Sprint** | 5-6 drills / day | Interview Prep / Fast Track | Focus on Tier 2 & Tier 3 core drills |
| **8-Week Deep-Dive** | 2-3 drills / day | Full Technical Mastery | Complete all 116 exercises + tests |
| **12-Week Staff Track** | 1-2 drills / day | Architect & Tech Lead | Deep dive into Tier 3 & Tier 4 blueprints |

---

## Sister Repositories & Ecosystem Roadmaps

Explore interactive roadmaps across all 14 pillars of the lakehouse ecosystem:
- [Modern Data Engineer Central Hub](https://moderndataengineer.uk)
- [Databricks Handbook](https://github.com/santoshjammi29/databricks-handbook)
- [PySpark Handbook](https://github.com/santoshjammi29/pyspark-handbook)
- [SQL Handbook](https://github.com/santoshjammi29/sql-handbook)
- [dbt Handbook](https://github.com/santoshjammi29/dbt-handbook)
- [Streaming Handbook](https://github.com/santoshjammi29/streaming-handbook)
- [DataOps & CI/CD Handbook](https://github.com/santoshjammi29/dataops-cicd-handbook)
- [System Design Handbook](https://github.com/santoshjammi29/data-system-design-handbook)
- [Interview Handbook](https://github.com/santoshjammi29/data-engineering-interview-handbook)
- [Security Handbook](https://github.com/santoshjammi29/security-handbook)
