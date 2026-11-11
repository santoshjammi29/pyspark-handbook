# Lakehouse FinOps & Cloud Data Cost Optimization Reference

This guide provides architectural strategies, configuration snippets, and SQL system queries to maximize cost efficiency, compute right-sizing, and storage lifecycle management across Databricks, Snowflake, Fabric, and Cloud Storage.

---

## 1. Databricks FinOps & DBU Optimization

### 1.1 Compute Rightsizing & Serverless Compute
- **Serverless SQL 2-Minute Auto-Stop**:
  Unlike classic VMs that take 3–5 minutes to spin up, Databricks Serverless SQL boots in <5 seconds. Set `auto_stop_mins = 2` to eliminate idle spend.
- **Single-Node vs Multi-Node Clusters**:
  For development, unit tests, and light batch jobs (<10GB), enforce Single-Node clusters (`spark.master: "local[*]"`) with 0 worker nodes.
- **Spot / Preemptible Instance Arbitrage**:
  For stateless ETL and batch jobs, use 100% Spot workers with an On-Demand driver node. Spark automatically replaces preempted workers.

### 1.2 DBU Telemetry via Unity Catalog System Tables
Databricks provides operational billing metrics in `system.billing.usage`:

```sql
-- Monthly DBU Consumption by SKU and Workload
SELECT 
    usage_date,
    sku_name,
    usage_unit,
    SUM(usage_quantity) AS total_dbus,
    SUM(usage_quantity * 0.40) AS estimated_usd_cost -- Replace with contracted DBU rate
FROM system.billing.usage
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY usage_date, sku_name, usage_unit
ORDER BY usage_date DESC, total_dbus DESC;

-- Identify Top 10 Most Expensive Databricks Jobs
SELECT 
    usage_metadata.job_id,
    identity_metadata.run_as,
    SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE usage_metadata.job_id IS NOT NULL
  AND usage_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY usage_metadata.job_id, identity_metadata.run_as
ORDER BY total_dbus DESC
LIMIT 10;
```

---

## 2. Delta Lake & Storage FinOps

### 2.1 Delta Deletion Vectors vs. File Rewrites
- Deletion Vectors decouple row deletes from Parquet file rewrites. Instead of rewriting a 500MB Parquet file for a single row deletion or update, Delta writes a tiny 1KB bitmask vector.
- **I/O Reduction**: 60% – 85% on CDC and GDPR/CCPA workflows.
```sql
ALTER TABLE silver.customer_master 
SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

### 2.2 Storage Compaction & Snapshot Purging (`OPTIMIZE` & `VACUUM`)
- Run bin-packing compaction to prevent small-file metadata tax on cloud storage:
```sql
-- Compact small files to ~1GB targets with Z-Order or Liquid Clustering
OPTIMIZE gold.fact_sales CLUSTER BY (store_id, sale_date);

-- Purge orphan physical files older than 7 days
VACUUM gold.fact_sales RETAIN 168 HOURS;
```

### 2.3 Cloud Storage Tiering (ADLS Gen2 / S3)
- Automate lifecycle policies to migrate Bronze raw files:
  - **Hot ($0.018/GB)**: First 30 days
  - **Cool ($0.010/GB)**: Day 31 to 90
  - **Cold ($0.0036/GB)**: Day 91 to 365
  - **Archive ($0.00099/GB)**: Beyond 365 days (88% cost reduction)

---

## 3. Microsoft Fabric Capacity Unit (CU) Smoothing & Auto-Pause

- **Fabric F-SKU Auto-Pause**:
  Non-production Fabric capacities (F2–F64) should be paused outside working hours (7 PM to 7 AM and weekends), reducing runtime from 730 hours to 220 hours (70% savings).
  ```bash
  az fabric capacity suspend --resource-group rg-fabric-dev --capacity-name capfabricdev01
  ```
- **Capacity Unit Smoothing**:
  Fabric dynamically averages compute spikes across 24-hour windows. Design pipelines with bounded concurrency to stay within smoothed limits and prevent interactive throttling.

---

## 4. Snowflake Credit Governance

- **Warehouse Auto-Suspend**: Enforce 60-second auto-suspend on all interactive development warehouses:
  ```sql
  ALTER WAREHOUSE dev_analytics_wh SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
  ```
- **Query Acceleration Service (QAS)**:
  Instead of permanently upsizing an entire warehouse from M to 2XL for rare outlier scans, enable QAS:
  ```sql
  ALTER WAREHOUSE reporting_wh SET 
    ENABLE_QUERY_ACCELERATION = TRUE 
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;
  ```
- **Statement Timeout**: Prevent runaway Cartesian joins from consuming credits indefinitely:
  ```sql
  ALTER WAREHOUSE dev_wh SET STATEMENT_TIMEOUT_IN_SECONDS = 1800; -- Kill queries running >30 min
  ```
