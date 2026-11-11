# Medallion / Lakehouse Architecture Reference

## Overview

The Medallion Architecture organises data into progressive quality layers, each adding value through transformation and validation.

```
Source Systems
     │
     ▼
┌──────────────────────────────────────────┐
│  BRONZE (Raw)                            │
│  - Full fidelity copy of source          │
│  - Schema-on-read                        │
│  - Append-only (immutable audit trail)   │
└──────────────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────────────┐
│  SILVER (Cleansed / Conformed)           │
│  - Validated, deduplicated               │
│  - Standardised types and formats        │
│  - Joined/enriched (entity-centric)      │
└──────────────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────────────┐
│  GOLD (Business / Consumption)           │
│  - Aggregated, denormalised              │
│  - Domain-specific marts                 │
│  - Optimised for BI, ML, APIs            │
└──────────────────────────────────────────┘
```

---

## Bronze Layer — Design Rules

### Purpose
- Preserve the exact state of the source data at ingestion time
- Enable full reprocessing / replay at any time

### Rules
1. **Never delete data** — Bronze is append-only
2. **Preserve source schema** — do not rename columns in Bronze
3. **Add ingestion metadata columns**:
   - `_ingestion_timestamp` — when the record was ingested
   - `_source_system` — origin system name
   - `_source_file` — file path or topic partition/offset
   - `_batch_id` — pipeline run ID for traceability
4. **Store as Delta Lake tables** (or Parquet with manifest)
5. **Partition by ingestion date** (`_ingestion_date`) not business date

### Ingestion Patterns
| Pattern | When to Use | Tool |
|---|---|---|
| Full extract | Small tables, no CDC support | ADF Copy, Spark JDBC |
| Watermark incremental | Tables with reliable updated_at | ADF, PySpark |
| CDC log-based | High-volume OLTP sources | Debezium, DMS, Azure SQL CDC |
| Event streaming | Real-time event data | Kafka, Event Hubs → Auto-Loader |
| File landing | File-based sources | ADLS Gen2 → Auto-Loader |

---

## Silver Layer — Design Rules

### Purpose
- Single source of truth for each entity/domain
- Business-agnostic cleansing and conforming

### Transformations Applied
1. **Type casting** — strings to dates, integers, decimals
2. **Deduplication** — remove duplicates using window functions or MERGE
3. **Null handling** — fill defaults, flag nulls per business rules
4. **Standardisation** — country codes, phone formats, currency codes
5. **Enrichment** — join reference data (e.g., country lookup, currency rates)
6. **Entity resolution** — merge duplicate customer records

### Silver Table Design
- One Silver table per entity (customer, product, order)
- Keep at entity grain — not yet aggregated
- Apply SCD Type 2 for historical tracking where needed
- Use Delta Lake MERGE for upserts

### Data Quality Gate (Silver Entry Criteria)
```python
# Great Expectations checks before writing to Silver
expectations = [
    expect_column_values_to_not_be_null("customer_id"),
    expect_column_values_to_be_unique("customer_id"),
    expect_column_values_to_match_regex("email", r"^[\w.]+@[\w.]+\.\w+$"),
    expect_column_values_to_be_between("age", 0, 120),
]
```

---

## Gold Layer — Design Rules

### Purpose
- Business-domain-specific, consumption-optimised datasets
- Aligned to Kimball dimensional model or domain aggregates

### What Goes in Gold
- Kimball star/snowflake schemas (fact + dimension tables)
- Pre-aggregated summary tables (daily sales, monthly KPIs)
- Denormalised wide tables for ML feature stores
- API-ready flattened datasets

### Gold Table Optimisation
```sql
-- Modern Standard (Delta Lake 3.x+ / Databricks Runtime 13.3+): Liquid Clustering
-- Replaces rigid physical partitioning and Z-Order with flexible multidimensional clustering
CREATE TABLE gold.fact_sales (
    order_date_sk INT,
    customer_sk   BIGINT,
    product_sk    BIGINT,
    revenue       DECIMAL(18,2),
    quantity      INT
)
USING DELTA
CLUSTER BY (order_date_sk, customer_sk);

-- Incremental optimization for clustered tables
OPTIMIZE gold.fact_sales;

-- Legacy Pattern (for backward compatibility on older clusters):
-- OPTIMIZE gold.fact_sales ZORDER BY (order_date_sk, customer_sk, product_sk);
```

---

## Microsoft Fabric — Medallion Implementation

| Layer | Fabric Artefact | Storage |
|---|---|---|
| Bronze | Lakehouse (Files section) | ADLS Gen2 OneLake |
| Silver | Lakehouse (Tables section) | Delta tables in OneLake |
| Gold | Lakehouse / Warehouse | Delta / Parquet in OneLake |
| Semantic | Fabric Semantic Model | DirectLake / Import |

### Fabric-Specific Considerations
- All Lakehouses share **OneLake** — a single unified storage layer
- Use **Notebook** (PySpark) or **Dataflow Gen2** for transformations
- Use **Data Pipeline** (ADF-compatible) for orchestration
- Use **Direct Lake** mode for semantic models — no data copy to SSAS

---

## Azure Databricks — Medallion Implementation

### Unity Catalog Namespace
```
catalog.schema.table
  │       │      └── Gold: fact_sales, dim_customer
  │       └── Schema = Layer: bronze, silver, gold
  └── Catalog = Environment: dev, prod
```

### Delta Live Tables (DLT) — Declarative Medallion
```python
import dlt
from pyspark.sql.functions import *

@dlt.table(comment="Bronze: raw orders from source system")
def bronze_orders():
    # Unity Catalog Volumes path (recommended standard)
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/Volumes/prod_catalog/raw_data/orders_volume/")

@dlt.table(
    comment="Silver: cleansed orders",
    expectations={"valid_order_id": "order_id IS NOT NULL"}
)
def silver_orders():
    return dlt.read_stream("bronze_orders") \
        .withColumn("order_date", to_date("order_timestamp")) \
        .dropDuplicates(["order_id"])

@dlt.table(comment="Gold: daily revenue summary")
def gold_daily_revenue():
    return dlt.read("silver_orders") \
        .groupBy("order_date") \
        .agg(sum("revenue").alias("total_revenue"))
```

---

## Streaming in Medallion Architecture

### Streaming Architecture Pattern
```
Kafka/Event Hubs → Bronze (raw events, append-only)
                 → Silver (stateful aggregation, dedup, watermarking)
                 → Gold  (pre-aggregated, served via API or BI)
```

### Latency Targets Per Layer
| Layer | Batch | Micro-batch | Real-time |
|---|---|---|---|
| Bronze | T+1 hour | T+1 minute | T+seconds |
| Silver | T+2 hours | T+5 minutes | T+30 seconds |
| Gold | T+4 hours | T+15 minutes | T+1 minute |

---

## Lake Zones vs Medallion — Comparison

| Zone Model | Medallion Equivalent | Key Difference |
|---|---|---|
| Landing / Raw | Bronze | Same concept |
| Staging / Cleansed | Silver | Same concept |
| Curated / Refined | Gold | Same concept |
| Consumption / Mart | Gold+ / Semantic | Medallion extends into semantic layer |
