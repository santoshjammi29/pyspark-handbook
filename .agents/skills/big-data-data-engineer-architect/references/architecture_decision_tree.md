# Architecture Decision Tree

## How to Choose the Right Architecture

```
START: What is the primary use case?
│
├── Primarily BI/Reporting (structured queries, low latency)?
│   │
│   ├── High concurrency (100+ users)?
│   │   └── → Synapse Dedicated SQL Pool / Snowflake / Redshift
│   │         + Kimball Star Schema on top
│   │
│   └── Medium concurrency or cost-sensitive?
│       └── → Databricks SQL Warehouse / Synapse Serverless
│             + Medallion Lakehouse with Gold star schema
│
├── Mixed (BI + ML + Streaming)?
│   └── → Medallion Lakehouse (ADLS Gen2 + Databricks/Fabric)
│         + dbt for transformations
│         + Semantic model for BI consumption
│
├── Primarily ML/Data Science?
│   └── → Medallion Lakehouse
│         + Feature Store (Databricks Feature Store / Feast)
│         + MLflow for experiment tracking
│
├── Real-time / Streaming dominant?
│   │
│   ├── Sub-second latency needed?
│   │   └── → Apache Flink + Azure Event Hubs + Redis/Cosmos
│   │
│   └── Sub-minute latency acceptable?
│       └── → Structured Streaming (Databricks / Fabric)
│             + Kafka/Event Hubs + Delta Lake
│
└── Legacy migration / EDW modernisation?
    │
    ├── Keep business logic, change platform?
    │   └── → Re-platform: SSIS → ADF, T-SQL → Spark SQL
    │
    ├── Full redesign + business rule changes?
    │   └── → Re-architect: Legacy DW → Medallion Lakehouse
    │
    └── Temporary: keep source, add cloud layer?
        └── → Federated query (Synapse Serverless / Trino)
              then migrate over time
```

---

## Architecture Selection Matrix

| Factor | Kimball DW | Medallion Lakehouse | Data Vault | Data Mesh |
|---|---|---|---|---|
| **Primary pattern** | Star schema | Bronze/Silver/Gold | Hub/Link/Satellite | Domain ownership |
| **Best for** | BI-heavy, defined requirements | Mixed/evolving requirements | High-change enterprise | Large org, multiple teams |
| **Schema flexibility** | Low (schema-on-write) | High (schema-on-read) | Medium | High |
| **Historical tracking** | SCD types | Delta Time Travel | Full history (default) | Varies by domain |
| **Query performance** | Very high (pre-aggregated) | High (with optimisation) | Lower (needs marts) | Varies |
| **Implementation speed** | Medium | Fast | Slow | Very slow |
| **Tooling** | SQL, SSAS | Spark, dbt, Delta | dbt-vault, WhereScape | Dataplex, Purview |
| **Cost** | Higher (dedicated DW) | Lower (serverless possible) | Higher (complexity) | Highest (federation) |

---

## Azure Platform Selection

```
Microsoft-only organization with SaaS preference & Direct Lake BI?
→ Microsoft Fabric (all-in-one OneLake platform)

Azure-native but want best-in-class Spark, Photon & Unity Catalog?
→ Azure Databricks + ADLS Gen2 (Liquid Clustering, Serverless, Volumes)

High-concurrency SQL DW with existing SQL Server skills?
→ Azure Synapse Analytics (Dedicated SQL Pool) or Fabric Warehouse

Multi-cloud or open-format vendor-neutral required?
→ Databricks (Azure/AWS/GCP) + Delta Lake (UniForm) / Apache Iceberg + Terraform

Sub-100GB single-node or local embedded transformations?
→ DuckDB + Polars / PyArrow (zero cluster management overhead)

Hybrid (on-prem + cloud)?
→ ADF Self-Hosted IR + Databricks + Azure SQL
```

---

## When to Use Each ETL/ELT Tool

| Scenario | Tool | Reason |
|---|---|---|
| Simple file copy, schedule-based | ADF Copy Activity | No code, GUI-based, fast setup |
| Complex multi-step pipeline, Azure-native | ADF Pipelines + Data Flows | Full orchestration, 90+ connectors |
| SQL-based transformations & modeling | dbt (Core / Cloud / Mesh) | Version control, testing, docs, semantic layer |
| Declarative Lakehouse Medallion pipelines | Databricks Delta Live Tables (DLT) | Out-of-the-box expectations, lineage, auto-compaction |
| Large-scale Spark transformations | Databricks Notebooks/Jobs / Photon | Full distributed Spark compute, Liquid Clustering |
| In-process analytical transformations (<100GB)| DuckDB + Polars | Ultra-low memory, sub-second execution, zero cluster spinup |
| Python-complex DAGs & custom orchestrations | Apache Airflow / Astronomer / Cosmos | Unlimited extensibility, dynamic DAG generation |
| SaaS source ingestion | Fivetran / Airbyte | Pre-built connectors, no-code managed syncs |
| Real-time streaming & event analytics | Kafka / Event Hubs + Structured Streaming | Exactly-once guarantees, stateful RocksDB processing |
| Low-latency CDC ingestion | Debezium + Kafka / Fabric Eventstreams | Log-based, sub-second transactional replication |

---

## Data Contract Decision

Always define a **data contract** when:
- A team produces data consumed by another team
- A dataset is exposed via API
- A Gold dataset feeds a regulated report
- A source schema might change independently

### Data Contract Elements
```yaml
name: "orders_silver"
version: "2.1.0"
owner: "Data Engineering Team"
sla:
  freshness: "6 hours"
  availability: "99.5%"
schema:
  - name: order_id
    type: string
    nullable: false
    unique: true
    description: "Unique order identifier"
  - name: order_date
    type: date
    nullable: false
  - name: revenue
    type: decimal(18,2)
    nullable: false
    checks:
      - min: 0
quality:
  row_count_min: 1000
  null_rate_max: 0.01
consumers:
  - "Finance Mart (fct_revenue)"
  - "Executive Dashboard"
breaking_change_policy: "30-day notice, bump major version"
```
