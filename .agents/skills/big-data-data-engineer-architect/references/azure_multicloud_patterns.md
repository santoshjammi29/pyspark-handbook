# Azure & Multi-Cloud Data Architecture Patterns

## Azure Data Platform — Service Map

```
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
│  ADF / Fabric Data Factory │ Event Hubs │ IoT Hub │ API Mgmt    │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER                                 │
│  ADLS Gen2 (OneLake) │ Azure Blob Storage │ Azure SQL           │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    PROCESSING LAYER                              │
│  Databricks │ Synapse Spark │ Fabric Spark │ Stream Analytics    │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    SERVING LAYER                                 │
│  Synapse Dedicated SQL │ Synapse Serverless │ Fabric Warehouse    │
│  Databricks SQL Warehouse │ Azure SQL       │                    │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    SEMANTIC / CONSUMPTION                        │
│  Power BI │ Fabric Semantic Model │ Analysis Services │ APIs     │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    GOVERNANCE LAYER                              │
│  Microsoft Purview │ Unity Catalog │ Key Vault │ Azure AD/Entra  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Azure Core Services Reference

### Storage
| Service | Use Case | Format |
|---|---|---|
| **ADLS Gen2** | Data lake, primary storage | Any (Parquet, Delta, CSV, JSON) |
| **Azure Blob Storage** | Unstructured files, archival | Any |
| **Azure SQL Database** | Relational OLTP | Tabular |
| **Cosmos DB** | NoSQL, global distribution | JSON, key-value, graph |

### Compute & Processing
| Service | Use Case | Spark? |
|---|---|---|
| **Azure Databricks** | Lakehouse compute, ML | ✓ (managed) |
| **Synapse Analytics Spark** | Integrated analytics | ✓ (managed) |
| **Microsoft Fabric** | Unified SaaS platform | ✓ (managed) |
| **Azure HDInsight** | Legacy Hadoop workloads | ✓ (self-managed) |
| **Azure Stream Analytics** | Real-time stream processing | ✗ (SQL-based) |

### SQL Serving
| Service | Type | Use Case |
|---|---|---|
| **Synapse Dedicated SQL Pool** | MPP Data Warehouse | High-concurrency BI queries |
| **Synapse Serverless SQL** | On-demand query | Ad-hoc lake exploration |
| **Fabric Warehouse** | Delta-native DW | Microsoft SaaS DW |
| **Databricks SQL Warehouse** | Lakehouse SQL | Lakehouse BI serving |

### Networking & Security
| Service | Purpose |
|---|---|
| **Azure Private Endpoint** | Private connectivity to PaaS services |
| **Azure VNet Integration** | Managed VNet for Databricks/Synapse |
| **Azure Key Vault** | Secrets, certificates, encryption keys |
| **Microsoft Entra ID (AAD)** | Identity, RBAC, Managed Identity |
| **Microsoft Purview** | Data governance, lineage, sensitivity labels |

---

## Architecture Patterns

### Pattern 1: Modern Lakehouse (Recommended Default)
**When**: Greenfield, Azure-native, mixed batch + streaming
```
On-Prem / SaaS → ADF → ADLS Gen2 (Bronze Delta)
                       → Databricks / Fabric (Silver → Gold)
                       → Power BI DirectLake
```

### Pattern 2: Synapse Analytics EDW
**When**: Migrating from on-prem Teradata/SQL DW, high-concurrency SQL
```
Source → ADF → ADLS Gen2 (staging) → Synapse Dedicated Pool
                                    → Power BI
```

### Pattern 3: Real-Time Analytics (Lambda)
**When**: Mixed real-time dashboards + historical batch
```
Events → Event Hubs → Stream Analytics → Synapse / Power BI (speed layer)
       → Databricks Auto-Loader → ADLS Gen2 → Gold (batch layer)
```

### Pattern 4: Microsoft Fabric (All-in-One SaaS)
**When**: Microsoft-first org, reduce infrastructure overhead
```
Source → Fabric Data Factory → OneLake Lakehouse (Bronze)
       → Fabric Notebook/Dataflow (Silver → Gold)
       → Fabric Semantic Model → Power BI
```

---

## Multi-Cloud Equivalency Map

| Capability | Azure | AWS | GCP |
|---|---|---|---|
| **Data Lake** | ADLS Gen2 | S3 | Cloud Storage |
| **MPP Data Warehouse** | Synapse Dedicated | Redshift | BigQuery |
| **Lakehouse Platform** | Databricks / Fabric | Databricks / EMR | Databricks / Dataproc |
| **Managed Spark** | Synapse Spark / Fabric | EMR Serverless | Dataproc Serverless |
| **Stream Ingestion** | Event Hubs | Kinesis Data Streams | Pub/Sub |
| **Stream Processing** | Stream Analytics | Kinesis Data Analytics | Dataflow (Beam) |
| **Managed Airflow** | — (MWAA via marketplace) | MWAA | Cloud Composer |
| **Data Integration** | ADF | AWS Glue / DMS | Dataflow / Data Transfer |
| **Data Catalogue** | Microsoft Purview | AWS Glue Data Catalog | Dataplex |
| **Secrets** | Azure Key Vault | AWS Secrets Manager | GCP Secret Manager |
| **Identity** | Azure AD / Entra | AWS IAM | Cloud IAM |
| **Monitoring** | Azure Monitor | CloudWatch | Cloud Monitoring |
| **Containerised Jobs** | AKS | EKS | GKE |
| **Serverless Functions** | Azure Functions | AWS Lambda | Cloud Functions |

---

## Cloud-Agnostic Tools (Multi-Cloud Portability)

| Tool | Purpose | Works On |
|---|---|---|
| **Apache Spark / Databricks** | Distributed compute | Azure, AWS, GCP, on-prem |
| **Delta Lake** | Table format | Any Spark environment |
| **Apache Iceberg** | Table format | Spark, Flink, Trino, Athena, BigQuery |
| **dbt** | Transformation | Any SQL warehouse |
| **Apache Kafka** | Streaming | Any cloud, on-prem |
| **Terraform** | Infrastructure as Code | Azure, AWS, GCP |
| **Apache Airflow** | Orchestration | Any cloud, on-prem |
| **Great Expectations** | Data quality | Any Python/Spark env |
| **OpenLineage** | Lineage | Airflow, Spark, dbt |
| **Airbyte / Fivetran** | Data integration | Any cloud |

---

## FinOps — Cloud Cost Governance

### Azure Cost Optimisation Strategies

**Databricks**
- Use **Spot Instances** (Azure Spot VMs) for non-critical jobs — up to 90% saving
- Enable **Auto-Termination** on interactive clusters (10–30 min idle)
- Use **Jobs Clusters** (ephemeral) not All-Purpose clusters for production
- Use **Photon** runtime only when justified by query performance
- **Pool clusters** to reduce startup time without keeping clusters alive

**Synapse Dedicated Pool**
- **Pause** pool when not in use (automated via Azure Automation)
- Right-size DWU based on query profile (start DW100c, scale up as needed)
- Use **Result Set Caching** for repeated dashboard queries
- Use **Workload Management** to prioritise critical queries

**Storage**
- Use **Lifecycle Management Policies** to tier cold data to Cool/Archive
- Enable **Soft Delete** with short retention for dev, longer for prod
- Use **Parquet + Snappy** compression (typically 3–5x smaller than CSV)

**General**
- Tag all resources with cost centre, environment, team
- Set **Budget Alerts** at 50%, 80%, 100% of monthly budget
- Use **Azure Advisor** recommendations
- Use **Reserved Instances** for stable, predictable workloads (1–3 year)

---

## Security Architecture

### Defence in Depth for Data Platforms
```
Layer 1: Network      — Private Endpoints, VNet, NSG, Firewall
Layer 2: Identity     — Managed Identity, Service Principals, Entra ID
Layer 3: Access       — RBAC (Azure), Unity Catalog, Synapse RBAC
Layer 4: Data         — Column-level encryption, Dynamic Data Masking
Layer 5: Audit        — Azure Monitor, Purview, Databricks Audit Logs
Layer 6: Compliance   — Purview Sensitivity Labels, Information Protection
```

### RBAC Design Pattern
```
Role: Data Engineer      → Read Bronze, Write Silver, Write Gold (dev)
Role: Data Analyst       → Read Gold only (all environments)
Role: Data Architect     → All environments, schema management
Role: DataOps/Platform   → Infrastructure, monitoring, no data access
Role: Business Owner     → Read semantic model / Power BI datasets only
```

---

## Azure Data Factory (ADF) Production Pipeline Patterns & Triage

### 1. Metadata-Driven Dynamic Ingestion Framework
Instead of creating dozens of hardcoded copy pipelines, implement a single parameterized metadata-driven pipeline driven by an Azure SQL Control Table:

```sql
-- Control Table Schema: etl_metadata.control.pipeline_registry
CREATE TABLE etl_metadata.control.pipeline_registry (
    table_id INT IDENTITY(1,1) PRIMARY KEY,
    source_schema NVARCHAR(100) NOT NULL,
    source_table NVARCHAR(100) NOT NULL,
    target_catalog NVARCHAR(100) NOT NULL,
    target_schema NVARCHAR(100) NOT NULL,
    target_table NVARCHAR(100) NOT NULL,
    watermark_column NVARCHAR(100) NULL,
    current_watermark_val DATETIME2(3) NULL,
    load_type NVARCHAR(20) DEFAULT 'INCREMENTAL', -- 'FULL' or 'INCREMENTAL'
    batch_concurrency_group INT DEFAULT 1,
    is_active BIT DEFAULT 1,
    last_run_status NVARCHAR(50) NULL,
    last_success_timestamp DATETIME2(3) NULL
);
```

**Pipeline Flow:**
1. **Lookup Activity**: `SELECT * FROM etl_metadata.control.pipeline_registry WHERE is_active = 1`.
2. **ForEach Activity (Sequential = False, BatchCount = 8)**:
   - Bounded concurrency prevents flooding on-premises SQL Server connection pools.
   - Child pipeline parameter pass: `@item().source_schema`, `@item().source_table`, `@item().current_watermark_val`.

### 2. High-Watermark Incremental Load with Two-Phase Commit
A common production failure is advancing the watermark *before* or *during* Databricks processing. If Databricks fails mid-stream, data is permanently lost from future scheduled loads.

**Correct Two-Phase Watermark Protocol:**
```
[1. Lookup Old Watermark] ──> [2. Query Source Max(ModifiedDate)]
                                          │
                                          ▼
                               [3. Extract Source Batch]
                               (WHERE Date > Old AND Date <= New)
                                          │
                                          ▼
                            [4. Databricks Notebook Run]
                            (ACID Delta Merge + Row Count Assert)
                                          │
                        ┌─────────────────┴─────────────────┐
                        ▼                                   ▼
                   [On Success]                        [On Failure]
           [5. Update Control Table]             [Alert On-Call Pager]
         (Watermark = New Watermark)          (Watermark Remains at Old)
```

### 3. Cross-System Retry Multipliers (ADF × Databricks)
When ADF executes a Databricks Job activity, both systems offer native retry mechanisms:
- **ADF Activity**: `retry`: 3, `retryIntervalInSeconds`: 30
- **Databricks Workflow Task**: `max_retries`: 3, `min_retry_interval_millis`: 30000

**The Disaster**: If an unhandled transient failure occurs, total executions = $(1 + 3) \times (1 + 3) = 16$ attempts! This causes database connection pool exhaustion, duplicate chargebacks, OCC commit conflicts in Delta, and uncontrolled cost explosion.

**Production Standard**:
1. **Centralize Retry Ownership**: Set Databricks task `max_retries: 0`. Let ADF act as the single source of truth for retry scheduling and backoff.
2. **Mandatory Idempotency**: Pass `@pipeline().RunId` to the Databricks notebook. Write logic must execute an idempotent `MERGE` or partition overwrite scoped to the execution run ID.

### 4. Self-Hosted Integration Runtime (SHIR) High Availability
For secure on-premises or private VNet data extraction:
- **HA Clustering**: Deploy 2 to 4 nodes in a single SHIR gateway cluster. High Availability automatically switches master role if Node 1 crashes.
- **Concurrent Job Throttling**: Configure concurrent jobs per node:
  $$\text{Concurrent Jobs} = \text{CPU Cores} \times 4$$
- **Network Security**: Enforce TLS 1.3 and outbound-only traffic on port 443 to Azure Service Bus endpoints; no inbound firewall ports required.
