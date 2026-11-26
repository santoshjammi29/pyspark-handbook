# 🏛️ Modern Data Engineering Lakehouse Ecosystem Taxonomy

> **Canonical Controlled Vocabulary & Standards (14-Repository Lakehouse Ecosystem)**  
> **Hub**: [moderndataengineer.uk](https://moderndataengineer.uk) | **Baseline**: 2025/2026 Lakehouse Architecture

---

## 1. The 14 Technical Pillars

Every concept, recipe, exercise, architecture scenario, and interview question across all 14 repositories maps to exactly one primary pillar:

| Pillar ID | Pillar Name | Sibling Authority | Target Core Topics |
|:---:|---|---|---|
| **01** | **Apache Spark & Distributed Compute** | `pyspark-handbook` (`R03`), `spark-experiments` (`R13`) | Catalyst optimizer, Tungsten memory, AQE, shuffle spill, skew salting, Arrow UDFs, Spark Connect. |
| **02** | **Databricks Platform & Lakehouse** | `databricks-handbook` (`R02`), `azure-databricks-project-blueprints` (`R11`) | Unity Catalog 3-tier namespace, Delta Live Tables (DLT), Lakeflow Jobs, Serverless, DABs GitOps, System Tables. |
| **03** | **SQL Server & Database Architecture** | `sql-handbook` (`R04`) | Query execution plans, B-Tree vs LSM, clustered columnstore, MVCC isolation, ACID, distributed joins. |
| **04** | **Modern Lakehouse Architectures** | `data-system-design-handbook` (`R08`) | Medallion architecture (Bronze/Silver/Gold), Delta Lake 3.3 UniForm, Apache Iceberg v2/v3, Apache Hudi. |
| **05** | **System Design for Data** | `data-system-design-handbook` (`R08`) | Distributed consensus (Raft), CAP/PACELC, rate limiting, circuit breakers, idempotency, active-active DR. |
| **06** | **General Data Engineering & Modeling** | `data-engineering-interview-handbook` (`R09`) | Kimball dimensional modeling (Star/Snowflake/OBT), SCD Types 1-6, Data Vault 2.0, bi-temporal ledgers. |
| **07** | **dbt & Analytics Engineering** | `dbt-handbook` (`R05`) | dbt Mesh cross-project references, microbatch incremental, schema contracts, custom generic tests, Semantic Layer. |
| **08** | **Data Lake Architecture & Formats** | `data-system-design-handbook` (`R08`) | Object storage prefix throttling (S3/ADLS/GCS), small file bin-packing, Liquid Clustering, Deletion Vectors. |
| **09** | **Apache Airflow & Orchestration** | `dataops-cicd-handbook` (`R07`) | Airflow 2/3 decoupled scheduler, Dynamic Task Mapping, deferrable operators, reactive datasets, DAG retries. |
| **10** | **Microsoft Fabric & OneLake** | `azure-cheat-sheet` (`R10`) | OneLake Shortcuts, Direct Lake mode over Delta, Lakehouse vs Warehouse, F-SKU Capacity Smoothing, KQL. |
| **11** | **Azure Data Factory & Pipelines** | `azure-cheat-sheet` (`R10`) | Self-Hosted Integration Runtime (SHIR) HA, watermark pipelines, Mapping Data Flows, REST OAuth2 pagination. |
| **12** | **Power BI & Semantic Layer** | `azure-cheat-sheet` (`R10`) | VertiPaq compression, DAX `CALCULATE()` context transition, composite models, aggregation tables, dynamic RLS. |
| **13** | **Python for Data Engineering** | `pyspark-handbook` (`R03`) | CPython GC, PyArrow zero-copy memory, Polars LazyFrames, asyncio event loop, Cython/PyO3 Rust interop. |
| **14** | **Real-Time Streaming & Event Architecture** | `streaming-handbook` (`R06`) | Kafka 3.9 KRaft, Apache Flink stateful streams, Chandy-Lamport barriers, RocksDB state, Debezium CDC, DLQs. |

---

## 2. Difficulty & Mastery Leveling Standards

| Level | Code Enum | Target Persona | Architectural Expectations |
|:---:|:---:|---|---|
| **L1** | `beginner` / `EASY` | Foundational | Core syntax, fundamental definitions, basic transformations, single-node execution. |
| **L2** | `intermediate` / `MEDIUM` | Practitioner | Production pipeline patterns, schema validation, standard configurations, error handling. |
| **L3** | `advanced` / `HARD` | Senior Engineer | Performance tuning, shuffle spill elimination, skew mitigation, incident triage runbooks. |
| **L4** | `architect` / `ARCHITECT` | Principal Architect | Trade-off matrices, cross-cloud DR, enterprise governance, FinOps cost optimization, zero-trust security. |

---

## 3. Approved Primary Sourced Evidence Standards

Every entry must cite at least one authoritative primary source:
- Apache Spark 3.5+ / 4.0 Official Documentation (`spark.apache.org`)
- Delta Lake 3.2 / 3.3 Documentation (`delta.io`)
- Apache Iceberg v2 / v3 REST Catalog Specification (`iceberg.apache.org`)
- Databricks Documentation (`docs.databricks.com`)
- dbt Core / Cloud Documentation (`docs.getdbt.com`)
- Apache Kafka 3.9+ KRaft & Apache Flink 1.20 Documentation (`kafka.apache.org`, `flink.apache.org`)
- Microsoft Azure & Fabric Documentation (`learn.microsoft.com`)
- BCBS 239 Risk Data Aggregation and Risk Reporting Standards (Bank for International Settlements)
- EBA / ECB Regulatory Technical Standards (IRRBB, IFRS 9 Staging)
- DORA State of DevOps & Thoughtworks Technology Radar

---

## 4. Controlled Tag Vocabulary (Max 8 per Asset)

`spark`, `databricks`, `delta-lake`, `iceberg`, `hudi`, `unity-catalog`, `dlt`, `dabs`, `serverless`, `photon`, `aqe`, `skew-join`, `salting`, `shuffle`, `memory-management`, `spill`, `streaming`, `kafka`, `flink`, `rocksdb`, `cdc`, `debezium`, `dbt`, `dbt-mesh`, `microbatch`, `contracts`, `airflow`, `orchestration`, `fabric`, `onelake`, `direct-lake`, `powerbi`, `dax`, `vertipaq`, `azure`, `adls`, `synapse`, `sql-server`, `tsql`, `duckdb`, `python`, `pyarrow`, `polars`, `finops`, `system-design`, `acid`, `liquid-clustering`, `deletion-vectors`, `data-vault`, `scd2`, `ifrs9`, `bcbs239`, `wap`, `data-diff`, `slim-ci`, `oidc`, `zero-trust`.
