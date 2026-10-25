"""
Master README Generator
Scans all 105 Databricks Python notebooks (.py) and builds an enterprise-grade,
clean, GitHub-native documentation with Mermaid diagrams, collapsible curriculum
modules, and zero bloat.
"""

import os
import re
from pathlib import Path

def first_sentence(text: str) -> str:
    """Extracts the first complete sentence, cleaning pipes and whitespace."""
    if not text:
        return ""
    text = text.replace("|", "&#124;").replace("\n", " ").strip()
    match = re.search(r"^(.*?[.!?])(?:\s+[A-Z]|$)", text)
    res = match.group(1).strip() if match else text
    return re.sub(r"\s+", " ", res)

def get_difficulty_badge(diff_str: str) -> str:
    d = diff_str.lower()
    if "beginner" in d:
        return "🟢 `Beginner`"
    elif "advanced" in d:
        return "🔴 `Advanced`"
    return "🟡 `Intermediate`"

def extract_notebook_meta(filepath: Path, base_dir: Path):
    with open(filepath, "r", encoding="utf-8") as f:
        txt = f.read(5000)

    num = filepath.name[:3]
    rel_path = filepath.relative_to(base_dir).as_posix()

    title_m = re.search(r"# MAGIC #+ Notebook \d+:\s*([^\n]+)", txt)
    title = title_m.group(1).strip() if title_m else filepath.stem

    diff_m = re.search(r"# MAGIC \*\*Difficulty:\*\*\s*`?([A-Za-z]+)`?", txt)
    diff = diff_m.group(1).strip() if diff_m else "Intermediate"

    scen_m = re.search(r"Business Scenario & Real-World Context\s*# MAGIC\s*([^\n]+)", txt)
    scen = first_sentence(scen_m.group(1)) if scen_m else ""

    arch_m = re.search(r"Spark & Databricks Architecture Notes\s*# MAGIC\s*([^\n]+)", txt)
    arch = first_sentence(arch_m.group(1)) if arch_m else ""

    return {
        "num": num,
        "title": title,
        "rel_path": rel_path,
        "difficulty": get_difficulty_badge(diff),
        "scenario": scen,
        "arch": arch
    }

def generate_readme(base_dir: Path):
    nb_dir = base_dir / "notebooks"

    modules = [
        {
            "folder": "01_core_dataframe_api",
            "num": "01",
            "title": "Module 01: Core PySpark Foundations & DataFrame API",
            "range": "001 – 012",
            "icon": "📊",
            "difficulty": "🟢 Beginner – 🟡 Intermediate",
            "coverage": "DataFrame creation, explicit StructType schemas, PyArrow Pandas UDFs, safe type casting, column projections, and DBUtils emulation."
        },
        {
            "folder": "02_complex_data_types",
            "num": "02",
            "title": "Module 02: Complex Data Types & Nested Structures",
            "range": "013 – 022",
            "icon": "🧩",
            "difficulty": "🟡 Intermediate",
            "coverage": "Nested StructTypes, Arrays, Maps, Higher-Order Functions (transform, filter, aggregate), JSON parsing, XML shredding, and schema evolution."
        },
        {
            "folder": "03_sql_and_window_functions",
            "num": "03",
            "title": "Module 03: Advanced SQL & Analytical Window Functions",
            "range": "023 – 032",
            "icon": "🪟",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "Ranking, lead/lag, rolling moving averages, cumulative distribution, OLAP cubes, pivots, broadcast hash joins, and range vs rows framing."
        },
        {
            "folder": "04_delta_lake_and_acid",
            "num": "04",
            "title": "Module 04: Delta Lake Architecture & ACID Operations",
            "range": "033 – 043",
            "icon": "🧱",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "Delta transaction log (_delta_log), ACID CRUD, high-throughput MERGE INTO, time travel, VACUUM retention, and Liquid Clustering."
        },
        {
            "folder": "05_data_ingestion_autoloader",
            "num": "05",
            "title": "Module 05: Data Ingestion Patterns & Auto Loader",
            "range": "044 – 053",
            "icon": "📥",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "Databricks Auto Loader (cloudFiles), notification modes, rescued data column, parallel JDBC partitioning, and multi-format lakehouse ingestion."
        },
        {
            "folder": "06_streaming_realtime",
            "num": "06",
            "title": "Module 06: Structured Streaming & Real-Time Processing",
            "range": "054 – 063",
            "icon": "⚡",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "AvailableNow triggers, event-time watermarking, stateful stream-stream joins, deduplication, and Dead Letter Queues (DLQ)."
        },
        {
            "folder": "07_lakehouse_modeling_scd",
            "num": "07",
            "title": "Module 07: Lakehouse Data Modeling, CDC & SCD Implementation",
            "range": "064 – 073",
            "icon": "🏛️",
            "difficulty": "🔴 Advanced",
            "coverage": "Medallion Architecture (Bronze/Silver/Gold), Slowly Changing Dimensions (SCD Types 1, 2, 3, 4), Debezium CDC replay, and Data Vault 2.0."
        },
        {
            "folder": "08_performance_and_tuning",
            "num": "08",
            "title": "Module 08: Performance Tuning, AQE & Skew Mitigation",
            "range": "074 – 085",
            "icon": "🏎️",
            "difficulty": "🔴 Advanced",
            "coverage": "Catalyst Logical & Physical Plans, Adaptive Query Execution (AQE), Salting join skew, Hive-style bucketing, OOM diagnosis, and Photon."
        },
        {
            "folder": "09_unity_catalog_and_security",
            "num": "09",
            "title": "Module 09: Unity Catalog Governance, Security & FinOps",
            "range": "086 – 095",
            "icon": "🛡️",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "Unity Catalog 3-tier namespace, Managed & External Volumes, Row-Level Security, Dynamic Column Masking, Secret Scopes, and DABs."
        },
        {
            "folder": "10_end_to_end_projects",
            "num": "10",
            "title": "Module 10: End-to-End Enterprise Production Projects",
            "range": "096 – 105",
            "icon": "🚀",
            "difficulty": "🔴 Advanced Capstone",
            "coverage": "Full capstones: FinTech Fraud Detection, Omnichannel Retail Lakehouse, Healthcare FHIR Ingestion, IoT Telemetry, RAG Embeddings, and Workflows."
        }
    ]

    total_notebooks = 0
    modules_detail = []

    for mod in modules:
        mod_folder = nb_dir / mod["folder"]
        nb_files = sorted(list(mod_folder.glob("*.py")))
        total_notebooks += len(nb_files)

        nb_entries = [extract_notebook_meta(f, base_dir) for f in nb_files]
        modules_detail.append((mod, nb_entries))

    # Build High-Level Navigation Table
    nav_table_rows = []
    for mod, _ in modules_detail:
        anchor = mod["folder"].replace("_", "-")
        nav_table_rows.append(
            f"| {mod['icon']} **[{mod['title']}](#{anchor})** | `{mod['range']}` | {mod['difficulty']} | {mod['coverage']} |"
        )
    nav_table_md = "\n".join(nav_table_rows)

    # Build Collapsible Detailed Tables
    details_md_list = []
    for mod, entries in modules_detail:
        anchor = mod["folder"].replace("_", "-")
        table_rows = []
        for e in entries:
            table_rows.append(
                f"| `{e['num']}` | [`{e['title']}`]({e['rel_path']}) | {e['difficulty']} | {e['scenario']} | {e['arch']} |"
            )
        table_body = "\n".join(table_rows)

        details_md = f"""<a id="{anchor}"></a>
<details>
<summary><b>{mod['icon']} {mod['title']} ({mod['range']})</b> — <i>{mod['difficulty']}</i></summary>

> 💡 **Core Focus:** {mod['coverage']}

| # | Databricks Notebook (.py) | Difficulty | Business Scenario | Key Architectural Focus |
|:---:|:---|:---:|:---|:---|
{table_body}

</details>
"""
        details_md_list.append(details_md)

    detailed_catalog_md = "\n".join(details_md_list)

    readme_content = f"""# ⚡ The PySpark Handbook

**105 Production Coding Use Cases, Performance Tuning Recipes & Lakehouse Masterclass**

[![CI Validation](https://github.com/santoshjammi29/pyspark-handbook/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshjammi29/pyspark-handbook/actions)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5%2B-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Runtime_15.4%2B_LTS-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2_ACID-00ADD8?style=flat-square&logo=delta&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.9_--_3.12-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![DABs](https://img.shields.io/badge/Deploy-Databricks_Asset_Bundles-008080?style=flat-square&logo=databricks&logoColor=white)](databricks.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)

*The definitive enterprise reference repository of 105 battle-tested Apache Spark & Databricks Lakehouse coding use cases.*  
*Structured in the official **Databricks Source Notebook format (`.py`)** for seamless 1-click sync with **Databricks Repos / Git Folders**, **Databricks Asset Bundles (DABs)**, **VS Code**, and local PySpark / CI runtimes.*

[Curriculum Overview](#-curriculum-overview) • [Lakehouse Architecture](#-lakehouse-architecture) • [Quickstart](#-quickstart) • [DAB Deployment](#-production-deployment-with-dabs) • [Interview Matrix](#-senior-data-engineer-interview-matrix) • [Cheat Sheet](#-performance-tuning-cheat-sheet)

---

## 🎯 Overview & Mission

Modern Lakehouse engineering demands deep fluency in distributed memory execution, ACID transaction semantics, streaming ingestion, query plan optimization, and enterprise data governance.

This repository provides **105 end-to-end, runnable, modular use cases** covering:
- **Core & Advanced Distributed Compute:** PyArrow Pandas UDFs, Higher-Order Functions, Nested Structs/Arrays, Windowing, and Catalyst internals.
- **Lakehouse Storage & ACID Transactions:** Delta Lake transaction log, MERGE INTO, Time Travel, VACUUM, and Liquid Clustering (`CLUSTER BY`).
- **Streaming & Ingestion:** Databricks Auto Loader (`cloudFiles`), event-time watermarking, stream-stream joins, and Dead Letter Queues (DLQ).
- **Enterprise Governance & DevOps:** Unity Catalog 3-tier namespace, Row Filters, Dynamic Column Masking, and Databricks Asset Bundles (DABs) IaC.

---

## 🏛️ Lakehouse Architecture

### 1. Medallion Lakehouse Architecture (Bronze → Silver → Gold)

```mermaid
flowchart LR
    subgraph Raw["Raw Sources"]
        S1["Cloud Storage (S3/ADLS)"]
        S2["Streaming / Kafka"]
        S3["REST APIs / DBs"]
    end

    subgraph Bronze["🥉 Bronze Layer"]
        B1["Raw Source Fidelity<br/>Append-Only Delta<br/>_rescued_data"]
    end

    subgraph Silver["🥈 Silver Layer"]
        S_L["Cleaned & Conformed<br/>Deduplicated & Filtered<br/>SCD Type 1 & 2 / DLQ"]
    end

    subgraph Gold["🥇 Gold Layer"]
        G1["Business Aggregates<br/>Star / Snowflake Marts<br/>Sub-second BI & Feature Store"]
    end

    Raw -->|"Auto Loader (cloudFiles)"| Bronze
    Bronze -->|"Delta MERGE / Streaming Watermark"| Silver
    Silver -->|"Aggregations / Z-Order / Liquid Clustering"| Gold
```

---

### 2. Databricks Unity Catalog 3-Tier Governance Namespace

```mermaid
flowchart TD
    UC["Unity Catalog Metastore<br/><i>(Centralized Governance, Lineage & Access Control)</i>"]

    subgraph Catalogs["Catalogs"]
        C1["dev_catalog"]
        C2["staging_catalog"]
        C3["prod_catalog"]
    end

    subgraph Schemas["Schemas"]
        S1["raw_ingest"]
        S2["silver_conformed"]
        S3["gold_analytics"]
    end

    subgraph Objects["Securable Objects"]
        O1["Managed & External Tables"]
        O2["Managed & External Volumes"]
        O3["Row Filters & Dynamic Column Masks"]
        O4["Registered Models & Functions"]
    end

    UC --> C1 & C2 & C3
    C3 --> S1 & S2 & S3
    S2 --> O1 & O2 & O3 & O4
```

---

### 3. Spark Catalyst Optimization & Adaptive Query Execution (AQE)

```mermaid
flowchart TD
    API["SQL Query / DataFrame API"] --> ULP["Unresolved Logical Plan"]
    ULP -->|Analyzer + Catalog| RLP["Resolved Logical Plan"]
    RLP -->|Optimizer (Predicate Pushdown, Pruning)| OLP["Optimized Logical Plan"]
    OLP -->|Planner (Cost Model)| PP["Selected Physical Plan"]
    PP -->|Whole-Stage CodeGen| RDD["Tungsten / RDD Bytecode Execution"]
    RDD -.->|Runtime Shuffle Stats| AQE["Adaptive Query Execution (AQE)<br/>• Dynamic Partition Coalescing<br/>• SortMergeJoin to BroadcastHashJoin<br/>• Skew Join Optimization"]
```

---

## 🚀 Quickstart

### Option 1: 1-Click Sync into Databricks Workspaces
Every notebook is written in the native Databricks `.py` format (`# Databricks notebook source`):
1. In your **Databricks Workspace**, navigate to **Workspace** > **Repos** (or **Git Folders**).
2. Click **Add Repo** (or **Create Git Folder**) and enter:
   ```text
   https://github.com/santoshjammi29/pyspark-handbook.git
   ```
3. Open any notebook under `notebooks/`, attach an active cluster, and click **Run All**!

---

### Option 2: Local Execution in VS Code or Terminal
Each notebook auto-detects its environment; if run locally without an active cluster, it automatically bootstraps a local SparkSession with Delta Lake support:

```bash
# 1. Clone repository
git clone https://github.com/santoshjammi29/pyspark-handbook.git
cd pyspark-handbook

# 2. Set up virtual environment & install dependencies
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. Execute any notebook directly as a standard Python script
python notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.py
```

---

## 🧭 Curriculum Overview

Click any module below to inspect its individual notebooks, real-world business scenarios, and architectural takeaways:

| Learning Track & Module | Range | Difficulty | Core Topics & Architectural Coverage |
|:---|:---:|:---:|:---|
{nav_table_md}

---

## 📚 Master Curriculum Catalog (105 Use Cases)

{detailed_catalog_md}

---

## 📦 Production Deployment with DABs

This repository natively integrates **Databricks Asset Bundles (DABs)** — the declarative standard for Infrastructure as Code (IaC) across Databricks environments.

### DAB Deployment Flow

```mermaid
flowchart LR
    subgraph Dev["Developer Environment"]
        DAB["databricks.yml<br/>resources/*.yml<br/>notebooks/"]
    end

    subgraph CI["GitHub Actions CI/CD"]
        PR["PR: bundle validate -t dev"]
        Merge["Merge: bundle deploy -t staging"]
        Release["Tag: bundle deploy -t prod"]
    end

    subgraph Workspaces["Databricks Lakehouse"]
        W_dev["Dev Sandbox<br/>/Users/${{user}}/.bundle/"]
        W_stage["Staging Workspace<br/>Automated Testing"]
        W_prod["Production Workspace<br/>Service Principal (OAuth M2M)"]
    end

    Dev --> PR
    PR --> Merge --> Release
    Dev -.->|"bundle deploy -t dev"| W_dev
    Merge --> W_stage
    Release --> W_prod
```

### Deployed Resource Specifications

All resources are defined declaratively in [`resources/`](resources/) and root [`databricks.yml`](databricks.yml):

| Object Type | Resource Config | Production Behavior & SLA |
|:---|:---|:---|
| ⚡ **Multi-Task Workflows & Jobs** | [`resources/jobs.yml`](resources/jobs.yml) | Multi-stage orchestration DAG: Auto Loader ingestion → JSON quarantine → SCD Type 2 dimension merge → Salting join → Gold rollup → Pipeline audit. Includes autoscaling clusters, AQE tuning, retry policies, and email alerts. |
| 🧱 **Delta Live Tables (DLT) Pipelines** | [`resources/dlt_pipelines.yml`](resources/dlt_pipelines.yml) | Declarative medallion ETL pipeline implementing data quality expectations (`@dlt.expect`, `@dlt.expect_or_drop`), Photon engine acceleration, and continuous/triggered lakehouse ingestion. |
| 🏛️ **Unity Catalog Schemas & Volumes** | [`resources/unity_catalog.yml`](resources/unity_catalog.yml) | Provisions conformed schemas (`${{var.catalog}}.${{var.schema}}`) and managed volumes (`raw_landing` for landing files and `streaming_checkpoints` for state storage). |
| 🤖 **Serverless Model Serving** | [`resources/model_serving.yml`](resources/model_serving.yml) | Deploys scalable REST endpoints for Unity Catalog registered models (`credit_card_fraud_model`) with automatic scale-to-zero capabilities for cost efficiency. |
| 📱 **Databricks Apps** | [`resources/apps.yml`](resources/apps.yml) | Deploys the Lakehouse Quality & Cost Monitor Streamlit app ([`apps/lakehouse_monitor/`](apps/lakehouse_monitor/)) directly onto Databricks serverless compute with workspace SSO. |
| 📊 **AI/BI Lakeview Dashboards** | [`resources/dashboards.yml`](resources/dashboards.yml) | Declarative executive analytics dashboard synchronized into workspace paths as version-controlled code. |

### Quick DAB Commands

```bash
# Validate bundle configurations across environments
databricks bundle validate -t dev

# Deploy to your isolated personal development sandbox
databricks bundle deploy -t dev

# Run the master pipeline on-demand
databricks bundle run pyspark_handbook_master_pipeline -t dev

# Clean up all deployed sandbox resources
databricks bundle destroy -t dev
```

---

## 🎯 Senior Data Engineer Interview Matrix

Common production challenges mapped directly to their reference notebook implementations:

| Interview Topic & Challenge | Core Technical Problem | Architectural Solution | Reference Notebook |
|:---|:---|:---|:---:|
| **Severe Data Skew in Distributed Joins** | One partition holds 90% of data, straggler tasks trigger job timeouts. | Key salting, synthetic repartitioning, and two-stage skew aggregation. | [`076_data_skew_mitigation`](notebooks/08_performance_and_tuning/076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins.py) |
| **ACID Concurrency & Write Conflicts** | Concurrent batch updates and streaming writes cause `ConcurrentAppendException`. | Optimistic Concurrency Control (OCC), transaction log mechanics, and partition isolation. | [`034_acid_crud_operations`](notebooks/04_delta_lake_and_acid/034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics.py) |
| **SCD Type 2 Historical Tracking** | Track point-in-time attribute changes without rewriting whole tables. | Two-stage Delta `MERGE INTO` with effective date intervals and `is_current` flags. | [`069_scd_type_2_tracking`](notebooks/07_lakehouse_modeling_scd/069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.py) |
| **Streaming Late-Arriving Data & Watermarks** | Real-time streams receive delayed events; unbounded state memory growth. | Event-time watermarking with appropriate drop tolerances and window aggregations. | [`056_streaming_watermarking`](notebooks/06_streaming_realtime/056_streaming_window_aggregations_and_watermarking_for_late_arriving_events.py) |
| **Incremental File Ingestion at Scale** | S3/ADLS bucket with 1,000,000+ files causes file listing timeouts. | Databricks Auto Loader (`cloudFiles`) with RocksDB state and checkpointing. | [`048_auto_loader_cloudfiles`](notebooks/05_data_ingestion_autoloader/048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.py) |
| **Adaptive Query Execution (AQE) Dynamic Tuning** | Static Spark configurations fail to adapt to unpredictable runtime shuffle sizes. | AQE dynamic shuffle partition coalescing, join switching, and skew joins. | [`075_adaptive_query_execution`](notebooks/08_performance_and_tuning/075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins.py) |
| **Fine-Grained Governance & PII Protection** | Multi-tenant users require dynamic masking for PII without duplicate tables. | Unity Catalog Row Filters and Dynamic Column Masking functions. | [`089_dynamic_column_masking`](notebooks/09_unity_catalog_and_security/089_dynamic_column_masking_for_pii_protection_and_compliance.py) |
| **Delta Lake Small Files & Compaction** | Thousands of micro-batch commits degrade query planning and metadata I/O. | Bin-packing compaction, `OPTIMIZE`, `auto_compaction`, and `VACUUM`. | [`038_delta_optimize_compaction`](notebooks/04_delta_lake_and_acid/038_delta_optimize_file_compaction_and_auto_compaction_strategies.py) |

---

## ⚡ Performance Tuning Cheat Sheet

### 1. Salting Skewed Keys in PySpark
```python
import pyspark.sql.functions as F

SALT_BUCKETS = 8

# Step 1: Add salt to skewed DataFrame
df_salted = df_large.withColumn("salt", F.concat(F.col("skewed_key"), F.lit("_"), F.floor(F.rand() * SALT_BUCKETS)))

# Step 2: Replicate the lookup dimension table to match salt keys
df_lookup_replicated = (
    df_small
    .withColumn("salt_array", F.array([F.lit(i) for i in range(SALT_BUCKETS)]))
    .withColumn("salt_id", F.explode("salt_array"))
    .withColumn("salt", F.concat(F.col("join_key"), F.lit("_"), F.col("salt_id")))
)

# Step 3: Perform balanced join
df_result = df_salted.join(df_lookup_replicated, on="salt", how="inner")
```

### 2. Production AQE Configurations (Apache Spark 3.5+)
```python
# Essential production configurations for Apache Spark 3.5+
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "67108864")  # 64MB
```

---

## 📂 Repository Structure

```
pyspark-handbook/
├── .github/workflows/
│   ├── ci.yml                     # Automated GitHub Actions CI workflow
│   └── deploy_bundles.yml         # Databricks Asset Bundles (DAB) CI/CD deployment
├── apps/
│   └── lakehouse_monitor/         # Databricks App: Streamlit Lakehouse monitor
├── notebooks/                     # 105 Databricks .py notebooks across 10 modules
│   ├── 01_core_dataframe_api/     # 001 - 012: Core DataFrame API, Schemas, Arrow Pandas UDFs
│   ├── 02_complex_data_types/     # 013 - 022: Arrays, Higher-Order Functions, Nested Structs, JSON
│   ├── 03_sql_and_window_functions/ # 023 - 032: OLAP Cubes, Pivots, Window Functions, Joins
│   ├── 04_delta_lake_and_acid/    # 033 - 043: Transaction Log, ACID CRUD, MERGE, Time Travel
│   ├── 05_data_ingestion_autoloader/ # 044 - 053: Auto Loader, Rescued Data, Parallel JDBC
│   ├── 06_streaming_realtime/     # 054 - 063: Structured Streaming, Triggers, Watermarks, DLQ
│   ├── 07_lakehouse_modeling_scd/ # 064 - 073: Medallion Architecture, SCD 1/2/3/4, Debezium CDC
│   ├── 08_performance_and_tuning/ # 074 - 085: Catalyst Optimizer, AQE, Salting Skew, OOM Fixes
│   ├── 09_unity_catalog_and_security/ # 086 - 095: 3-Tier Namespace, Volumes, RLS, Column Masking
│   └── 10_end_to_end_projects/    # 096 - 105: Finance Fraud, Retail, Healthcare, IoT, RAG
├── resources/                     # Databricks Asset Bundle (DAB) definitions
│   ├── apps.yml                   # Databricks Apps resource specification
│   ├── dashboards.yml             # Lakeview AI/BI dashboards specification
│   ├── dlt_pipelines.yml          # Delta Live Tables (DLT) declarative pipelines
│   ├── jobs.yml                   # Multi-task orchestration workflows & clusters
│   ├── model_serving.yml          # Serverless real-time model serving endpoints
│   └── unity_catalog.yml          # Unity Catalog schemas, volumes, and grants
├── scripts/
│   ├── generate_readme.py         # Rebuilds the GitHub master catalog and documentation
│   ├── validate_notebooks.py      # Structural integrity, syntax, and compliance checker
│   └── export_to_ipynb.py         # Converts .py notebooks to Jupyter .ipynb format
├── tests/
│   ├── test_bundle_integrity.py   # Validates DAB configs, resources, and object paths
│   └── test_notebook_integrity.py # Automated integrity test suite (count, format, syntax)
├── databricks.yml                 # Root Databricks Asset Bundle (DAB) configuration
├── requirements.txt               # Production and local PySpark dependencies
├── pyproject.toml
├── LICENSE                        # MIT Open Source License
└── README.md
```

---

## 🧪 Automated Testing & Continuous Integration

This repository enforces strict code quality and structural contracts on every commit via GitHub Actions:
- **Notebook File Contract:** Exactly 105 `.py` Databricks notebooks must exist.
- **Databricks Formatting:** Every notebook begins with `# Databricks notebook source` without empty prepended cells, uses valid `# COMMAND ----------` delimiters, and includes `# MAGIC %md` documentation.
- **Zero Video / Extraneous References:** Verified completely free of video timestamps, video links, or external author references.
- **AST Syntax Check:** All code blocks pass standard Python AST validation.

```bash
# Validate notebook format and AST compliance
python scripts/validate_notebooks.py

# Run integrity tests
python tests/test_notebook_integrity.py
```

---

## 🤝 Contributing

Contributions, feedback, and optimization recipes are warmly welcomed:
1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/awesome-pyspark-recipe`)
3. Commit your Changes (`git commit -m 'Add SCD2 PySpark implementation'`)
4. Push to the Branch (`git push origin feature/awesome-pyspark-recipe`)
5. Open a Pull Request

---

## 📜 License

This project is licensed under the **[MIT License](LICENSE)** — feel free to use it for personal study, enterprise lakehouse development, internal team training, and technical interview preparation.

Maintained with ❤️ by [Santosh Jammi](https://github.com/santoshjammi29)
"""

    with open(base_dir / "README.md", "w", encoding="utf-8") as f:
        f.write(readme_content)

    print(f"Successfully generated clean GitHub-themed README.md with {total_notebooks} cataloged notebooks!")

if __name__ == "__main__":
    generate_readme(Path(__file__).parent.parent)
