"""
Master README Generator
Scans all 105 Databricks Python notebooks (.py) and builds an enterprise-grade,
visually stunning, and functionally organized catalog and documentation.
"""

import re
from pathlib import Path

def sanitize_table_text(text: str) -> str:
    """Escapes pipes and cleans whitespace for Markdown tables."""
    if not text:
        return ""
    cleaned = text.replace("|", "&#124;").replace("\n", " ").strip()
    return re.sub(r"\s+", " ", cleaned)

def generate_readme(base_dir: Path):
    nb_dir = base_dir / "notebooks"
    module_folders = sorted([d for d in nb_dir.iterdir() if d.is_dir()])

    module_metadata = {
        "01_core_dataframe_api": {
            "title": "Module 01: Core PySpark Foundations & DataFrame API",
            "range": "001 – 012",
            "icon": "📊",
            "summary": "Core DataFrame manipulation, explicit StructType schemas, PyArrow Pandas UDFs, safe type casting, column projections, and Databricks dbutils emulation.",
            "diff_summary": "🟢 Beginner – 🟡 Intermediate",
            "anchor": "module-01"
        },
        "02_complex_data_types": {
            "title": "Module 02: Complex Data Types & Nested Structures Handling",
            "range": "013 – 022",
            "icon": "🧩",
            "summary": "Nested StructTypes, Arrays, Maps, Higher-Order Functions (transform, filter, aggregate), JSON parsing, XML shredding, and schema evolution.",
            "diff_summary": "🟡 Intermediate",
            "anchor": "module-02"
        },
        "03_sql_and_window_functions": {
            "title": "Module 03: Advanced SQL, Aggregations & Analytical Window Functions",
            "range": "023 – 032",
            "icon": "🪟",
            "summary": "Ranking, lead/lag, rolling moving averages, cumulative distribution, OLAP cubes, pivots, broadcast hash joins, and range vs rows framing.",
            "diff_summary": "🟡 Intermediate – 🔴 Advanced",
            "anchor": "module-03"
        },
        "04_delta_lake_and_acid": {
            "title": "Module 04: Delta Lake Architecture, ACID Operations & Time Travel",
            "range": "033 – 043",
            "icon": "🧱",
            "summary": "Delta transaction log (_delta_log JSON & checkpoints), ACID CRUD, high-throughput MERGE INTO, time travel, VACUUM retention, and Liquid Clustering.",
            "diff_summary": "🟡 Intermediate – 🔴 Advanced",
            "anchor": "module-04"
        },
        "05_data_ingestion_autoloader": {
            "title": "Module 05: Data Ingestion Patterns, Auto Loader & File Formats",
            "range": "044 – 053",
            "icon": "📥",
            "summary": "Databricks Auto Loader (cloudFiles), notification modes, rescued data column, parallel JDBC partitioning, and multi-format lakehouse ingestion.",
            "diff_summary": "🟡 Intermediate – 🔴 Advanced",
            "anchor": "module-05"
        },
        "06_streaming_realtime": {
            "title": "Module 06: Streaming & Near Real-Time Processing",
            "range": "054 – 063",
            "icon": "⚡",
            "summary": "Structured Streaming, AvailableNow triggers, event-time watermarking, stateful stream-stream joins, deduplication, and Dead Letter Queues (DLQ).",
            "diff_summary": "🟡 Intermediate – 🔴 Advanced",
            "anchor": "module-06"
        },
        "07_lakehouse_modeling_scd": {
            "title": "Module 07: Lakehouse Data Modeling, CDC & SCD Implementation",
            "range": "064 – 073",
            "icon": "🏛️",
            "summary": "Medallion Architecture (Bronze/Silver/Gold), Slowly Changing Dimensions (SCD Type 1, 2, 3, 4), Debezium CDC replay, and Data Vault 2.0.",
            "diff_summary": "🔴 Advanced",
            "anchor": "module-07"
        },
        "08_performance_and_tuning": {
            "title": "Module 08: Performance Tuning, Query Optimization & Skew Mitigation",
            "range": "074 – 085",
            "icon": "🏎️",
            "summary": "Catalyst Logical & Physical Plans, Adaptive Query Execution (AQE), Salting join skew, Hive-style bucketing, OOM diagnosis, and Photon.",
            "diff_summary": "🔴 Advanced",
            "anchor": "module-08"
        },
        "09_unity_catalog_and_security": {
            "title": "Module 09: Databricks Platform, Unity Catalog, Security & FinOps",
            "range": "086 – 095",
            "icon": "🛡️",
            "summary": "Unity Catalog 3-tier namespace, Managed & External Volumes, Row-Level Security, Dynamic Column Masking, Secret Scopes, and Databricks Asset Bundles.",
            "diff_summary": "🟡 Intermediate – 🔴 Advanced",
            "anchor": "module-09"
        },
        "10_end_to_end_projects": {
            "title": "Module 10: End-to-End Enterprise Projects & Production Workflows",
            "range": "096 – 105",
            "icon": "🚀",
            "summary": "Full capstones: FinTech Fraud Detection, Omnichannel Retail Lakehouse, Healthcare FHIR Ingestion, IoT Telemetry, RAG Embeddings, and Workflows.",
            "diff_summary": "🔴 Advanced Capstone",
            "anchor": "module-10"
        }
    }

    catalog_sections = []
    total_count = 0

    # Build Quick Jump Matrix
    nav_rows = []
    for mod_key, meta in module_metadata.items():
        module_subname = meta['title'].split(':')[1].strip()
        nav_rows.append(
            f"| {meta['icon']} **[{module_subname}](#{meta['anchor']})** "
            f"| `{meta['range']}` "
            f"| {meta['diff_summary']} "
            f"| {meta['summary']} |"
        )
    quick_jump_table = (
        "| Learning Track & Module | Notebooks | Difficulty Level | Core Architectural Coverage |\n"
        "|:---|:---:|:---:|:---|\n" + "\n".join(nav_rows)
    )

    # Build Detailed Modules Catalog
    for mod_dir in module_folders:
        mod_key = mod_dir.name
        meta = module_metadata.get(mod_key, {
            "title": mod_key,
            "range": "",
            "icon": "📁",
            "summary": "",
            "diff_summary": "",
            "anchor": mod_key
        })

        module_desc = meta['summary']

        table_lines = [
            f'<a id="{meta["anchor"]}"></a>',
            f"<details open>",
            f"<summary><h3>{meta['icon']} {meta['title']} ({meta['range']})</h3></summary>\n",
            f"> 💡 **Focus:** {module_desc}\n",
            "| # | Databricks Notebook (.py) | Difficulty | Business Scenario & Real-World Problem | Key Architectural Focus |",
            "|:---:|:---|:---:|:---|:---|"
        ]

        nbs = sorted(list(mod_dir.glob("*.py")))
        for nb_path in nbs:
            total_count += 1
            rel_path = f"notebooks/{mod_key}/{nb_path.name}"
            content = nb_path.read_text(encoding="utf-8")

            nb_id = nb_path.stem[:3]

            # Extract Title
            title_match = re.search(r"# MAGIC # Notebook \d+:\s*(.*)", content)
            if title_match:
                clean_title = title_match.group(1).strip()
            else:
                clean_title = nb_path.stem[4:].replace("_", " ").title()
            clean_title = sanitize_table_text(clean_title)

            # Extract Difficulty
            difficulty = "Intermediate"
            if "`Beginner`" in content:
                difficulty = "Beginner"
            elif "`Advanced`" in content:
                difficulty = "Advanced"

            if difficulty == "Beginner":
                diff_badge = "🟢 `Beginner`"
            elif difficulty == "Intermediate":
                diff_badge = "🟡 `Intermediate`"
            else:
                diff_badge = "🔴 `Advanced`"

            # Extract Scenario
            scenario = "Enterprise PySpark implementation and architectural design pattern."
            scenario_match = re.search(
                r"### Business Scenario & Real-World Context\s*\n# MAGIC (.*?)(?=\n# MAGIC\n# MAGIC ###|\n# MAGIC ###|\n\n# COMMAND ----------)",
                content,
                re.DOTALL
            )
            if scenario_match:
                clean_lines = [
                    l.replace("# MAGIC", "").strip()
                    for l in scenario_match.group(1).splitlines()
                    if l.replace("# MAGIC", "").strip()
                ]
                scenario = " ".join(clean_lines)
                if len(scenario) > 135:
                    scenario = scenario[:132] + "..."
            scenario = sanitize_table_text(scenario)

            # Extract Primary Architectural Takeaway
            takeaway = "Production-grade distributed processing and fault tolerance."
            takeaways_match = re.search(
                r"### Key Architectural Takeaways\s*\n(.*?)(?=\n# MAGIC\n# MAGIC ---|\n# MAGIC ---|\n\n# COMMAND ----------)",
                content,
                re.DOTALL
            )
            if takeaways_match:
                t_lines = [
                    l.replace("# MAGIC", "").strip()
                    for l in takeaways_match.group(1).splitlines()
                    if l.replace("# MAGIC", "").strip() and (l.strip().startswith("-") or l.strip().startswith("# MAGIC -"))
                ]
                if t_lines:
                    first_takeaway = t_lines[0].lstrip("- *").rstrip("*").strip()
                    first_takeaway = re.sub(r"^\*\*(.*?)\*\*.*", r"\1", first_takeaway)
                    if len(first_takeaway) > 105:
                        first_takeaway = first_takeaway[:102] + "..."
                    takeaway = first_takeaway
            takeaway = sanitize_table_text(takeaway)

            table_lines.append(
                f"| `{nb_id}` | [`{clean_title}`]({rel_path}) | {diff_badge} | {scenario} | {takeaway} |"
            )

        table_lines.append("\n<p align=\"right\"><a href=\"#-interactive-curriculum-navigation\">⬆️ Back to Top</a></p>\n")
        table_lines.append("</details>\n")
        catalog_sections.append("\n".join(table_lines))

    all_catalog_md = "\n".join(catalog_sections)

    readme_content = f"""<div align="center">

# ⚡ The PySpark Handbook

**105 Production Coding Use Cases, Performance Tuning Recipes & Lakehouse Masterclass**

[![Stars](https://img.shields.io/github/stars/santoshjammi29/pyspark-handbook?style=for-the-badge&logo=github&color=FFD700)](https://github.com/santoshjammi29/pyspark-handbook/stargazers)
[![Forks](https://img.shields.io/github/forks/santoshjammi29/pyspark-handbook?style=for-the-badge&logo=github&color=blue)](https://github.com/santoshjammi29/pyspark-handbook/network/members)
[![CI Notebook Validation](https://github.com/santoshjammi29/pyspark-handbook/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshjammi29/pyspark-handbook/actions)
[![Databricks](https://img.shields.io/badge/Databricks-Native_.py_Notebooks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5%2B-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2_ACID-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-008080?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Python](https://img.shields.io/badge/Python-3.9_--_3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg?style=for-the-badge)](LICENSE)
[![Notebook Count](https://img.shields.io/badge/Notebooks-105_Complete-success.svg?style=for-the-badge)](#-master-curriculum-catalog-of-105-use-cases)

*The definitive enterprise reference repository of 105 battle-tested Apache Spark & Databricks Lakehouse coding use cases.*  
*Structured in the official **Databricks Source Notebook format (`.py`)** for seamless 1-click sync with **Databricks Repos / Git Folders**, **Databricks Asset Bundles (DABs)**, **VS Code**, and local PySpark / CI runtimes.*

[📖 Curriculum Catalog](#-master-curriculum-catalog-of-105-use-cases) • [📦 DAB Prod Deployment](#-enterprise-production-deployment-with-databricks-asset-bundles-dabs) • [⚡ Quick Cheat Sheet](#-quick-cheat-sheet--tldr) • [🎯 Interview Matrix](#-senior-data-engineer-interview-matrix) • [🚀 Quickstart](#-databricks--local-quickstart)

---

</div>

## 🎯 Executive Summary & Mission

Modern Lakehouse engineering demands deep fluency in distributed memory execution, ACID transaction semantics, streaming ingestion, query plan optimization, and enterprise data governance. 

This repository provides **105 end-to-end, runnable, modular use cases** designed for:
- **Senior & Staff Data Engineers:** Mastering complex data types, query execution tuning (AQE, DPP), join skew resolution (salting), and high-throughput Delta MERGE operations.
- **Lakehouse & Solution Architects:** Implementing production Medallion Architectures (Bronze → Silver → Gold), Change Data Capture (CDC), Slowly Changing Dimensions (SCD Types 1, 2, 3, 4), and Unity Catalog governance.
- **Interview Candidates:** Preparing for senior/lead technical interviews with real-world scenarios, architectural trade-offs, and 300+ interview questions.

---

## 🧭 Interactive Curriculum Navigation

Click any module below to jump directly to its complete catalog of notebooks, business scenarios, and architectural takeaways:

{quick_jump_table}

---

## 🏛️ Lakehouse Architecture & Visual Designs

### 1. The Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                  MEDALLION LAKEHOUSE ARCHITECTURE                                       │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

  ┌───────────────────┐
  │   RAW SOURCES     │
  │ • Cloud Storage   │
  │ • IoT Telemetry   │
  │ • Debezium CDC    │
  │ • REST APIs / DBs │
  └─────────┬─────────┘
            │
            ▼   [Auto Loader / cloudFiles: Schema Inference, Rescued Data, Incremental Notification]
  ┌───────────────────────────────────────────────────────────────────────────────────────┐
  │ 🥉 BRONZE LAYER (Raw Ingestion / Raw Lakehouse Storage)                               │
  │ • Raw source fidelity, append-only Delta tables                                       │
  │ • System metadata columns (_rescued_data, _input_file_name, _ingest_timestamp)        │
  │ • Fast ingestion without expensive transformations                                    │
  └─────────┬─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼   [Delta MERGE INTO, Deduplication, Safe Type Casting, Quarantine Rules, Watermarking]
  ┌───────────────────────────────────────────────────────────────────────────────────────┐
  │ 🥈 SILVER LAYER (Cleaned, Conformed, Enriched Lakehouse Model)                        │
  │ • Cleansed, validated, and normalized tables with strict schema enforcement            │
  │ • Slowly Changing Dimensions (SCD Type 1 & Type 2), CDC upserts, Conformed Dimensions │
  │ • Corrupted records routed to Dead Letter Queues (DLQ)                                 │
  └─────────┬─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼   [Aggregations, Window Functions, Dimensional Modeling, Star/Snowflake Schema, Caching]
  ┌───────────────────────────────────────────────────────────────────────────────────────┐
  │ 🥇 GOLD LAYER (Curated Business & Analytics Marts)                                    │
  │ • Aggregated KPIs, summary tables, OLAP cubes, customer 360 views                     │
  │ • Highly optimized with Z-Ordering / Liquid Clustering for sub-second BI latency       │
  │ • Powering BI dashboards (Tableau/PowerBI), Feature Stores, and GenAI / RAG pipelines │
  └───────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2. Databricks Unity Catalog 3-Tier Governance Namespace

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  UNITY CATALOG METASTORE                                         │
│                      (Centralized Governance, Auditing, Lineage & IAM)                           │
└───────────────────────────────────┬──────────────────────────────────────────────────────────────┘
                                    │
    ┌───────────────────────────────┴───────────────────────────────┐
    │                                                               │
    ▼                                                               ▼
┌──────────────────────────────────────────────┐  ┌──────────────────────────────────────────────┐
│ CATALOG: `production_lakehouse`             │  │ CATALOG: `development_sandbox`                │
└───────────────────┬──────────────────────────┘  └───────────────────┬──────────────────────────┘
                    │                                                 │
    ┌───────────────┴───────────────┐                 ┌───────────────┴───────────────┐
    ▼                               ▼                 ▼                               ▼
┌──────────────────────┐  ┌────────────────────┐  ┌──────────────────────┐  ┌────────────────────┐
│ SCHEMA: `finance`    │  │ SCHEMA: `sales`    │  │ SCHEMA: `analytics`  │  │ SCHEMA: `staging`  │
└──────────┬───────────┘  └─────────┬──────────┘  └──────────┬───────────┘  └─────────┬──────────┘
           │                        │                        │                        │
  ┌────────┴────────┐      ┌────────┴────────┐      ┌────────┴────────┐      ┌────────┴────────┐
  │ • Tables        │      │ • Tables        │      │ • Tables        │      │ • Tables        │
  │ • Dynamic Masks │      │ • Row Filters   │      │ • Views         │      │ • External Tabs │
  │ • Volumes (Raw) │      │ • Volumes (Doc) │      │ • Functions/UDFs│      │ • Managed Vols  │
  └─────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
```

---

### 3. Spark Catalyst Optimization & Adaptive Query Execution (AQE)

```
SQL Query / DataFrame API
         │
         ▼
[ Unresolved Logical Plan ] ──> (Analyzer + Catalog) ──> [ Resolved Logical Plan ]
                                                                  │
                                                                  ▼
                                                      [ Optimized Logical Plan ]
                                                      • Constant Folding
                                                      • Predicate Pushdown
                                                      • Projection Pruning
                                                                  │
                                                                  ▼
                                                       [ Physical Plans (1..N) ]
                                                                  │
                                                                  ▼ (Cost-Based Model)
                                                        [ Selected Physical Plan ]
                                                                  │
                                                                  ▼ (Code Generation)
                                                      [ RDD Bytecode Execution ]
                                                                  │
                                                                  ▼ (Runtime Shuffle Stats)
                                                    [ Adaptive Query Execution (AQE) ]
                                                    ├── Coalesce shuffle partitions dynamically
                                                    ├── Convert SortMergeJoin to BroadcastHashJoin
                                                    └── Split & optimize skewed join partitions
```

---

## 🚀 Databricks & Local Quickstart

### 1. Direct Sync into Databricks Workspaces (1-Click Setup)
Because every notebook in this repository is written in the native Databricks `.py` format, importing into your workspace requires zero conversion:
1. Open your **Databricks Workspace**.
2. Navigate to **Workspace** > **Repos** (or **Git Folders**).
3. Click **Add Repo** (or **Create Git Folder**) and enter:
   ```text
   https://github.com/santoshjammi29/pyspark-handbook.git
   ```
4. Open any notebook under `notebooks/`. It will instantly render interactive Databricks cells, markdown documentation, widgets, and execution controls!
5. Attach any active cluster and click **Run All**!

---

### 2. Local Execution in VS Code or Terminal
Every notebook includes an environment-aware SparkSession bootstrapper (`try: spark except NameError:`). If executed locally, it automatically initializes a self-contained local PySpark session with Delta Lake support:

```bash
# 1. Clone repository
git clone https://github.com/santoshjammi29/pyspark-handbook.git
cd pyspark-handbook

# 2. Set up virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install PySpark, Delta Lake, and PyArrow
pip install -r requirements.txt

# 4. Execute any notebook directly as a standard Python script
python notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.py
```

---

### 3. Convert to Jupyter `.ipynb` (Optional)
If you prefer viewing in Jupyter Lab or Jupyter Notebook:
```bash
python scripts/export_to_ipynb.py
```
This generates standard `.ipynb` files under the `jupyter_exports/` directory.

---

## 📦 Enterprise Production Deployment with Databricks Asset Bundles (DABs)

Modern enterprise lakehouses require robust, reproducible **Infrastructure as Code (IaC)**. Manual workspace notebook execution and UI-configured jobs inevitably cause configuration drift, broken paths, and untracked changes across development, staging, and production environments.

This repository natively integrates **Databricks Asset Bundles (DABs)** — the official declarative standard for building, deploying, and managing **all Databricks object types** from a single version-controlled codebase.

---

### 1. DAB Architecture & Continuous Delivery (CI/CD) Workflow

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DATABRICKS ASSET BUNDLES (DABs) DEPLOYMENT PIPELINE                                            │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

   Developer Machine (VS Code / CLI)              GitHub Remote (CI/CD)                  Databricks Lakehouse Workspaces
  ┌─────────────────────────────────┐       ┌───────────────────────────────┐       ┌──────────────────────────────────────────────┐
  │ • databricks.yml                │       │ 🛡️ PULL REQUEST:              │       │ 🧑‍💻 DEV TARGET:                             │
  │ • resources/jobs.yml            │ ────> │   └── databricks bundle       │ ────> │    • Workspace: /Users/${{user}}/.bundle/     │
  │ • resources/dlt_pipelines.yml   │       │       validate -t dev         │       │    • Isolated schemas & rapid iteration      │
  │ • resources/unity_catalog.yml   │       ├───────────────────────────────┤       ├──────────────────────────────────────────────┤
  │ • resources/model_serving.yml   │       │ 🧪 MERGE TO MAIN:             │       │ 🧪 STAGING TARGET:                           │
  │ • resources/apps.yml            │       │   └── databricks bundle       │ ────> │    • Workspace: /Shared/.bundle/staging      │
  │ • resources/dashboards.yml      │       │       deploy -t staging       │       │    • Automated integration & QA tests        │
  │ • notebooks/ & apps/            │       ├───────────────────────────────┤       ├──────────────────────────────────────────────┤
  └─────────────────────────────────┘       │ 🚀 PRODUCTION RELEASE:        │       │ 🚀 PROD TARGET:                              │
                                            │   └── databricks bundle       │ ────> │    • Workspace: /Shared/.bundle/prod         │
                                            │       deploy -t prod          │       │    • Non-human Service Principal (OAuth M2M) │
                                            └───────────────────────────────┘       │    • Live alerts & strict SLA execution      │
                                                                                    └──────────────────────────────────────────────┘
```

---

### 2. Full Matrix of Databricks Objects Deployed via This Codebase

Every major Databricks resource type is defined declaratively under [`resources/`](resources/) and root [`databricks.yml`](databricks.yml):

| Object Type | Resource Config | Description & Production Behavior |
|:---|:---|:---|
| ⚡ **Multi-Task Workflows & Jobs** | [`resources/jobs.yml`](resources/jobs.yml) | Multi-stage orchestration DAG: Auto Loader ingestion → JSON quarantine → SCD Type 2 dimension merge → Data skew salting join → Gold analytics rollup → Master pipeline audit. Features autoscaling job clusters (1..4 nodes), AQE tuning, retry policies, and email alerting. |
| 🧱 **Delta Live Tables (DLT) Pipelines** | [`resources/dlt_pipelines.yml`](resources/dlt_pipelines.yml) | Declarative medallion ETL pipeline implementing data quality expectations (`@dlt.expect`, `@dlt.expect_or_drop`), Photon engine acceleration, and continuous/triggered lakehouse ingestion. |
| 🏛️ **Unity Catalog Schemas & Volumes** | [`resources/unity_catalog.yml`](resources/unity_catalog.yml) | Provisions conformed schemas (`${{var.catalog}}.${{var.schema}}`) and managed volumes (`raw_landing` for landing files and `streaming_checkpoints` for state storage) across target environments. |
| 🤖 **Serverless Model Serving Endpoints** | [`resources/model_serving.yml`](resources/model_serving.yml) | Deploys scalable REST endpoints for Unity Catalog registered models (`credit_card_fraud_model`) with automatic scale-to-zero capabilities for cost efficiency. |
| 📱 **Databricks Apps (Streamlit / Dash)** | [`resources/apps.yml`](resources/apps.yml) | Deploys the internal Lakehouse Quality & Cost Monitor web application ([`apps/lakehouse_monitor/`](apps/lakehouse_monitor/)) directly onto Databricks serverless compute with workspace SSO. |
| 📊 **AI/BI Lakeview Dashboards** | [`resources/dashboards.yml`](resources/dashboards.yml) | Declarative executive analytics dashboard synchronized into workspace paths as version-controlled code. |

---

### 3. Step-by-Step Production Deployment Guide

#### Step 1: Install and Authenticate Databricks CLI
Install the modern Databricks CLI (v0.200.0+) and log into your workspace via OAuth:

```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Authenticate with your Databricks workspace
databricks auth login --host https://<your-workspace-instance>.cloud.databricks.com
```

#### Step 2: Validate Bundle Syntax & Configurations
Verify that all bundle variables, notebook dependencies, and YAML schemas are valid:

```bash
# Validate against development target
databricks bundle validate -t dev

# Validate against staging or production targets
databricks bundle validate -t staging
databricks bundle validate -t prod
```

#### Step 3: Deploy to Your Personal Development Sandbox
Deploy your workspace notebooks, jobs, pipelines, and apps into your isolated user sandbox without touching shared production assets:

```bash
databricks bundle deploy -t dev
```

> **How it works:** Under `mode: development`, DAB automatically prefixes your resources with `[dev ${{workspace.current_user.userName}}]`, isolates all files under `/Users/${{workspace.current_user.userName}}/.bundle/pyspark-handbook/dev`, and sets workflows to `PAUSED` so test jobs never fire unexpectedly.

#### Step 4: Run the Master Pipeline On-Demand
Trigger a test execution of the multi-task Lakehouse workflow directly from your terminal:

```bash
databricks bundle run pyspark_handbook_master_pipeline -t dev
```

#### Step 5: Inspect Deployed Resources or Teardown
```bash
# List all deployed resources in the target environment
databricks bundle summary -t dev

# Clean up all deployed development sandbox resources
databricks bundle destroy -t dev
```

---

### 4. Automated CI/CD with GitHub Actions

This repository includes a production-ready GitHub Actions workflow ([`.github/workflows/deploy_bundles.yml`](.github/workflows/deploy_bundles.yml)):
1. **Pull Requests:** Automatically executes `databricks bundle validate -t dev` on every PR affecting notebooks, resources, or configurations.
2. **Main Branch Merges:** Deploys assets directly to the **Staging** workspace (`databricks bundle deploy -t staging`).
3. **Release Promotion:** Deploys assets to **Production** (`databricks bundle deploy -t prod`) using dedicated **Service Principal credentials** (`DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`) with OAuth M2M authentication.

---

### 5. Enterprise Governance & Security Best Practices
- 🔒 **Service Principal Execution (`run_as`):** Never run production scheduled workflows under individual human user accounts. Production targets run strictly under a governed Databricks Service Principal.
- 🌐 **Catalog Isolation:** Development runs target `dev_catalog`, staging runs target `staging_catalog`, and production targets `prod_catalog`, preventing accidental data corruption.
- ⚙️ **Dynamic Variable Substitution:** Zero hardcoded URLs or database names. Everything leverages `${{var.catalog}}`, `${{var.schema}}`, and `${{bundle.target}}` for seamless multi-cloud, multi-workspace portability.
- 🧹 **Orphan Prevention:** Deleting an object in `resources/*.yml` and running `databricks bundle deploy` automatically deletes the corresponding remote workspace resource.

---

## 🎯 Senior Data Engineer Interview Matrix

The table below maps real-world production challenges and frequently asked Staff / Senior Data Engineer interview topics to the exact notebook implementations in this repository:

| Interview Topic & Challenge | Core Technical Problem | Architectural Solution | Reference Notebook |
|:---|:---|:---|:---:|
| **Severe Data Skew in Distributed Joins** | One partition holds 90% of data, straggler tasks trigger job timeouts. | Key salting, synthetic repartitioning, and two-stage skew aggregation. | [`076_data_skew_mitigation`](notebooks/08_performance_and_tuning/076_data_skew_detection_and_mitigation_using_salting_and_two_stage_joins.py) |
| **ACID Concurrency & Write Conflicts** | Concurrent batch updates and streaming writes cause `ConcurrentAppendException`. | Optimistic Concurrency Control (OCC), transaction log mechanics, and partition isolation. | [`034_acid_crud_operations`](notebooks/04_delta_lake_and_acid/034_acid_crud_operations_on_delta_insert_update_and_delete_mechanics.py) |
| **SCD Type 2 Historical Tracking** | Track point-in-time attribute changes without rewriting whole tables. | Two-stage Delta `MERGE INTO` with effective date intervals and `is_current` flags. | [`069_scd_type_2_tracking`](notebooks/07_lakehouse_modeling_scd/069_slowly_changing_dimension_type_2_scd_type_2_historical_tracking_with_effective_dates.py) |
| **Streaming Late-Arriving Data & Watermarks** | Real-time streams receive delayed events; unbounded memory growth. | Event-time watermarking with appropriate drop tolerances and window aggregations. | [`056_streaming_watermarking`](notebooks/06_streaming_realtime/056_streaming_window_aggregations_and_watermarking_for_late_arriving_events.py) |
| **Incremental File Ingestion at Scale** | S3/ADLS bucket with 1,000,000+ files causes file listing timeouts. | Databricks Auto Loader (`cloudFiles`) with RocksDB state and checkpointing. | [`048_auto_loader_cloudfiles`](notebooks/05_data_ingestion_autoloader/048_databricks_auto_loader_directory_mode_cloudfiles_schema_inference_and_checkpointing.py) |
| **Adaptive Query Execution (AQE) Dynamic Tuning** | Static Spark configurations fail to adapt to unpredictable runtime shuffle sizes. | AQE dynamic shuffle partition coalescing, join switching, and skew joins. | [`075_adaptive_query_execution`](notebooks/08_performance_and_tuning/075_adaptive_query_execution_aqe_dynamic_coalescing_join_switching_and_skew_joins.py) |
| **Fine-Grained Data Governance & Compliance** | Multi-tenant users require dynamic masking for PII without duplicate tables. | Unity Catalog Row Filters and Dynamic Column Masking functions. | [`089_dynamic_column_masking`](notebooks/09_unity_catalog_and_security/089_dynamic_column_masking_for_pii_protection_and_compliance.py) |
| **Delta Lake Small Files & Compaction** | Thousands of micro-batch commits degrade query planning and metadata I/O. | Bin-packing compaction, `OPTIMIZE`, `auto_compaction`, and `VACUUM`. | [`038_delta_optimize_compaction`](notebooks/04_delta_lake_and_acid/038_delta_optimize_file_compaction_and_auto_compaction_strategies.py) |

---

## 📚 Master Curriculum Catalog of 105 Use Cases

{all_catalog_md}

---

## 📂 Repository Structure

```
pyspark-handbook/
├── .github/
│   └── workflows/
│       ├── ci.yml                     # Automated GitHub Actions CI workflow
│       └── deploy_bundles.yml         # Databricks Asset Bundles (DAB) CI/CD deployment
├── apps/
│   └── lakehouse_monitor/             # Databricks App: Streamlit Lakehouse monitor
│       ├── app.py                     # Streamlit application entrypoint
│       ├── app.yaml                   # Databricks App definition manifest
│       └── requirements.txt           # App dependencies (streamlit, databricks-sdk)
├── notebooks/
│   ├── 01_core_dataframe_api/         # 001 - 012: Core DataFrame API, Schemas, Arrow Pandas UDFs, DBUtils
│   ├── 02_complex_data_types/         # 013 - 022: Arrays, Higher-Order Functions, Nested Structs, JSON, XML
│   ├── 03_sql_and_window_functions/   # 023 - 032: OLAP Cubes, Pivots, Window Functions, Joins, Catalyst SQL
│   ├── 04_delta_lake_and_acid/        # 033 - 043: Transaction Log, ACID CRUD, MERGE, Time Travel, Liquid Clustering
│   ├── 05_data_ingestion_autoloader/  # 044 - 053: Auto Loader, Rescued Data, Parallel JDBC, Parquet, Archives
│   ├── 06_streaming_realtime/         # 054 - 063: Structured Streaming, Triggers, Watermarks, Dual-Stream Joins, DLQ
│   ├── 07_lakehouse_modeling_scd/     # 064 - 073: Medallion Architecture, SCD 1/2/3/4, Debezium CDC, Data Vault 2.0
│   ├── 08_performance_and_tuning/     # 074 - 085: Catalyst Optimizer, AQE, Salting Skew, Bucketing, Photon, OOM Fixes
│   ├── 09_unity_catalog_and_security/ # 086 - 095: 3-Tier Namespace, Volumes, RLS, Column Masking, FinOps, DABs
│   └── 10_end_to_end_projects/        # 096 - 105: Finance Fraud, Omnichannel Retail, Healthcare, IoT, RAG, Alerts
├── resources/                         # Modular Databricks Asset Bundle definitions
│   ├── apps.yml                       # Databricks Apps resource specification
│   ├── dashboards.yml                 # Lakeview AI/BI dashboards specification
│   ├── dlt_pipelines.yml              # Delta Live Tables (DLT) declarative pipelines
│   ├── jobs.yml                       # Multi-task orchestration workflows & clusters
│   ├── model_serving.yml              # Serverless real-time model serving endpoints
│   └── unity_catalog.yml              # Unity Catalog schemas, volumes, and grants
├── scripts/
│   ├── notebook_builder.py            # Core engine for generating clean Databricks .py notebooks
│   ├── generate_all.py                # Master CLI tool to rebuild all 105 notebooks
│   ├── generate_readme.py             # Rebuilds the GitHub master catalog and documentation
│   ├── validate_notebooks.py          # Structural integrity, syntax, and Databricks compliance checker
│   └── export_to_ipynb.py             # Converts .py notebooks to Jupyter .ipynb format
├── tests/
│   ├── test_bundle_integrity.py       # Validates DAB configs, resources, and object paths
│   └── test_notebook_integrity.py     # Automated integrity test suite (count, format, no video traces)
├── databricks.yml                     # Root Databricks Asset Bundle (DAB) configuration
├── .gitignore
├── LICENSE                            # MIT Open Source License
├── pyproject.toml
├── requirements.txt                   # Production and local PySpark dependencies
└── README.md                          # Master documentation and use case catalog
```

---

## ⚡ Quick Cheat Sheet & TL;DR

### 1. Salting Skewed Keys in PySpark
```python
import pyspark.sql.functions as F

SALT_BUCKETS = 8

# Step 1: Add salt to skewed DataFrame
df_salted = df_large.withColumn("salt", F.concat(F.col("skewed_key"), F.lit("_"), F.floor(F.rand() * SALT_BUCKETS)))

# Step 2: Replicate the lookup dimension table to match salt keys
df_lookup_replicated = df_small.withColumn("salt_array", F.array([F.lit(i) for i in range(SALT_BUCKETS)])) \\
                               .withColumn("salt_id", F.explode("salt_array")) \\
                               .withColumn("salt", F.concat(F.col("join_key"), F.lit("_"), F.col("salt_id")))

# Step 3: Perform balanced join
df_result = df_salted.join(df_lookup_replicated, on="salt", how="inner")
```

### 2. Enabling Adaptive Query Execution (AQE) Best Practices
```python
# Essential production configurations for Apache Spark 3.5+
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "67108864")  # 64MB
```

---

## 🧪 Automated Testing & Continuous Integration

This repository enforces strict code quality and structural contracts on every commit via GitHub Actions:
- **Notebook File Contract:** Exactly 105 `.py` Databricks notebooks must exist.
- **Databricks Formatting:** Every notebook begins with `# Databricks notebook source` without empty prepended cells, uses valid `# COMMAND ----------` delimiters, and includes `# MAGIC %md` documentation.
- **Zero Video / Extraneous References:** Verified completely free of video timestamps, video links, or external author references.
- **AST Syntax Check:** All code blocks pass standard Python AST validation.

To run the verification suite locally:
```bash
# Validate notebook format and AST compliance
python scripts/validate_notebooks.py

# Run integrity tests
python tests/test_notebook_integrity.py
```

---

## 🤝 Contributing

Contributions, feedback, and optimization recipes are warmly welcomed!
1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/awesome-pyspark-recipe`)
3. Commit your Changes (`git commit -m 'Add SCD2 PySpark implementation'`)
4. Push to the Branch (`git push origin feature/awesome-pyspark-recipe`)
5. Open a Pull Request

---

## ⭐ Star History

If this handbook helped you ace an interview or optimize a production lakehouse pipeline, **please give it a star!** ⭐

[![Star History Chart](https://api.star-history.com/svg?repos=santoshjammi29/pyspark-handbook&type=Date)](https://star-history.com/#santoshjammi29/pyspark-handbook&Date)

---

## 📜 License & Acknowledgments

This project is licensed under the **[MIT License](LICENSE)** — feel free to use it for personal study, enterprise lakehouse development, internal team training, and technical interview preparation.

<div align="center">

Maintained with ❤️ by [Santosh Jammi](https://github.com/santoshjammi29)

</div>
"""

    with open(base_dir / "README.md", "w", encoding="utf-8") as f:
        f.write(readme_content)

    print(f"Successfully generated master README.md with {total_count} cataloged notebooks!")

if __name__ == "__main__":
    generate_readme(Path(__file__).parent.parent)
