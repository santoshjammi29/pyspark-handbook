"""
Master README Generator for The PySpark Handbook
Scans all 105 Databricks Python notebooks (.py) and builds enterprise-grade,
clean, GitHub-native documentation with Mermaid diagrams, interactive shell instructions,
whiteboard blueprints, deep-dive architectures, production incident runbooks,
4-tier interview guides, and sister repository cross-links.
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
        return "🟢<br>Beginner"
    elif "advanced" in d:
        return "🔴<br>Advanced"
    elif "principal" in d or "architect" in d or "capstone" in d:
        return "🟣<br>Principal"
    return "🟡<br>Intermediate"

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
            "level": "🟢<br>Beginner",
            "difficulty": "🟢 Beginner – 🟡 Intermediate",
            "coverage": "DataFrame creation, explicit StructType schemas, PyArrow Pandas UDFs, safe type casting, column projections, and DBUtils emulation."
        },
        {
            "folder": "02_complex_data_types",
            "num": "02",
            "title": "Module 02: Complex Data Types & Nested Structures",
            "range": "013 – 022",
            "icon": "🧩",
            "level": "🟡<br>Intermediate",
            "difficulty": "🟡 Intermediate",
            "coverage": "Nested StructTypes, Arrays, Maps, Higher-Order Functions (transform, filter, aggregate), JSON parsing, XML shredding, and schema evolution."
        },
        {
            "folder": "03_sql_and_window_functions",
            "num": "03",
            "title": "Module 03: Advanced SQL & Analytical Window Functions",
            "range": "023 – 032",
            "icon": "🪟",
            "level": "🟡<br>Intermediate",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "Ranking, lead/lag, rolling moving averages, cumulative distribution, OLAP cubes, pivots, broadcast hash joins, and range vs rows framing."
        },
        {
            "folder": "04_delta_lake_and_acid",
            "num": "04",
            "title": "Module 04: Delta Lake Architecture & ACID Operations",
            "range": "033 – 043",
            "icon": "🧱",
            "level": "🔴<br>Advanced",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "Transaction Log internals (_delta_log), ACID CRUD, MERGE INTO optimizations, Time Travel, VACUUM safety, and Deletion Vectors."
        },
        {
            "folder": "05_data_ingestion_autoloader",
            "num": "05",
            "title": "Module 05: Data Ingestion, Auto Loader & Cloud Formats",
            "range": "044 – 053",
            "icon": "📥",
            "level": "🟡<br>Intermediate",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "Databricks Auto Loader (cloudFiles), schema inference, _rescued_data poison pill quarantine, Parquet, Avro, and parallel JDBC ingestion."
        },
        {
            "folder": "06_streaming_realtime",
            "num": "06",
            "title": "Module 06: Structured Streaming & Near Real-Time Processing",
            "range": "054 – 063",
            "icon": "⚡",
            "level": "🔴<br>Advanced",
            "difficulty": "🔴 Advanced",
            "coverage": "Streaming micro-batches, AvailableNow triggers, event-time watermarking, stateful window aggregations, stream-stream joins, and DLQ sinks."
        },
        {
            "folder": "07_lakehouse_modeling_scd",
            "num": "07",
            "title": "Module 07: Lakehouse Data Modeling, CDC & SCD Implementation",
            "range": "064 – 073",
            "icon": "🏛️",
            "level": "🔴<br>Advanced",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "Medallion Architecture (Bronze/Silver/Gold), Slowly Changing Dimensions (SCD Types 1, 2, 3, 4), Debezium CDC replay, and Data Vault 2.0."
        },
        {
            "folder": "08_performance_and_tuning",
            "num": "08",
            "title": "Module 08: Performance Tuning, AQE & Skew Mitigation",
            "range": "074 – 085",
            "icon": "🏎️",
            "level": "🔴<br>Advanced",
            "difficulty": "🔴 Advanced",
            "coverage": "Catalyst Logical & Physical Plans, Adaptive Query Execution (AQE), Salting join skew, Hive-style bucketing, OOM diagnosis, and Photon."
        },
        {
            "folder": "09_unity_catalog_and_security",
            "num": "09",
            "title": "Module 09: Unity Catalog Governance, Security & FinOps",
            "range": "086 – 095",
            "icon": "🛡️",
            "level": "🔴<br>Advanced",
            "difficulty": "🟡 Intermediate – 🔴 Advanced",
            "coverage": "Unity Catalog 3-tier namespace, Managed & External Volumes, Row-Level Security, Dynamic Column Masking, Secret Scopes, and DABs."
        },
        {
            "folder": "10_end_to_end_projects",
            "num": "10",
            "title": "Module 10: End-to-End Enterprise Production Projects",
            "range": "096 – 105",
            "icon": "🚀",
            "level": "🟣<br>Principal",
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
            f"| {mod['icon']} **[{mod['title']}](#{anchor})** | `{mod['range']}` | {mod['level']} | {mod['coverage']} |"
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

| Recipe | Notebook Title | Level | Real-World Scenario | Spark Architecture Key Concept |
| :---: | :--- | :---: | :--- | :--- |
{table_body}

[⬆️ Back to Curriculum Overview](#-curriculum-overview)
</details>"""
        details_md_list.append(details_md)

    collapsible_catalog_md = "\n\n".join(details_md_list)

    readme_content = f"""# ⚡ The PySpark Handbook

**105 Production Coding Use Cases, Performance Tuning Recipes, Lakehouse Masterclass & Incident Runbooks**

[![CI Validation](https://github.com/santoshjammi29/pyspark-handbook/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshjammi29/pyspark-handbook/actions)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5%2B_--_4.0-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Runtime_15.4%2B_LTS-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2_ACID-00ADD8?style=flat-square&logo=delta&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.9_--_3.12-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![DABs](https://img.shields.io/badge/Deploy-Databricks_Asset_Bundles-008080?style=flat-square&logo=databricks&logoColor=white)](databricks.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)

*The definitive enterprise reference repository of 105 battle-tested Apache Spark & Databricks Lakehouse coding use cases, architectural blueprints, disaster recovery incident runbooks, and 4-tier technical interview preparation guide.*  
*Structured in the official **Databricks Source Notebook format (`.py`)** for seamless 1-click sync with **Databricks Repos / Git Folders**, **Databricks Asset Bundles (DABs)**, **VS Code / Databricks Connect v2**, and CI/CD runtimes.*

[Curriculum Overview](#-curriculum-overview) • [Lakehouse Architecture](#-lakehouse-architecture) • [Whiteboard Blueprints](#-whiteboard-blueprints) • [Interactive CLI](#-interactive-cli-explorer) • [Quickstart](#-quickstart) • [Architectures](#️-architectural-deep-dives) • [Runbooks](#-production-incident-runbooks) • [Interview Matrix](#-4-tier-interview-preparation-matrix) • [Sister Repositories](#-sister-repositories--knowledge-base)

---

## 🎯 Overview & Mission

Modern Lakehouse engineering demands deep fluency in distributed memory execution, ACID transaction semantics, streaming ingestion, query plan optimization, and enterprise data governance.

This repository provides **105 end-to-end, runnable, modular use cases** covering:
- **Core & Advanced Distributed Compute:** PyArrow Pandas UDFs, Higher-Order Functions, Nested Structs/Arrays, Windowing, and Catalyst internals.
- **Lakehouse Storage & ACID Transactions:** Delta Lake transaction log, MERGE INTO, Time Travel, VACUUM, and Liquid Clustering (`CLUSTER BY`).
- **Streaming & Ingestion:** Databricks Auto Loader (`cloudFiles`), event-time watermarking, stream-stream joins, and Dead Letter Queues (DLQ).
- **Enterprise Governance & DevOps:** Unity Catalog 3-tier namespace, Row Filters, Dynamic Column Masking, and Databricks Asset Bundles (DABs) IaC.
- **Operational Triage & Reliability:** 6 production incident runbooks covering Driver/Executor OOMs, Shuffle Disk Spill, Skew Salting, State Store Leaks, and PyArrow serialization crashes.

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
    Bronze -->|"Delta MERGE / Streaming<br/>Watermark"| Silver
    Silver -->|"Aggregations / Z-Order /<br/>Liquid Clustering"| Gold
```

---

## 🖼️ Whiteboard Blueprints

The repository includes **52 publication-grade architectural whiteboard blueprints** mirrored under `assets/whiteboards/` and cataloged in [docs/whiteboard_diagrams/README.md](docs/whiteboard_diagrams/README.md), illustrating:
- **Catalyst Optimizer & Photon Engine** execution pipelines
- **Spark Unified Memory Pool** (Execution vs Storage dynamic borrowing)
- **Shuffle Skew & Two-Phase Salting** distribution flows
- **Structured Streaming RocksDB** state store and watermark lifecycles
- **Delta Lake 3.x ACID** transaction log mechanics and Liquid Clustering

---

## 💻 Interactive CLI Explorer

Explore recipes, search code patterns, inspect incident runbooks, and study interview guides directly from your terminal:

```bash
# Search by recipe number (e.g. Recipe 025: Window Functions)
python scripts/interactive_shell.py 025

# Search by topic or concept keyword
python scripts/interactive_shell.py "pandas udf"
python scripts/interactive_shell.py "two phase salting"

# List all available recipes, runbooks, and interview tiers
python scripts/interactive_shell.py --list
python scripts/interactive_shell.py --runbooks
python scripts/interactive_shell.py --architectures
python scripts/interactive_shell.py --interview
```

---

## 🚀 Quickstart

### Prerequisites
- Python 3.9, 3.10, 3.11, or 3.12
- PySpark 3.5.0+ (`pip install -r requirements.txt`)
- Java 11 or 17 (required for local Spark execution)

### 1. Clone the Repository
```bash
git clone https://github.com/santoshjammi29/pyspark-handbook.git
cd pyspark-handbook
```

### 2. Local Environment Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run Validation Tests
```bash
python scripts/run_tests.py
```

---

## 🗺️ Curriculum Overview

The 105 recipes progress systematically across **4 difficulty tiers**:
- **Level 100 (Beginner):** Core DataFrame transformations, StructType schemas, basic actions, file I/O.
- **Level 200 (Intermediate):** Complex types, explode, analytical window functions, Auto Loader ingestion.
- **Level 300 (Advanced):** Catalyst plan inspection, AQE skew join optimization, stateful streaming watermarks.
- **Level 400 (Principal/Architect):** Memory sizing calculations, disaster recovery, end-to-end multi-tenant capstones.

| Module | Recipes | Difficulty | Core Topics & Competencies |
| :--- | :---: | :---: | :--- |
{nav_table_md}

---

## 📚 Complete 105 Recipe Catalog

{collapsible_catalog_md}

---

## 🏛️ Architectural Deep Dives

Four production-tested architectural specifications located under `architectures/`:

| Spec ID | Document Title | Target Level | Core Architectural Focus |
| :---: | :--- | :---: | :--- |
| **01** | **[The Catalyst Optimizer & Project Tungsten](architectures/01_catalyst_optimizer_and_tungsten_engine.md)** | `Level 300-400` | Query optimization pipeline, UnsafeRow binary format, Whole-Stage Code Generation, and CPU register-level execution. |
| **02** | **[Unified Memory Management & Spill Mechanics](architectures/02_spark_unified_memory_management_and_spill_mechanics.md)** | `Level 300-400` | JVM Heap partitioning, Execution vs Storage dynamic borrowing, Tungsten disk spill math, and G1GC configuration. |
| **03** | **[Adaptive Query Execution (AQE) Deep Dive](architectures/03_adaptive_query_execution_aqe_deep_dive.md)** | `Level 300-400` | Dynamic partition coalescing, runtime join switching to BHJ, and automatic partition splitting for skewed joins. |
| **04** | **[Structured Streaming & Stateful Stream Processing](architectures/04_structured_streaming_and_stateful_stream_processing.md)** | `Level 300-400` | Micro-batch vs continuous processing, event-time watermarking, RocksDB off-heap state store, and WAL exactly-once semantics. |

---

## 🚨 Production Incident Runbooks

Six operational disaster recovery runbooks located under `runbooks/`:

| Runbook ID | Title & Direct Link | Severity | Root Cause & Remediation Focus |
| :---: | :--- | :---: | :--- |
| **01** | **[Driver & Executor OOM Triage](runbooks/01_driver_and_executor_oom_triage.md)** | `SEV-1` | Resolve `java.lang.OutOfMemoryError`, container killed by YARN/K8s (Exit code 137), `.collect()` traps, and overhead sizing. |
| **02** | **[Tungsten Shuffle Spill & Partition Sizing](runbooks/02_shuffle_spill_and_partition_sizing.md)** | `SEV-2` | Eliminate massive `Spill (Disk)` using dynamic partition calculation (150MB target rule) and AQE partition coalescing. |
| **03** | **[Data Skew & Two-Phase Salting](runbooks/03_data_skew_and_two_phase_salting.md)** | `SEV-1` | Triage single straggler tasks stuck at 99%, null key accumulation, and implement two-phase key salting with exploded lookups. |
| **04** | **[Streaming State Store Leaks & Watermark Drops](runbooks/04_streaming_state_store_and_watermark_drops.md)** | `SEV-1` | Stop state store memory leaks via event-time watermarking, switch to RocksDB, and recover from checkpoint corruption. |
| **05** | **[Delta ACID Concurrency & Metadata Triage](runbooks/05_delta_acid_concurrency_and_metadata_bottlenecks.md)** | `SEV-2` | Resolve `ConcurrentModificationException`, eliminate small file latency via `OPTIMIZE` and Liquid Clustering, and tune `VACUUM`. |
| **06** | **[PyArrow Serialization & Worker Crashes](runbooks/06_pandas_udf_arrow_serialization_and_gc_pauses.md)** | `SEV-2` | Fix Python daemon crashes, tune `spark.sql.execution.arrow.maxRecordsPerBatch`, and eliminate JVM GC overhead. |

---

## 🎓 4-Tier Interview Preparation Matrix

Comprehensive technical interview guides and flashcard deck located under `interview/`:

| Tier | Role Target | Core Focus Areas | Direct Link |
| :---: | :--- | :--- | :---: |
| **Tier 1** | **Junior & Associate PySpark Engineer** | SparkSession, DataFrame API, Lazy Evaluation, Narrow vs Wide Dependencies, Basic Aggregations, File Formats. | [`01_junior.md`](interview/01_junior_and_associate_pyspark_engineer.md) |
| **Tier 2** | **Mid-Level Data Engineer** | Complex Types, Higher-Order Functions, Analytical Windows, PyArrow Pandas UDFs, Delta Lake ACID, Auto Loader. | [`02_mid_level.md`](interview/02_mid_level_data_engineer.md) |
| **Tier 3** | **Senior Distributed Systems Engineer** | Catalyst Optimizer, Project Tungsten, Memory Pools & Spill Triage, AQE Skew Joins, Two-Phase Salting, Stateful Streaming. | [`03_senior.md`](interview/03_senior_distributed_systems_engineer.md) |
| **Tier 4** | **Staff & Principal Spark Architect** | Cluster Sizing Formulas, Multi-Tenant Fair Queues, Geo-DR, Zero-Copy Lakehouse Federation, FinOps Compute Right-Sizing. | [`04_staff.md`](interview/04_staff_and_principal_spark_architect.md) |
| **Deck** | **50 Rapid-Fire Flashcards** | 50 rapid-fire questions and answers across all 10 modules of the PySpark Handbook curriculum. | [`05_flashcards.md`](interview/05_flashcards_deck_50.md) |

---

## 🌐 Ecosystem Interlinking & Sister Repositories

The Modern Data Engineering ecosystem is intentionally partitioned into specialized, tightly coupled reference repositories anchored by **[Modern Data Engineer Platform (`moderndataengineer.uk`)](https://github.com/santoshjammi29/moderndataengineer)**. Use this cross-reference matrix to navigate directly between complementary implementations across our repositories:

| Core Engineering Focus | Flagship Repository | Direct Deep-Link Topics & Implementations in Sister Repositories |
| :--- | :--- | :--- |
| **Apache Spark & PySpark Core** | **[The PySpark Handbook](https://github.com/santoshjammi29/pyspark-handbook)** | • **Delta Lake ACID & Liquid Clustering**: [Databricks Handbook Mod 03 & 04](https://github.com/santoshjammi29/databricks-handbook)<br>• **SQL Windowing Equivalents**: [SQL Handbook Mod 04](https://github.com/santoshjammi29/sql-handbook)<br>• **Azure VNet Injection & Fabric Spark**: [Azure Cheat Sheet](https://github.com/santoshjammi29/azure-cheat-sheet) |
| **Databricks Data Intelligence Platform** | **[The Databricks Handbook](https://github.com/santoshjammi29/databricks-handbook)** | • **Core PyArrow UDFs & Higher-Order Functions**: [PySpark Handbook Mod 01 & 02](https://github.com/santoshjammi29/pyspark-handbook)<br>• **Databricks SQL vs ANSI Rosetta Stone**: [SQL Handbook Runbook 05](https://github.com/santoshjammi29/sql-handbook)<br>• **Azure Databricks Landing Zones & Bicep IaC**: [Azure Cheat Sheet](https://github.com/santoshjammi29/azure-cheat-sheet) |
| **Relational & Cloud MPP SQL** | **[The SQL Handbook](https://github.com/santoshjammi29/sql-handbook)** | • **Distributed Data Movement & Skew Salting**: [PySpark Runbook 03](https://github.com/santoshjammi29/pyspark-handbook)<br>• **Serverless SQL Warehouse Spill Tuning**: [Databricks Runbook 05](https://github.com/santoshjammi29/databricks-handbook)<br>• **Synapse & Fabric OneLake Direct Lake SQL**: [Azure Architecture 01](https://github.com/santoshjammi29/azure-cheat-sheet) |
| **Enterprise Cloud Architecture & Fabric** | **[Azure Enterprise Handbook](https://github.com/santoshjammi29/azure-cheat-sheet)** | • **Databricks Asset Bundles (DABs) & Lakeflow**: [Databricks Handbook Mod 07](https://github.com/santoshjammi29/databricks-handbook)<br>• **Structured Streaming & RocksDB State**: [PySpark Handbook Mod 06](https://github.com/santoshjammi29/pyspark-handbook)<br>• **Cloud Warehouse Cost Hardening**: [SQL Handbook Runbook 06](https://github.com/santoshjammi29/sql-handbook) |
| **Complete 11,950+ Verified Encyclopedia** | **[Modern Data Engineer Hub](https://github.com/santoshjammi29/moderndataengineer)** | • **Interactive Execution Simulators & 52 Blueprints**: [Live Web Platform (`moderndataengineer.uk`)](https://moderndataengineer.uk)<br>• **Google-Grade BM25F Search & Virtual DOM**: [Architecture Spec](https://github.com/santoshjammi29/moderndataengineer/blob/main/docs/STANDARDIZED_ARCHITECTURE.md) |

---

## 📜 License

Distributed under the **[MIT License](LICENSE)** — feel free to use it for personal study, enterprise lakehouse development, internal team training, and technical interview preparation.

Maintained with ❤️ by [Santosh Jammi](https://github.com/santoshjammi29) (2026)
"""

    with open(base_dir / "README.md", "w", encoding="utf-8") as f:
        f.write(readme_content)

    print(f"Successfully generated clean GitHub-themed README.md with {total_notebooks} cataloged notebooks!")

if __name__ == "__main__":
    generate_readme(Path(__file__).parent.parent)
