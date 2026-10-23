"""
Master README Generator
Scans all 105 Databricks Python notebooks (.py) and builds an enterprise-grade catalog and documentation.
"""

from pathlib import Path

def generate_readme(base_dir: Path):
    nb_dir = base_dir / "notebooks"
    module_folders = sorted([d for d in nb_dir.iterdir() if d.is_dir()])

    module_titles = {
        "01_core_dataframe_api": "Module 01: Core PySpark Foundations & DataFrame API (001 - 012)",
        "02_complex_data_types": "Module 02: Complex Data Types & Nested Structures Handling (013 - 022)",
        "03_sql_and_window_functions": "Module 03: Advanced SQL, Aggregations & Analytical Window Functions (023 - 032)",
        "04_delta_lake_and_acid": "Module 04: Delta Lake Architecture, ACID Operations & Time Travel (033 - 043)",
        "05_data_ingestion_autoloader": "Module 05: Data Ingestion Patterns, Auto Loader & File Formats (044 - 053)",
        "06_streaming_realtime": "Module 06: Streaming & Near Real-Time Processing (054 - 063)",
        "07_lakehouse_modeling_scd": "Module 07: Lakehouse Data Modeling, CDC & SCD Implementation (064 - 073)",
        "08_performance_and_tuning": "Module 08: Performance Tuning, Query Optimization & Skew Mitigation (074 - 085)",
        "09_unity_catalog_and_security": "Module 09: Databricks Platform, Unity Catalog, Security & FinOps (086 - 095)",
        "10_end_to_end_projects": "Module 10: End-to-End Enterprise Projects & Production Workflows (096 - 105)"
    }

    catalog_md = []
    total_count = 0

    for mod_dir in module_folders:
        mod_key = mod_dir.name
        title = module_titles.get(mod_key, mod_key)
        catalog_md.append(f"\n### {title}\n")
        catalog_md.append("| # | Databricks Notebook | Difficulty | Business Scenario & Core Concept |")
        catalog_md.append("|---|---|---|---|")

        nbs = sorted(list(mod_dir.glob("*.py")))
        for nb_path in nbs:
            total_count += 1
            rel_path = f"notebooks/{mod_key}/{nb_path.name}"
            content = nb_path.read_text(encoding="utf-8")

            # Extract fields
            nb_id = nb_path.stem[:3]
            clean_title = nb_path.stem[4:].replace("_", " ").title()

            difficulty = "Intermediate"
            if "Difficulty:** `Beginner`" in content or "`Beginner`" in content:
                difficulty = "Beginner"
            elif "Difficulty:** `Advanced`" in content or "`Advanced`" in content:
                difficulty = "Advanced"

            # Extract business scenario snippet
            scenario = "Enterprise PySpark implementation and architectural use case."
            if "## 1. Business Scenario & Real-World Context" in content:
                parts = content.split("## 1. Business Scenario & Real-World Context")
                if len(parts) > 1:
                    scenario_part = parts[1].split("## 2. Technical Requirement")[0].strip()
                    # Clean magic prefixes
                    cleaned_lines = [
                        line.replace("# MAGIC", "").strip()
                        for line in scenario_part.splitlines()
                        if line.replace("# MAGIC", "").strip()
                    ]
                    scenario = " ".join(cleaned_lines)
                    if len(scenario) > 115:
                        scenario = scenario[:112] + "..."

            if difficulty == "Beginner":
                diff_badge = "🟢 `Beginner`"
            elif difficulty == "Intermediate":
                diff_badge = "🟡 `Intermediate`"
            else:
                diff_badge = "🔴 `Advanced`"

            catalog_md.append(f"| {nb_id} | [`{nb_path.name}`]({rel_path}) | {diff_badge} | {scenario} |")

    catalog_section = "\n".join(catalog_md)

    readme_content = f"""# ⚡ Apache Spark & Databricks Lakehouse: 105 Coding Use Cases Masterclass

[![CI Notebook Validation](https://github.com/santoshjammi29/spark-coding-usecases-masterclass/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshjammi29/spark-coding-usecases-masterclass/actions)
[![Databricks](https://img.shields.io/badge/Databricks-Native_.py_Notebooks-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4%2B-FDEE21?logo=apachespark&logoColor=black)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-008080?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Notebook Count](https://img.shields.io/badge/Notebooks-105_Complete-success.svg)](#-master-catalog-of-105-use-cases)

Welcome to the **Spark & Databricks Coding Use Cases Masterclass**! This repository is a production-grade enterprise library of **105 native Databricks Python notebooks (`.py`)**, covering essential data engineering patterns, optimization strategies, and architectural designs across Apache Spark, Delta Lake, and the Databricks Lakehouse platform.

Every notebook is structured in the official **Databricks Source Notebook format** (`# Databricks notebook source` and `# COMMAND ----------` cell delimiters) so it syncs directly into **Databricks Repos / Git Folders**, **Databricks Asset Bundles (DABs)**, **VS Code**, or standard Python runtimes.

---

## 🏗️ Curriculum Architecture Overview

The 105 use cases are organized across 10 progressive, enterprise modules:

```
spark-coding-usecases-masterclass/
├── .github/
│   └── workflows/
│       └── ci.yml                 # Automated CI running test suite & notebook validation
├── notebooks/
│   ├── 01_core_dataframe_api/     # 001 - 012: Core DataFrame API, Schemas, Arrow Pandas UDFs, DBUtils
│   ├── 02_complex_data_types/     # 013 - 022: Arrays, Higher-Order Functions, Nested Structs, JSON, XML
│   ├── 03_sql_and_window_functions/# 023 - 032: OLAP Cubes, Pivots, Window Functions, Joins, Catalyst SQL
│   ├── 04_delta_lake_and_acid/    # 033 - 043: Transaction Log, ACID CRUD, MERGE, Time Travel, Liquid Clustering
│   ├── 05_data_ingestion_autoloader/# 044 - 053: Auto Loader, Rescued Data, Parallel JDBC, Parquet, Archives
│   ├── 06_streaming_realtime/     # 054 - 063: Structured Streaming, Triggers, Watermarks, Dual-Stream Joins, DLQ
│   ├── 07_lakehouse_modeling_scd/ # 064 - 073: Medallion Architecture, SCD 1/2/3/4, Debezium CDC, Data Vault 2.0
│   ├── 08_performance_and_tuning/ # 074 - 085: Catalyst Optimizer, AQE, Salting Skew, Bucketing, Photon, OOM Fixes
│   ├── 09_unity_catalog_and_security/# 086 - 095: 3-Tier Namespace, Volumes, RLS, Column Masking, FinOps, DABs
│   └── 10_end_to_end_projects/    # 096 - 105: Finance Fraud, Omnichannel Retail, Healthcare, IoT, RAG, Alerts
├── scripts/
│   ├── generate_all.py            # Regenerates all 105 notebooks programmatically
│   ├── validate_notebooks.py      # Verifies Databricks source structure, commands & syntax
│   ├── export_to_ipynb.py         # Converts .py notebooks to Jupyter .ipynb format
│   └── generate_readme.py         # Rebuilds master catalog index
├── tests/
│   └── test_notebook_integrity.py # Pytest test suite asserting notebook contracts
├── .gitignore
├── LICENSE                        # MIT License
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 🚀 Deployment & Usage

### 1. Direct Sync into Databricks Repos / Git Folders (Recommended)
Because all notebooks are native Databricks `.py` source files, they sync seamlessly with Databricks Workspaces:
1. Open your **Databricks Workspace**.
2. Click **Workspace** > **Repos** (or **Git Folders**).
3. Click **Add Repo** and paste:
   ```
   https://github.com/santoshjammi29/spark-coding-usecases-masterclass.git
   ```
4. All 105 notebooks will immediately render with native Databricks cells, markdown headings, widgets, and interactive execution!

### 2. Run Locally in VS Code or Terminal
```bash
# Clone the repository
git clone https://github.com/santoshjammi29/spark-coding-usecases-masterclass.git
cd spark-coding-usecases-masterclass

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run any notebook directly as a Python script
python notebooks/01_core_dataframe_api/001_sparksession_initialization_and_dataframe_creation_strategies.py
```

### 3. Convert to Jupyter `.ipynb` (Optional)
If you prefer viewing in Jupyter Lab:
```bash
python scripts/export_to_ipynb.py
```
This generates standard `.ipynb` files under `jupyter_exports/`.

---

## 📚 Master Catalog of 105 Use Cases

{catalog_section}

---

## 🧪 Testing & Validation

All 105 notebooks are verified by automated tests:
```bash
# Validate Databricks source structure and Python AST syntax
python scripts/validate_notebooks.py

# Run test suite
pytest tests/
```

---

## 🤝 License
This repository is open-sourced under the [MIT License](LICENSE).
"""

    with open(base_dir / "README.md", "w", encoding="utf-8") as f:
        f.write(readme_content)

    print(f"Generated clean README.md with {total_count} cataloged notebooks!")

if __name__ == "__main__":
    generate_readme(Path(__file__).parent.parent)
