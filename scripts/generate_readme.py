"""
Master README Generator
Scans all 105 notebooks and builds a production-grade catalog and documentation.
"""

import json
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
        catalog_md.append("| # | Notebook | Difficulty | Business Scenario & Core Concept |")
        catalog_md.append("|---|---|---|---|")

        nbs = sorted(list(mod_dir.glob("*.ipynb")))
        for nb_path in nbs:
            total_count += 1
            rel_path = f"notebooks/{mod_key}/{nb_path.name}"
            with open(nb_path, "r", encoding="utf-8") as f:
                d = json.load(f)

            # Extract header text from first cell
            first_cell = "".join(d["cells"][0]["source"])
            
            # Extract fields
            nb_id = nb_path.name[:3]
            clean_title = nb_path.stem[4:].replace("_", " ").title()
            
            difficulty = "Intermediate"
            if "**Difficulty:** `Beginner`" in first_cell or "`Beginner`" in first_cell:
                difficulty = "Beginner"
            elif "**Difficulty:** `Advanced`" in first_cell or "`Advanced`" in first_cell:
                difficulty = "Advanced"

            # Extract business scenario snippet
            scenario = "Enterprise PySpark implementation and architectural use case."
            if "## 1. Business Scenario & Real-World Context" in first_cell:
                parts = first_cell.split("## 1. Business Scenario & Real-World Context")
                if len(parts) > 1:
                    scenario_part = parts[1].split("## 2. Technical Requirement")[0].strip()
                    scenario = scenario_part.replace("\n", " ")
                    if len(scenario) > 110:
                        scenario = scenario[:107] + "..."

            diff_badge = f"`{difficulty}`"
            if difficulty == "Beginner":
                diff_badge = "🟢 `Beginner`"
            elif difficulty == "Intermediate":
                diff_badge = "🟡 `Intermediate`"
            else:
                diff_badge = "🔴 `Advanced`"

            catalog_md.append(f"| {nb_id} | [{clean_title}]({rel_path}) | {diff_badge} | {scenario} |")

    catalog_section = "\n".join(catalog_md)

    readme_content = f"""# ⚡ Apache Spark & Databricks Lakehouse: 105 Coding Use Cases Masterclass

[![CI Notebook Validation](https://github.com/santoshjammi29/spark-coding-usecases-masterclass/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshjammi29/spark-coding-usecases-masterclass/actions)
[![PySpark](https://img.shields.io/badge/PySpark-3.4%2B-FDEE21?logo=apachespark&logoColor=black)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Databricks Unity Catalog](https://img.shields.io/badge/Databricks-Unity_Catalog-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Notebook Count](https://img.shields.io/badge/Notebooks-105_Complete-success.svg)](#-master-catalog-of-105-use-cases)

Welcome to the **Spark Coding Use Cases Masterclass**! This repository is a comprehensive, production-grade collection of **105 fully executable Jupyter Notebooks**, each solving a concrete real-world engineering challenge across Apache Spark, PySpark, Delta Lake, and the Databricks Lakehouse platform.

Every single notebook is **100% self-contained**—with realistic synthetic mock datasets, architectural explanations, production best practices, error-handling patterns, and senior data engineering interview questions.

---

## 📺 Masterclass Reference Alignment
This curriculum directly expands and codifies the architectural principles from the **[Databricks All in One Free Masterclass Playlist](https://www.youtube.com/playlist?list=PLlHuhl-cAAnH30NbAzagvT4FXSWu94uQS)** by *DataBeli*, mapping each video concept to hands-on notebooks:

| Masterclass Playlist Theme | Videos | Covered In Modules |
|---|---|---|
| **Databricks Workspaces, UI & Magic Commands** | Videos 02, 04, 06 | Module 01 (Notebooks 001 - 012) |
| **Orchestration, %run vs dbutils.notebook.run** | Video 07, 10 | Module 01, 10 (Notebooks 012, 105) |
| **Secret Scopes & DBUtils File Management** | Videos 08, 09 | Module 01, 09 (Notebooks 012, 087, 091) |
| **Auto Loader (cloudFiles) & Ingestion Engine** | Video 11 | Module 05 (Notebooks 048, 049, 050) |
| **Debugging, Execution Plans & Performance Tuning** | Video 12, 13 | Module 08 (Notebooks 074 - 085) |
| **Cluster Architecture, Photon & Sizing** | Video 13, 29 | Module 08 (Notebooks 081, 084) |
| **Unity Catalog Governance (3-Tier, Volumes, ACLs)** | Video 03, 14 | Module 09 (Notebooks 086 - 089) |
| **FinOps & Job Cost Calculation with SQL** | Video 15 | Module 09 (Notebook 092) |
| **LakeFlow Connect & CDC Ingestion** | Video 16 | Module 05, 07 (Notebooks 047, 071) |
| **Lakeflow Declarative Pipelines & DLT Expectations** | Video 17 | Module 09 (Notebook 093) |
| **Genie AI Agents, Dashboards & Semantic Layer** | Videos 18, 19, 24, 25, 31 | Module 03, 09, 10 (Notebooks 025, 095, 104) |
| **Finance Domain End-to-End Medallion Project** | Video 20 | Module 04, 07, 10 (Notebooks 035, 069, 096) |
| **CI/CD with Databricks Asset Bundles (DABs)** | Video 21 | Module 09 (Notebook 094) |
| **PII Data Masterclass (Masking, AES Encryption)** | Video 22 | Module 01, 09 (Notebooks 005, 089, 090) |
| **Data Architect Interview Questions & Scenarios** | Video 23 | Embedded across all 105 Notebooks! |
| **Retail Multi-Source Data Engineering Project** | Video 26 | Module 07, 10 (Notebooks 072, 097) |
| **Agentic AI & Generative AI on Lakehouse** | Video 27 | Module 10 (Notebook 104) |
| **Data Modeling (Star Schema, SCD 1/2/3/4, Data Vault)** | Video 28 | Module 07 (Notebooks 064 - 073) |

---

## 🗂️ Project Repository Structure

```
spark-coding-usecases-masterclass/
├── .github/
│   └── workflows/
│       └── ci.yml                 # Automated CI running test suite & notebook validation
├── notebooks/
│   ├── 01_core_dataframe_api/     # Notebooks 001 - 012: PySpark session, schema, UDFs, dbutils
│   ├── 02_complex_data_types/     # Notebooks 013 - 022: Arrays, higher-order functions, JSON, XML
│   ├── 03_sql_and_window_functions/# Notebooks 023 - 032: Grouping sets, pivots, ranking, gaps & islands
│   ├── 04_delta_lake_and_acid/    # Notebooks 033 - 043: ACID, MERGE, Time Travel, VACUUM, Z-Order
│   ├── 05_data_ingestion_autoloader/# Notebooks 044 - 053: Auto Loader, CSV, Parquet, Avro, JDBC, APIs
│   ├── 06_streaming_realtime/     # Notebooks 054 - 063: Structured streaming, watermarks, joins, DLQ
│   ├── 07_lakehouse_modeling_scd/ # Notebooks 064 - 073: Medallion architecture, SCD 1/2/3/4, Data Vault
│   ├── 08_performance_and_tuning/ # Notebooks 074 - 085: Catalyst, AQE, Salting, Skew, Bucketing, Photon
│   ├── 09_unity_catalog_and_security/# Notebooks 086 - 095: 3-tier namespace, RLS, Column masking, FinOps
│   └── 10_end_to_end_projects/    # Notebooks 096 - 105: Finance, Retail, Healthcare, IoT, RAG, Alerts
├── scripts/
│   ├── generate_all.py            # Regenerates all 105 notebooks programmatically
│   ├── validate_notebooks.py      # Verifies JSON format, cell count & AST syntax
│   └── export_to_databricks.py    # Converts .ipynb to Databricks native .py source format
├── tests/
│   └── test_notebook_integrity.py # Automated pytest test suite
├── .gitignore
├── LICENSE                        # MIT License
├── pyproject.toml
├── requirements.txt
└── README.md                      # Master documentation & 105-notebook index
```

---

## 🚀 Quick Start Guide

### 1. Run Locally (VS Code / Jupyter / Standard PySpark)

```bash
# Clone the repository
git clone https://github.com/santoshjammi29/spark-coding-usecases-masterclass.git
cd spark-coding-usecases-masterclass

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies (PySpark, Delta Spark, PyArrow, Pandas, PyTest)
pip install -r requirements.txt

# Launch Jupyter Lab
jupyter lab
```

### 2. Import into Databricks Workspace
- **Via Databricks Repos / Git Folders**:
  1. Open your Databricks workspace.
  2. Navigate to **Workspace** > **Repos** (or **Git Folders**).
  3. Click **Add Repo** and paste your GitHub repository URL:
     `https://github.com/santoshjammi29/spark-coding-usecases-masterclass.git`.
  4. All 105 notebooks will open natively in your workspace!

- **Convert to Native Databricks `.py` Format**:
  If you prefer `# Databricks notebook source` format:
  ```bash
  python scripts/export_to_databricks.py
  ```

---

## 📚 Master Catalog of 105 Use Cases

{catalog_section}

---

## ⚙️ CI/CD & Quality Assurance

This repository includes continuous integration tests to ensure that every single notebook is valid, well-structured, and executable:
- **`python scripts/validate_notebooks.py`**: Ensures all 105 `.ipynb` files have valid JSON syntax, proper metadata (`nbformat: 4`), complete problem statements, code cells, and valid Python syntax.
- **`pytest tests/`**: Automated test suite asserting module coverage, cell counts, and data contracts.

---

## 🤝 Contributing & License
Contributions, feedback, and additional use cases are welcome! Please feel free to open a Pull Request or Issue.

Licensed under the [MIT License](LICENSE).
"""

    with open(base_dir / "README.md", "w", encoding="utf-8") as f:
        f.write(readme_content)

    print(f"Generated comprehensive README.md with {total_count} cataloged notebooks!")

if __name__ == "__main__":
    generate_readme(Path(__file__).parent.parent)
