"""
Notebook Builder Utility
Constructs Databricks native Python source notebook (.py) files
with '# Databricks notebook source', '# COMMAND ----------', and '# MAGIC %md' blocks.
"""

from pathlib import Path
from typing import List, Dict, Any

def format_markdown_cell(content: str) -> str:
    lines = content.strip().split("\n")
    magic_lines = ["# MAGIC %md"]
    for line in lines:
        magic_lines.append(f"# MAGIC {line}" if line else "# MAGIC")
    return "\n".join(magic_lines)

def build_notebook(
    nb_id: str,
    title: str,
    module_folder: str,
    module_name: str,
    difficulty: str,
    business_scenario: str,
    problem_statement: str,
    architectural_notes: str,
    sections: List[Dict[str, str]],
    key_takeaways: List[str],
    interview_questions: List[str],
    yt_ref: str = None  # Optional / ignored to eliminate video references
) -> str:
    """
    Builds a complete, richly documented Databricks native Python notebook (.py).
    """
    parts = ["# Databricks notebook source"]

    # Header Markdown Cell (Clean enterprise presentation, no video references)
    header_md = f"""# Notebook {nb_id}: {title}

**Module:** {module_name}  
**Difficulty:** `{difficulty}`  

---

## 1. Business Scenario & Real-World Context
{business_scenario}

## 2. Technical Requirement & Problem Statement
{problem_statement}

## 3. Spark & Databricks Architecture Notes
{architectural_notes}"""
    parts.append(format_markdown_cell(header_md))

    # Bootstrap Markdown & Code
    bootstrap_md = """## 4. Environment Setup & SparkSession Initialization
We configure a resilient `SparkSession` compatible with Databricks runtime, local PySpark development, or CI/CD test runners."""
    parts.append(format_markdown_cell(bootstrap_md))

    clean_app_title = title.replace(' ', '_').replace(':', '').replace('&', 'and')
    bootstrap_code = f"""import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Initialize SparkSession with Delta Lake support and optimized local configs
spark = (
    SparkSession.builder
    .appName("UseCase_{nb_id}_{clean_app_title}")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Version: {{spark.version}}")
print(f"Session Active: {{spark.sparkContext.appName}}")"""
    parts.append(bootstrap_code)

    # Dynamic Sections
    for idx, sec in enumerate(sections, start=5):
        sec_title = sec.get("title", f"Step {idx}")
        sec_expl = sec.get("explanation", "")
        sec_code = sec.get("code", "")

        sec_md = f"## {idx}. {sec_title}\n{sec_expl}"
        parts.append(format_markdown_cell(sec_md))
        if sec_code:
            parts.append(sec_code.strip())

    # Summary & Interview Questions
    summary_md = f"""## 9. Key Architectural Takeaways
""" + "\n".join([f"- **{t}**" if not t.startswith("-") else t for t in key_takeaways]) + f"""

## 10. Senior Data Engineering Interview Questions
""" + "\n".join([f"{i+1}. {q}" for i, q in enumerate(interview_questions)]) + """

---
*Created for the **Spark & Databricks Coding Use Cases Masterclass**.*"""

    parts.append(format_markdown_cell(summary_md))

    # Join with standard Databricks command delimiter
    return "\n\n# COMMAND ----------\n\n".join(parts) + "\n"

def write_notebook(path: Path, nb_content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(nb_content)
