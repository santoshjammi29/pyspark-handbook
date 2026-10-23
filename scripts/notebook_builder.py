"""
Notebook Builder Utility
Constructs well-formed Jupyter Notebook (.ipynb) files for Spark Coding Use Cases.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional

def make_markdown_cell(content: str) -> Dict[str, Any]:
    lines = [line + "\n" for line in content.split("\n")]
    if lines and lines[-1].endswith("\n"):
        lines[-1] = lines[-1][:-1]
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": lines
    }

def make_code_cell(code: str) -> Dict[str, Any]:
    lines = [line + "\n" for line in code.split("\n")]
    if lines and lines[-1].endswith("\n"):
        lines[-1] = lines[-1][:-1]
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": lines
    }

def build_notebook(
    nb_id: str,
    title: str,
    module_folder: str,
    module_name: str,
    difficulty: str,
    yt_ref: str,
    business_scenario: str,
    problem_statement: str,
    architectural_notes: str,
    sections: List[Dict[str, str]],
    key_takeaways: List[str],
    interview_questions: List[str]
) -> Dict[str, Any]:
    """
    Builds a complete, richly documented Jupyter notebook structure.
    sections is a list of dicts with {"title": str, "explanation": str, "code": str}
    """
    cells = []

    # Cell 1: Master Markdown Header
    header_md = f"""# Notebook {nb_id}: {title}

**Module:** {module_name}  
**Difficulty:** `{difficulty}`  
**Databricks Masterclass Reference:** {yt_ref}  

---

## 1. Business Scenario & Real-World Context
{business_scenario}

## 2. Technical Requirement & Problem Statement
{problem_statement}

## 3. Spark & Databricks Architecture Notes
{architectural_notes}
"""
    cells.append(make_markdown_cell(header_md))

    # Cell 2: Universal SparkSession bootstrap code
    bootstrap_md = """## 4. Environment Setup & SparkSession Initialization
We configure a resilient `SparkSession` compatible with local PySpark development, Databricks runtime, or CI/CD test runners."""
    cells.append(make_markdown_cell(bootstrap_md))

    bootstrap_code = """import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Initialize SparkSession with Delta Lake support and optimized local configs
spark = (
    SparkSession.builder
    .appName(f"UseCase_{nb_id}_{title.replace(' ', '_')}")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f" Spark Version: {spark.version}")
print(f" Session Active: {spark.sparkContext.appName}")
""".replace("{nb_id}", nb_id).replace("{title.replace(' ', '_')}", title.replace(' ', '_').replace(':', ''))
    cells.append(make_code_cell(bootstrap_code))

    # Dynamic sections
    for idx, sec in enumerate(sections, start=5):
        sec_title = sec.get("title", f"Step {idx}")
        sec_expl = sec.get("explanation", "")
        sec_code = sec.get("code", "")
        
        md_text = f"## {idx}. {sec_title}\n{sec_expl}"
        cells.append(make_markdown_cell(md_text))
        if sec_code:
            cells.append(make_code_cell(sec_code))

    # Summary & Interview Cell
    summary_md = f"""## 9. Key Architectural Takeaways
""" + "\n".join([f"- **{t}**" if not t.startswith("-") else t for t in key_takeaways]) + f"""

## 10. Senior Data Engineering Interview Questions
""" + "\n".join([f"{i+1}. {q}" for i, q in enumerate(interview_questions)]) + """

---
*Created for the **Spark Coding Use Cases Masterclass** repository referencing the Databricks All-in-One Playlist.*
"""
    cells.append(make_markdown_cell(summary_md))

    notebook_json = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.10.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    return notebook_json

def write_notebook(path: Path, nb_dict: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(nb_dict, f, indent=1, ensure_ascii=False)
