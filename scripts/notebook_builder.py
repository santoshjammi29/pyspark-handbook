"""
Notebook Builder Utility
Constructs Databricks native Python source notebook (.py) files
with clean '# Databricks notebook source', '# COMMAND ----------', and '# MAGIC %md' blocks.
Optimized for Databricks Repos / Git Folders, local PySpark, and CI/CD test runners.
"""

import re
from pathlib import Path
from typing import List, Dict, Any, Optional

def clean_text_spacing(text: str) -> str:
    """Removes trailing whitespace from lines and collapses 3+ newlines to 2."""
    if not text:
        return ""
    cleaned_lines = [line.rstrip() for line in text.strip().splitlines()]
    result = "\n".join(cleaned_lines)
    return re.sub(r"\n{3,}", "\n\n", result)

def format_markdown_cell(content: str) -> str:
    """Formats markdown content into clean Databricks MAGIC %md lines without redundant blank lines."""
    content_clean = clean_text_spacing(content)
    lines = content_clean.split("\n")
    
    magic_lines = ["# MAGIC %md"]
    prev_blank = False
    
    for line in lines:
        if line:
            magic_lines.append(f"# MAGIC {line}")
            prev_blank = False
        else:
            if not prev_blank:
                magic_lines.append("# MAGIC")
                prev_blank = True

    # Remove any trailing empty '# MAGIC' lines
    while len(magic_lines) > 1 and magic_lines[-1] == "# MAGIC":
        magic_lines.pop()

    return "\n".join(magic_lines)

def clean_code_cell(code: str) -> str:
    """Cleans code strings, removing excessive blank lines and trailing spaces."""
    return clean_text_spacing(code)

def build_notebook(
    nb_id: str,
    title: str,
    module_folder: str = "",
    module_name: str = "",
    difficulty: str = "Intermediate",
    business_scenario: str = "",
    problem_statement: str = "",
    architectural_notes: str = "",
    sections: Optional[List[Dict[str, str]]] = None,
    key_takeaways: Optional[List[str]] = None,
    interview_questions: Optional[List[str]] = None,
    yt_ref: Optional[str] = None,
    **kwargs
) -> str:
    """
    Builds a clean, reader-friendly, directly executable Databricks native Python notebook (.py).
    """
    sections = sections or []
    key_takeaways = key_takeaways or []
    interview_questions = interview_questions or []

    cells = []

    # 1. Header & Architecture Markdown
    header_md = f"""# Notebook {nb_id}: {title}

**Module:** {module_name}  
**Difficulty:** `{difficulty}`  

---

### Business Scenario & Real-World Context
{business_scenario.strip()}

### Technical Requirement & Problem Statement
{problem_statement.strip()}

### Spark & Databricks Architecture Notes
{architectural_notes.strip()}"""
    cells.append(format_markdown_cell(header_md))

    # 2. Environment Setup Markdown
    setup_md = """### Environment Setup & SparkSession Initialization
If running inside a Databricks notebook, the `spark` session is pre-initialized by the cluster.
If executing outside Databricks (e.g. VS Code, local CLI, CI/CD runner), we initialize a local SparkSession with Delta Lake support."""
    cells.append(format_markdown_cell(setup_md))

    # 3. Environment Setup Code
    clean_app_title = title.replace(' ', '_').replace(':', '').replace('&', 'and').replace('-', '_')
    clean_app_title = re.sub(r'[^a-zA-Z0-9_]', '', clean_app_title)

    setup_code = f"""import os
import sys

# In Databricks, the SparkSession 'spark' is pre-initialized by the cluster.
# For local PySpark / CI execution, initialize a local session with Delta Lake support:
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("UseCase_{nb_id}_{clean_app_title}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Version: {{spark.version}}")
print(f"Session Active: {{spark.sparkContext.appName}}")"""
    cells.append(clean_code_cell(setup_code))

    # 4. Dynamic Implementation Sections
    for idx, sec in enumerate(sections, start=1):
        sec_title = sec.get("title", f"Step {idx}").strip()
        sec_expl = sec.get("explanation", "").strip()
        sec_code = sec.get("code", "").strip()

        # Format title cleanly without duplicated step prefixes
        if re.match(r"^(Step|Strategy|Part|Phase|Section)\s+\d+[:\.]?", sec_title, re.IGNORECASE):
            heading = f"### {sec_title}"
        else:
            heading = f"### Step {idx}: {sec_title}"

        sec_md = f"{heading}\n{sec_expl}" if sec_expl else heading
        cells.append(format_markdown_cell(sec_md))
        if sec_code:
            cells.append(clean_code_cell(sec_code))

    # 5. Key Architectural Takeaways & Interview Questions
    takeaways_formatted = "\n".join([f"- **{t}**" if not t.startswith("-") else t for t in key_takeaways])
    questions_formatted = "\n".join([f"{i+1}. {q}" for i, q in enumerate(interview_questions)])

    summary_md = f"""### Key Architectural Takeaways
{takeaways_formatted}

---

### Senior Data Engineering Interview Questions
{questions_formatted}

---
*Created for the **Spark & Databricks Coding Use Cases Masterclass**.*"""
    cells.append(format_markdown_cell(summary_md))

    # Build file:
    # Cell 1 starts immediately on line 1 with "# Databricks notebook source\n"
    # This prevents an empty blank command 0 in Databricks!
    first_cell = f"# Databricks notebook source\n{cells[0]}"
    all_cells = [first_cell] + cells[1:]

    return "\n\n# COMMAND ----------\n\n".join(all_cells) + "\n"

def write_notebook(path: Path, nb_content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(nb_content, encoding="utf-8")
