"""
Export to Jupyter Notebook (.ipynb) Utility
Converts Databricks native Python source notebooks (.py) to standard Jupyter Notebooks (.ipynb).
"""

import json
from pathlib import Path

def convert_databricks_py_to_ipynb(py_path: Path, out_path: Path):
    content = py_path.read_text(encoding="utf-8")
    commands = content.split("# COMMAND ----------")

    cells = []
    for cmd in commands:
        cmd_stripped = cmd.strip()
        if not cmd_stripped:
            continue

        if cmd_stripped.startswith("# MAGIC %md") or "# MAGIC %md" in cmd_stripped:
            lines = []
            for line in cmd_stripped.splitlines():
                if line.startswith("# MAGIC %md"):
                    continue
                elif line.startswith("# MAGIC "):
                    lines.append(line[8:])
                elif line.startswith("# MAGIC"):
                    lines.append(line[7:])
                elif not line.startswith("# Databricks notebook source"):
                    lines.append(line)
            cells.append({
                "cell_type": "markdown",
                "metadata": {},
                "source": [l + "\n" for l in lines]
            })
        else:
            lines = []
            for line in cmd_stripped.splitlines():
                if not line.startswith("# Databricks notebook source"):
                    lines.append(line)
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [l + "\n" for l in lines]
            })

    nb_json = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.10.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(nb_json, f, indent=1, ensure_ascii=False)

def export_all(base_dir: Path):
    py_files = sorted(list((base_dir / "notebooks").glob("*/*.py")))
    out_dir = base_dir / "jupyter_exports"
    print(f"Exporting {len(py_files)} Databricks notebooks to Jupyter .ipynb format in {out_dir}...")

    for py_path in py_files:
        rel = py_path.relative_to(base_dir / "notebooks")
        target_ipynb = out_dir / rel.parent / f"{py_path.stem}.ipynb"
        convert_databricks_py_to_ipynb(py_path, target_ipynb)

    print(f"Exported {len(py_files)} Jupyter notebooks successfully!")

if __name__ == "__main__":
    base_dir = Path(__file__).parent.parent
    export_all(base_dir)
