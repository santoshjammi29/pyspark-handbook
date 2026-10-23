"""
Export to Databricks Source Format Utility
Converts Jupyter Notebooks (.ipynb) to Databricks native Python source files (.py)
with '# Databricks notebook source' and '# COMMAND ----------' cell delimiters.
"""

import json
from pathlib import Path

def convert_ipynb_to_databricks_py(ipynb_path: Path, out_path: Path):
    with open(ipynb_path, "r", encoding="utf-8") as f:
        nb = json.load(f)

    lines = ["# Databricks notebook source\n"]
    for idx, cell in enumerate(nb.get("cells", [])):
        cell_type = cell.get("cell_type")
        source = "".join(cell.get("source", []))

        if idx > 0:
            lines.append("\n# COMMAND ----------\n\n")

        if cell_type == "markdown":
            lines.append("%md\n")
            lines.append(source)
        elif cell_type == "code":
            lines.append(source)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

def export_all(base_dir: Path):
    nb_files = sorted(list((base_dir / "notebooks").glob("*/*.ipynb")))
    out_dir = base_dir / "databricks_source"
    print(f"Exporting {len(nb_files)} notebooks to Databricks Python format in {out_dir}...")

    for nb_path in nb_files:
        rel = nb_path.relative_to(base_dir / "notebooks")
        target_py = out_dir / rel.parent / f"{nb_path.stem}.py"
        convert_ipynb_to_databricks_py(nb_path, target_py)

    print(f"Exported {len(nb_files)} Databricks source files successfully!")

if __name__ == "__main__":
    base_dir = Path(__file__).parent.parent
    export_all(base_dir)
