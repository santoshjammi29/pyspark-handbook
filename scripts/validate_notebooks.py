"""
Notebook Validation Suite
Inspects all 105 Databricks Python notebooks (.py) for valid format, commands, and Python syntax.
"""

import ast
import sys
from pathlib import Path

def validate_all_notebooks(base_dir: Path) -> bool:
    notebook_dir = base_dir / "notebooks"
    notebook_files = sorted(list(notebook_dir.glob("*/*.py")))

    if not notebook_files:
        print("[ERROR] No Python notebook files (.py) found!")
        return False

    print(f"Validating {len(notebook_files)} Databricks Python notebooks (.py) across all modules...")
    errors = []

    for nb_path in notebook_files:
        rel_path = nb_path.relative_to(base_dir)
        try:
            content = nb_path.read_text(encoding="utf-8")

            # Check header
            if not content.startswith("# Databricks notebook source"):
                errors.append(f"{rel_path}: Missing '# Databricks notebook source' header")

            commands = content.split("# COMMAND ----------")
            if len(commands) < 4:
                errors.append(f"{rel_path}: Too few command cells ({len(commands)})")

            code_cell_count = 0
            markdown_cell_count = 0

            for idx, cmd in enumerate(commands):
                cmd_stripped = cmd.strip()
                if not cmd_stripped:
                    continue

                if cmd_stripped.startswith("# MAGIC %md") or "# MAGIC %md" in cmd_stripped:
                    markdown_cell_count += 1
                else:
                    code_cell_count += 1
                    # Clean code lines and validate syntax
                    code_lines = [
                        line for line in cmd_stripped.splitlines()
                        if not line.strip().startswith("# MAGIC") and not line.strip().startswith("%")
                    ]
                    code_clean = "\n".join(code_lines).strip()
                    if code_clean:
                        try:
                            ast.parse(code_clean)
                        except SyntaxError as se:
                            errors.append(f"{rel_path} command {idx}: Python SyntaxError: {se}")

            if code_cell_count < 1 or markdown_cell_count < 1:
                errors.append(
                    f"{rel_path}: Must contain at least 1 code and 1 markdown cell (got {code_cell_count} code, {markdown_cell_count} md)"
                )

        except Exception as e:
            errors.append(f"{rel_path}: Unexpected error: {e}")

    if errors:
        print(f"\n[FAIL] Found {len(errors)} validation errors:")
        for err in errors:
            print(f"  - {err}")
        return False

    print(f"\n[PASS] All {len(notebook_files)} Databricks Python notebooks (.py) are structurally sound and valid!")
    return True

if __name__ == "__main__":
    base_dir = Path(__file__).parent.parent
    success = validate_all_notebooks(base_dir)
    sys.exit(0 if success else 1)
