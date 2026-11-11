#!/usr/bin/env python3
"""
Databricks Notebook Integrity & AST Syntax Validator
Validates that notebooks adhere to the official Databricks format (# COMMAND ----------, # MAGIC %md)
and that all Python code cells compile cleanly without syntax errors.
"""

import ast
import sys
from pathlib import Path

def validate_notebook(path: Path):
    content = path.read_text(encoding="utf-8")
    errors = []
    
    if not content.startswith("# Databricks notebook source"):
        errors.append("Missing '# Databricks notebook source' header")
        
    commands = content.split("# COMMAND ----------")
    if len(commands) < 2:
        errors.append("Notebook must contain at least 2 cells delimited by '# COMMAND ----------'")
        
    for idx, cmd in enumerate(commands):
        cmd_stripped = cmd.strip()
        if not cmd_stripped or cmd_stripped.startswith("# MAGIC %md") or "# MAGIC %md" in cmd_stripped:
            continue
            
        code_lines = [
            line for line in cmd_stripped.splitlines()
            if not line.strip().startswith("# MAGIC") and not line.strip().startswith("%")
        ]
        clean_code = "\n".join(code_lines).strip()
        if clean_code:
            try:
                ast.parse(clean_code)
            except SyntaxError as e:
                errors.append(f"Cell {idx} SyntaxError: {e}")
                
    return errors

def main():
    target_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    notebooks = list(target_dir.glob("**/*.py"))
    print(f"Scanning {len(notebooks)} Python files in {target_dir}...")
    
    failed = 0
    for nb in notebooks:
        errs = validate_notebook(nb)
        if errs:
            failed += 1
            print(f"FAILED: {nb.name}")
            for err in errs:
                print(f"  - {err}")
                
    if failed == 0:
        print(f"All {len(notebooks)} notebooks validated successfully! AST & Format 100% clean.")
        sys.exit(0)
    else:
        print(f"\n{failed} notebooks failed validation.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
