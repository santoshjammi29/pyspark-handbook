#!/usr/bin/env python3
"""
Interactive CLI Explorer for The PySpark Handbook:
Search, inspect, and explore production recipes, architectures, runbooks, and interview guides.

Usage:
    python scripts/interactive_shell.py 025
    python scripts/interactive_shell.py "keyword"
    python scripts/interactive_shell.py --list
    python scripts/interactive_shell.py --runbooks
    python scripts/interactive_shell.py --architectures
    python scripts/interactive_shell.py --interview
    python scripts/interactive_shell.py --ecosystem
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONTENT_DIR = BASE_DIR / "notebooks"
RUNBOOKS_DIR = BASE_DIR / "runbooks"
ARCH_DIR = BASE_DIR / "architectures"
INTERVIEW_DIR = BASE_DIR / "interview"

def list_all():
    print("\n📚 The PySpark Handbook: Core Catalog\n" + "=" * 80)
    for mod_dir in sorted(list(CONTENT_DIR.glob("*"))):
        if not mod_dir.is_dir():
            if mod_dir.suffix in [".md", ".py", ".sql"]:
                print(f"   ⚡ {mod_dir.name}")
            continue
        print(f"\n📂 {mod_dir.name}:")
        for item in sorted(list(mod_dir.glob("*.py"))):
            print(f"   ⚡ {item.name}")
    print("\n" + "=" * 80)

def list_runbooks():
    print("\n🚨 The PySpark Handbook: Production Incident Runbooks\n" + "=" * 80)
    for rb in sorted(list(RUNBOOKS_DIR.glob("*.md"))):
        print(f"   🚨 {rb.name}")
    print("\n" + "=" * 80)

def list_architectures():
    print("\n🏛️ The PySpark Handbook: Deep-Dive Architectures\n" + "=" * 80)
    for arch in sorted(list(ARCH_DIR.glob("*.md"))):
        print(f"   🏛️ {arch.name}")
    print("\n" + "=" * 80)

def list_interviews():
    print("\n🎓 The PySpark Handbook: 4-Tier Interview Preparation Matrix\n" + "=" * 80)
    for iv in sorted(list(INTERVIEW_DIR.glob("*.md"))):
        print(f"   🎓 {iv.name}")
    print("\n" + "=" * 80)

def list_ecosystem():
    print("""
🌐 Modern Data Engineering Ecosystem Cross-Navigation:
================================================================================
Central Platform: https://moderndataengineer.uk (11,950+ verified assets)

Cross-Repository Topic Mapping:
1. PySpark & Distributed Compute:
   • The PySpark Handbook:        https://github.com/santoshjammi29/pyspark-handbook
   • The Databricks Handbook:     https://github.com/santoshjammi29/databricks-handbook

2. Relational & MPP SQL:
   • The SQL Handbook:            https://github.com/santoshjammi29/sql-handbook
   • Cross-Engine Rosetta Stone:  sql-handbook/runbooks/05_cross_engine_rosetta_stone.md

3. Cloud Infrastructure & Security:
   • Azure Enterprise Handbook:   https://github.com/santoshjammi29/azure-cheat-sheet
   • Unity Catalog Governance:    databricks-handbook/notebooks/09_unity_catalog_and_security

4. Incident Triage Runbooks:
   • Spark/Databricks OOMs:       pyspark-handbook/runbooks/01_driver_and_executor_oom_triage.md
   • Data Skew Salting:           pyspark-handbook/runbooks/03_data_skew_and_two_phase_salting.md
   • Cloud Networking & DNS:      azure-cheat-sheet/runbooks/02_vnet_peering_and_private_endpoint_dns_failures.md
   • Warehouse FinOps:            sql-handbook/runbooks/06_finops_and_warehouse_cost_optimization.md
================================================================================
""")

def find_file(query: str):
    if query.isdigit():
        padded = f"{int(query):03d}"
        matches = list(CONTENT_DIR.glob(f"*/*{padded}*.py")) or list(CONTENT_DIR.glob(f"*{padded}*.py"))
        if matches:
            return matches[0]
            
    clean_q = query.lower().replace(" ", "_")
    clean_q_dash = query.lower().replace(" ", "-")
    for search_dir in [CONTENT_DIR, RUNBOOKS_DIR, ARCH_DIR, INTERVIEW_DIR]:
        matches = (list(search_dir.glob(f"*{clean_q}*")) or 
                   list(search_dir.glob(f"*/*{clean_q}*")) or
                   list(search_dir.glob(f"*{clean_q_dash}*")) or
                   list(search_dir.glob(f"*/*{clean_q_dash}*")))
        if matches:
            return matches[0]
        
    for search_dir in [CONTENT_DIR, RUNBOOKS_DIR, ARCH_DIR, INTERVIEW_DIR]:
        for doc in sorted(list(search_dir.glob("*/*")) + list(search_dir.glob("*"))):
            if doc.is_file() and doc.suffix in [".py", ".sql", ".md"]:
                if query.lower() in doc.read_text(encoding="utf-8").lower():
                    return doc
            
    return None

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("Usage: python scripts/interactive_shell.py <query_or_number>")
        print("       python scripts/interactive_shell.py --list")
        print("       python scripts/interactive_shell.py --runbooks")
        print("       python scripts/interactive_shell.py --architectures")
        print("       python scripts/interactive_shell.py --interview")
        print("       python scripts/interactive_shell.py --ecosystem")
        sys.exit(0)

    arg = sys.argv[1]
    if arg == "--list":
        list_all()
        sys.exit(0)
    elif arg == "--runbooks":
        list_runbooks()
        sys.exit(0)
    elif arg == "--architectures":
        list_architectures()
        sys.exit(0)
    elif arg == "--interview":
        list_interviews()
        sys.exit(0)
    elif arg == "--ecosystem":
        list_ecosystem()
        sys.exit(0)

    match = find_file(arg)
    if not match:
        print(f"❌ No matching content found for: '{arg}'")
        sys.exit(1)

    print(f"\n📖 Found: {match.name}\n" + "=" * 80)
    print(match.read_text(encoding="utf-8"))
    print("=" * 80)

if __name__ == "__main__":
    main()
