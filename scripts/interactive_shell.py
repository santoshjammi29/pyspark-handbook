#!/usr/bin/env python3
"""
Interactive CLI Explorer for The PySpark Handbook:
Search, inspect, and explore 105 PySpark production recipes, architectures, runbooks, and interview guides.

Usage:
    python scripts/interactive_shell.py 025
    python scripts/interactive_shell.py "pandas udf"
    python scripts/interactive_shell.py --list
    python scripts/interactive_shell.py --interview
    python scripts/interactive_shell.py --runbooks
    python scripts/interactive_shell.py --architectures
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = BASE_DIR / "notebooks"
RUNBOOKS_DIR = BASE_DIR / "runbooks"
ARCH_DIR = BASE_DIR / "architectures"
INTERVIEW_DIR = BASE_DIR / "interview"

def list_all():
    print("\n📚 The PySpark Handbook: 105 Production Recipes\n" + "=" * 80)
    for mod_dir in sorted(list(NOTEBOOKS_DIR.glob("*"))):
        if not mod_dir.is_dir():
            continue
        print(f"\n📂 {mod_dir.name}:")
        for nb in sorted(list(mod_dir.glob("*.py"))):
            print(f"   ⚡ {nb.name}")
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

def find_file(query: str):
    # Try exact number prefix in notebooks
    if query.isdigit():
        padded = f"{int(query):03d}"
        matches = list(NOTEBOOKS_DIR.glob(f"*/*{padded}*.py"))
        if matches:
            return matches[0]
            
    # Substring search in filenames across all dirs
    clean_q = query.lower().replace(" ", "_")
    for search_dir, ext in [(NOTEBOOKS_DIR, "*.py"), (RUNBOOKS_DIR, "*.md"), (ARCH_DIR, "*.md"), (INTERVIEW_DIR, "*.md")]:
        matches = list(search_dir.glob(f"*{clean_q}*{ext}")) or list(search_dir.glob(f"*/*{clean_q}*{ext}"))
        if matches:
            return matches[0]
        
    # Search content in notebooks
    for nb in sorted(list(NOTEBOOKS_DIR.glob("*/*.py"))):
        if query.lower() in nb.read_text(encoding="utf-8").lower():
            return nb
            
    # Search content in runbooks & architectures
    for doc in sorted(list(RUNBOOKS_DIR.glob("*.md")) + list(ARCH_DIR.glob("*.md"))):
        if query.lower() in doc.read_text(encoding="utf-8").lower():
            return doc
            
    return None

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("Usage: python scripts/interactive_shell.py <recipe_number_or_keyword>")
        print("       python scripts/interactive_shell.py --list")
        print("       python scripts/interactive_shell.py --runbooks")
        print("       python scripts/interactive_shell.py --architectures")
        print("       python scripts/interactive_shell.py --interview")
        print("Example: python scripts/interactive_shell.py 025")
        print("         python scripts/interactive_shell.py 'pandas udf'")
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

    match = find_file(arg)
    if not match:
        print(f"❌ No matching recipe, runbook, or architecture found for: '{arg}'")
        sys.exit(1)

    print(f"\n📖 Found: {match.name}\n" + "=" * 80)
    print(match.read_text(encoding="utf-8"))
    print("=" * 80)

if __name__ == "__main__":
    main()
