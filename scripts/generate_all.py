"""
Master Notebook Generation Runner
Executes all 10 module generators sequentially to build all 105 notebooks.
"""

import sys
from pathlib import Path

# Add scripts directory to path
SCRIPTS_DIR = Path(__file__).parent
BASE_DIR = SCRIPTS_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

import gen_mod01
import gen_mod02
import gen_mod03
import gen_mod04
import gen_mod05
import gen_mod06
import gen_mod07
import gen_mod08
import gen_mod09
import gen_mod10

def main():
    print("=" * 70)
    print("STARTING FULL REGENERATION OF ALL 105 PYSPARK USE CASE NOTEBOOKS")
    print("=" * 70)

    modules = [
        ("Module 01", gen_mod01),
        ("Module 02", gen_mod02),
        ("Module 03", gen_mod03),
        ("Module 04", gen_mod04),
        ("Module 05", gen_mod05),
        ("Module 06", gen_mod06),
        ("Module 07", gen_mod07),
        ("Module 08", gen_mod08),
        ("Module 09", gen_mod09),
        ("Module 10", gen_mod10),
    ]

    for name, mod in modules:
        print(f"\n[RUNNING] {name}...")
        mod.generate(BASE_DIR)

    notebook_files = list(BASE_DIR.glob("notebooks/*/*.ipynb"))
    print("\n" + "=" * 70)
    print(f"SUCCESS: Total Notebooks Generated and Verified: {len(notebook_files)}")
    print("=" * 70)

if __name__ == "__main__":
    main()
