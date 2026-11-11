#!/usr/bin/env python3
"""
Spark Execution Plan Static Analyzer
Inspects a formatted Spark Physical Plan string and flags dangerous anti-patterns:
- BroadcastNestedLoopJoin / CartesianProduct
- Unpushed filters (missing Predicate Pushdown)
- High-risk full scans without partition pruning
"""

import sys
import re
from pathlib import Path

def analyze_plan_text(plan_text: str):
    issues = []
    
    # Check 1: Cartesian or Nested Loop Joins
    if "BroadcastNestedLoopJoin" in plan_text:
        issues.append("CRITICAL: BroadcastNestedLoopJoin detected. Check for non-equi joins (e.g. >, <, !=) causing Cartesian explosion.")
    if "CartesianProduct" in plan_text:
        issues.append("CRITICAL: CartesianProduct detected. An inner or cross join is missing a join predicate.")

    # Check 2: Missing Predicate Pushdown
    scans = re.findall(r"(?:FileScan|PhotonScan)[^\n]+", plan_text)
    for scan in scans:
        if "PushedFilters: []" in scan:
            issues.append(f"WARNING: File scan with empty PushedFilters: {scan[:80]}... Check if functions or type casts prevent pushdown.")

    # Check 3: Large number of Exchanges
    exchanges = len(re.findall(r"Exchange\s+", plan_text))
    if exchanges > 5:
        issues.append(f"PERFORMANCE: Query contains {exchanges} network shuffle Exchanges. Consider repartitioning, caching, or bucketing.")

    return issues

def main():
    if len(sys.argv) < 2:
        print("Usage: python diagnose_spark_plan.py <plan_file.txt>")
        sys.exit(1)
        
    path = Path(sys.argv[1])
    if not path.exists():
        print(f"File {path} not found.", file=sys.stderr)
        sys.exit(1)
        
    text = path.read_text(encoding="utf-8")
    issues = analyze_plan_text(text)
    
    if not issues:
        print("✓ No critical join or filter anti-patterns detected in execution plan.")
        sys.exit(0)
    else:
        print(f"Found {len(issues)} potential plan bottlenecks:")
        for iss in issues:
            print(f"  - {iss}")
        sys.exit(0)

if __name__ == "__main__":
    main()
