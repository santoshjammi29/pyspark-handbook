#!/usr/bin/env python3
"""
Lightweight Test Runner for The PySpark Handbook
Executes all test functions across the test suite without requiring pytest on PATH.
"""

import sys
import inspect
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

import tests.test_notebook_integrity as t_nb
import tests.test_bundle_integrity as t_bundle
import tests.test_runbooks_and_architectures as t_arch

def run_module_tests(module, base_dir):
    passed = 0
    failed = 0
    errors = []
    
    print(f"\n▶ Testing {module.__name__}...")
    for attr_name in dir(module):
        if attr_name.startswith("test_"):
            test_fn = getattr(module, attr_name)
            if callable(test_fn):
                try:
                    sig = inspect.signature(test_fn)
                    if "base_dir" in sig.parameters:
                        test_fn(base_dir)
                    else:
                        test_fn()
                    print(f"  ✔ {attr_name}")
                    passed += 1
                except Exception as e:
                    print(f"  ✖ {attr_name}: {e}")
                    failed += 1
                    errors.append((attr_name, str(e)))
                    
    return passed, failed, errors

def main():
    print("=" * 60)
    print("⚡ PySpark Handbook - Test Suite Runner")
    print("=" * 60)
    
    total_passed = 0
    total_failed = 0
    all_errors = []
    
    for mod in [t_nb, t_bundle, t_arch]:
        p, f, errs = run_module_tests(mod, BASE_DIR)
        total_passed += p
        total_failed += f
        all_errors.extend(errs)
        
    print("\n" + "=" * 60)
    print(f"RESULTS: {total_passed} Passed, {total_failed} Failed")
    print("=" * 60)
    
    if total_failed > 0:
        print("\nFailures:")
        for name, err in all_errors:
            print(f"- {name}: {err}")
        sys.exit(1)
    else:
        print("\n🎉 ALL TESTS PASSED SUCCESSFULLY!")
        sys.exit(0)

if __name__ == "__main__":
    main()
