"""
Test Suite: Databricks Python Notebook Integrity & Standards Verification
"""

from pathlib import Path

def test_notebook_count():
    base_dir = Path(__file__).parent.parent
    nb_files = list((base_dir / "notebooks").glob("*/*.py"))
    assert len(nb_files) == 105, f"Expected exactly 105 .py notebooks, found {len(nb_files)}"

def test_notebook_databricks_format():
    base_dir = Path(__file__).parent.parent
    nb_files = list((base_dir / "notebooks").glob("*/*.py"))
    for nb in nb_files:
        content = nb.read_text(encoding="utf-8")
        assert content.startswith("# Databricks notebook source"), f"{nb.name}: Missing Databricks source header"
        assert "# COMMAND ----------" in content, f"{nb.name}: Missing cell delimiter"
        assert "# MAGIC %md" in content, f"{nb.name}: Missing markdown documentation cells"
        # Ensure no video references remain
        assert "Video " not in content, f"{nb.name}: Contains leftover video reference"

def test_all_ten_modules_present():
    base_dir = Path(__file__).parent.parent
    expected_modules = [
        "01_core_dataframe_api",
        "02_complex_data_types",
        "03_sql_and_window_functions",
        "04_delta_lake_and_acid",
        "05_data_ingestion_autoloader",
        "06_streaming_realtime",
        "07_lakehouse_modeling_scd",
        "08_performance_and_tuning",
        "09_unity_catalog_and_security",
        "10_end_to_end_projects"
    ]
    for mod in expected_modules:
        mod_dir = base_dir / "notebooks" / mod
        assert mod_dir.exists(), f"Module directory missing: {mod}"
        nbs = list(mod_dir.glob("*.py"))
        assert len(nbs) >= 10, f"Module {mod} has fewer than 10 notebooks (found {len(nbs)})"

if __name__ == "__main__":
    print("Running test suite...")
    test_notebook_count()
    test_notebook_databricks_format()
    test_all_ten_modules_present()
    print("[PASS] All automated tests passed successfully!")
