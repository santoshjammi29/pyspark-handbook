"""
Test Suite: Databricks Asset Bundle (DAB) Integrity & Resource Verification
"""

import os
import re
from pathlib import Path

def test_bundle_config_exists():
    base_dir = Path(__file__).parent.parent
    bundle_file = base_dir / "databricks.yml"
    assert bundle_file.exists(), "databricks.yml is missing at repository root"
    content = bundle_file.read_text(encoding="utf-8")
    assert "name: pyspark-handbook" in content
    assert "targets:" in content
    assert "dev:" in content
    assert "staging:" in content
    assert "prod:" in content

def test_bundle_resources_exist():
    base_dir = Path(__file__).parent.parent
    resources_dir = base_dir / "resources"
    assert resources_dir.exists() and resources_dir.is_dir()
    
    expected_resources = [
        "jobs.yml",
        "dlt_pipelines.yml",
        "unity_catalog.yml",
        "model_serving.yml",
        "apps.yml",
        "dashboards.yml"
    ]
    for res in expected_resources:
        file_path = resources_dir / res
        assert file_path.exists(), f"Expected bundle resource missing: {res}"

def test_notebook_paths_in_resources_exist():
    """Verify that every notebook path referenced in bundle YAML resources exists on disk."""
    base_dir = Path(__file__).parent.parent
    resources_dir = base_dir / "resources"
    
    for yml_file in resources_dir.glob("*.yml"):
        content = yml_file.read_text(encoding="utf-8")
        # Find paths matching ../notebooks/... or notebooks/...
        matches = re.findall(r'notebook_path:\s*["\']?\.\./(notebooks/[^\s"\']+\.py)["\']?', content)
        for nb_rel_path in matches:
            nb_abs_path = base_dir / nb_rel_path
            assert nb_abs_path.exists(), f"{yml_file.name} references non-existent notebook: {nb_rel_path}"

def test_databricks_app_integrity():
    base_dir = Path(__file__).parent.parent
    app_dir = base_dir / "apps" / "lakehouse_monitor"
    assert app_dir.exists(), "apps/lakehouse_monitor directory is missing"
    assert (app_dir / "app.py").exists(), "app.py is missing in apps/lakehouse_monitor"
    assert (app_dir / "app.yaml").exists(), "app.yaml manifest is missing in apps/lakehouse_monitor"
    assert (app_dir / "requirements.txt").exists(), "requirements.txt is missing in apps/lakehouse_monitor"

def test_github_actions_bundle_deploy_workflow():
    base_dir = Path(__file__).parent.parent
    workflow_file = base_dir / ".github" / "workflows" / "deploy_bundles.yml"
    assert workflow_file.exists(), ".github/workflows/deploy_bundles.yml is missing"
    content = workflow_file.read_text(encoding="utf-8")
    assert "databricks bundle validate" in content
    assert "databricks bundle deploy -t staging" in content
    assert "databricks bundle deploy -t prod" in content

if __name__ == "__main__":
    print("Running DAB test suite...")
    test_bundle_config_exists()
    test_bundle_resources_exist()
    test_notebook_paths_in_resources_exist()
    test_databricks_app_integrity()
    test_github_actions_bundle_deploy_workflow()
    print("[PASS] All Databricks Asset Bundle tests passed successfully!")
