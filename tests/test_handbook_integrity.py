"""Static zero-JVM integrity suite for The PySpark Handbook."""

import ast
import fnmatch
import json
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

MODULE_DIRS = [
    "01_foundations",
    "02_production_patterns",
    "03_performance_and_streaming",
    "04_spark4_and_modern_api",
    "05_lakehouse_formats",
    "06_platform_adapters",
]

CORE_DIRS = MODULE_DIRS[:5]


def _iter_repo_files():
    skip_dirs = {".git", "__pycache__", ".pytest_cache", ".ruff_cache", "site"}
    for path in BASE_DIR.rglob("*"):
        if any(part in skip_dirs for part in path.parts):
            continue
        if path.is_file():
            yield path


def test_six_module_directories_exist():
    for d in MODULE_DIRS:
        mod_dir = BASE_DIR / d
        assert mod_dir.exists() and mod_dir.is_dir(), f"Missing module directory: {d}"
        py_files = list(mod_dir.glob("*.py"))
        assert len(py_files) >= 1, f"Expected at least 1 Python module in {d}/"


def test_all_python_files_compile():
    errors = []
    for py_file in sorted(_iter_repo_files()):
        if py_file.suffix != ".py":
            continue
        source = py_file.read_text(encoding="utf-8")
        try:
            ast.parse(source, filename=str(py_file))
        except SyntaxError as exc:
            errors.append(f"{py_file.relative_to(BASE_DIR)}: {exc}")
    assert not errors, "Python syntax errors:\n" + "\n".join(errors)


def test_core_modules_free_of_databricks_headers_and_sparkcontext():
    violations = []
    for d in CORE_DIRS:
        for py_file in sorted((BASE_DIR / d).rglob("*.py")):
            rel = py_file.relative_to(BASE_DIR)
            text = py_file.read_text(encoding="utf-8")
            if "# Databricks notebook source" in text or "# COMMAND ----------" in text:
                violations.append(f"{rel}: contains Databricks notebook cell boilerplate")
            tree = ast.parse(text, filename=str(py_file))
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and node.attr == "sparkContext":
                    violations.append(f"{rel}:{node.lineno}: uses sparkContext (incompatible with Spark Connect)")
    assert not violations, "Core Spark boundary violations:\n" + "\n".join(violations)


def test_markdown_links_resolve():
    md_files = []
    for root_md in ("README.md", "CONTRIBUTING.md", "AGENTS.md"):
        p = BASE_DIR / root_md
        if p.exists():
            md_files.append(p)
    for sub in ("docs", "architectures", "runbooks", "interview", "topics", ".github"):
        sub_dir = BASE_DIR / sub
        if sub_dir.exists():
            md_files.extend(sorted(sub_dir.rglob("*.md")))

    fenced_re = re.compile(r"```.*?```", re.DOTALL)
    inline_re = re.compile(r"`[^`\n]+`")
    link_re = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")

    broken = []
    for md_file in md_files:
        content = md_file.read_text(encoding="utf-8")
        stripped = inline_re.sub("", fenced_re.sub("", content))
        for raw_target in link_re.findall(stripped):
            target = raw_target.strip().split()[0].strip("<>")
            if not target or target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            path_part = target.split("#", 1)[0]
            if not path_part:
                continue
            resolved = (md_file.parent / path_part).resolve()
            if not resolved.exists():
                broken.append(f"{md_file.relative_to(BASE_DIR)} -> {raw_target}")

    assert not broken, "Broken relative Markdown links:\n" + "\n".join(broken)


def test_no_files_over_500kb():
    max_bytes = 500 * 1024
    oversized = [
        f"{p.relative_to(BASE_DIR)} ({p.stat().st_size} bytes)"
        for p in _iter_repo_files()
        if p.stat().st_size > max_bytes
    ]
    assert not oversized, "Files exceeding 500 KB limit:\n" + "\n".join(oversized)


def test_no_banned_dirs_or_extensions():
    banned_dirs = ("notebooks", "scripts", "apps")
    for d in banned_dirs:
        assert not (BASE_DIR / d).exists(), f"Banned directory still present: {d}/"

    banned_exts = {".csv", ".parquet", ".orc", ".avro", ".ipynb"}
    banned_names = ("Dockerfile*", "*devcontainer*")
    bad_files = []
    for p in _iter_repo_files():
        if p.suffix.lower() in banned_exts:
            bad_files.append(f"{p.relative_to(BASE_DIR)} (banned extension {p.suffix})")
        if any(fnmatch.fnmatch(p.name, pat) for pat in banned_names):
            bad_files.append(f"{p.relative_to(BASE_DIR)} (banned file pattern)")
    assert not bad_files, "Banned lab/dataset artifacts found:\n" + "\n".join(bad_files)


def test_manifest_json_valid_and_paths_exist():
    manifest_path = BASE_DIR / "manifest.json"
    assert manifest_path.exists(), "manifest.json missing"
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = data.get("entries", [])
    assert data.get("entry_count") == len(entries), "manifest.json entry_count mismatch"
    ids = [e["id"] for e in entries]
    assert len(ids) == len(set(ids)), "Duplicate entry IDs in manifest.json"
    missing = [e["relative_path"] for e in entries if not (BASE_DIR / e["relative_path"]).exists()]
    assert not missing, "Missing paths referenced in manifest.json:\n" + "\n".join(missing)


def test_databricks_asset_bundle_paths_resolve():
    dab_dir = BASE_DIR / "06_platform_adapters" / "databricks_asset_bundles"
    assert (dab_dir / "databricks.yml").exists(), "databricks.yml missing under 06_platform_adapters/databricks_asset_bundles/"
    res_dir = dab_dir / "resources"
    assert res_dir.is_dir(), "DAB resources/ directory missing"
    yml_files = list(res_dir.glob("*.yml"))
    assert len(yml_files) >= 5, f"Expected >=5 DAB resource YAML files, found {len(yml_files)}"

    missing = []
    for yml_file in yml_files:
        text = yml_file.read_text(encoding="utf-8")
        for match in re.findall(r'(?:notebook_path|path):\s*["\']?([^\s"\']+\.py)["\']?', text):
            target = (yml_file.parent / match).resolve()
            if not target.exists():
                missing.append(f"{yml_file.name} -> {match}")
    assert not missing, "Missing Python paths in DAB resources:\n" + "\n".join(missing)
