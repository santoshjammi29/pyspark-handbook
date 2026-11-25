# Contributing to PySpark Handbook

Thank you for contributing to **PySpark Handbook** (`santoshjammi29/pyspark-handbook`), part of the **Modern Data Engineering Lakehouse Ecosystem** ([moderndataengineer.uk](https://moderndataengineer.uk)).

This repository is a **code-only, zero-bloat Apache Spark 4.0+ reference handbook**. Keep contributions crisp, portable, and statically verifiable.

---

## 1. Strict Code-Only Policy

To keep clones fast and CI under 30 seconds with zero JVM overhead, Pull Requests **must** adhere to these repository guardrails:

- **No Labs or Notebooks**: Do NOT add `.ipynb` files, `# Databricks notebook source` / `# COMMAND ----------` headers, interactive lab runners, Dockerfiles, or devcontainers.
- **No Datasets or Large Binaries**: Do NOT commit `*.csv`, `*.parquet`, `*.orc`, `*.avro`, sample data dumps, or any file larger than **500 KB**.
- **No JVM CI Dependencies**: `requirements.txt` contains lightweight static tooling only (`ruff`, `pytest`). Do not add `pyspark`, `delta-spark`, `jupyter`, or Java installs to CI.
- **Core vs. Platform Isolation**:
  - `01_foundations/` through `05_lakehouse_formats/` must be **vendor-neutral, Spark Connect-compatible PySpark 4.0+** with zero `spark.sparkContext` calls and zero hardcoded `/tmp/` writes.
  - Cloud/vendor-specific integrations (Databricks, Microsoft Fabric, AWS EMR, GCP Dataproc, Kubernetes) belong exclusively in `06_platform_adapters/`.

---

## 2. Mandatory 5-Part Module Blueprint

Every `.py` pattern file in `01_foundations/` .. `06_platform_adapters/` must follow this concise 5-part structure:

1. **Scenario & Scale Docstring**: Production problem statement, data scale context, and Spark 4.0+ / Spark Connect applicability.
2. **Anti-Pattern (`# ❌ Bad Practice`)**: Realistic broken or slow pattern with an explicit `# Otherwise: <blast radius>` comment (e.g., *Driver OOM*, *ANSI cast exception*, *40x shuffle spill*).
3. **ANSI-Safe PySpark 4.0+ Fix (`# ✅ Good Practice`)**: Clean, typed, idiomatic DataFrame / SQL / Spark Connect solution safe under `spark.sql.ansi.enabled = true`.
4. **`explain("formatted")` & Spark UI Diagnostic Notes**: Concise comment block explaining what Catalyst operators, AQE re-optimizations, or Spark UI metrics confirm the fix.
5. **2–4 Production / Interview Gotchas**: Sharp edge cases, version caveats (e.g., Spark 3.5 -> 4.0), and operational invariants.

---

## 3. Documentation & Diagram Standards

- **Mermaid Safety**: Quote link labels containing parentheses (`-->|"Label (text)"|`), avoid raw `<` / `>` / `<=` in labels, avoid `;` in `sequenceDiagram` notes, and escape literal `\$`.
- **Relative Links**: Verify all Markdown links resolve relative to the file's directory; use canonical GitHub URLs for cross-repo references.
- **Whiteboard Invariant**: `assets/whiteboards/` maintains exactly **52** `.webp` blueprints. Do not add `.jpg` files or duplicate aliases.

---

## 4. Local Pre-Flight & PR Workflow

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m compileall -q .
ruff check .
pytest tests/
```

1. Fork the repository and branch from `main` (`feat/...` or `fix/...`).
2. Follow Conventional Commits (e.g., `feat(spark4): add variant shredding pattern`).
3. Ensure all static CI checks pass before requesting review.
