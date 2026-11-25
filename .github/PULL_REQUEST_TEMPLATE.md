## Description

<!-- Concise summary of the change, affected module(s), and motivation. -->

## Type of Change

- [ ] 🐛 Bug fix / Spark 4.0+ accuracy update
- [ ] ✨ New pattern module (5-part blueprint)
- [ ] 📝 Documentation / architecture / runbook / interview update
- [ ] ⚙️ Static CI / tooling improvement

## Code-Only & Quality Checklist

In accordance with [`CONTRIBUTING.md`](../CONTRIBUTING.md):

- [ ] **Code-Only PR**: No datasets (`*.csv`, `*.parquet`, `*.orc`, `*.avro`), no `.ipynb` notebooks, no lab runners, and no file > 500 KB.
- [ ] **5-Part Blueprint** *(for `.py` pattern modules)*: Includes (1) Scenario/Scale docstring, (2) `# ❌ Bad Practice` + `# Otherwise:`, (3) `# ✅ Good Practice` (ANSI-safe PySpark 4.0+), (4) `explain("formatted")` / Spark UI diagnostic notes, and (5) 2–4 Production/Interview Gotchas.
- [ ] **Core vs. Platform Separation**: Modules in `01_`–`05_` have zero `# Databricks notebook source` headers and zero `spark.sparkContext` calls; vendor-specific adapters live in `06_platform_adapters/`.
- [ ] **Static Verification Passed**: `python -m compileall -q .`, `ruff check .`, and `pytest tests/` pass locally.
- [ ] **Secret Sanitization**: No real credentials, tokens, or live cloud resource identifiers committed.
