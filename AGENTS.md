# Autonomous Agent Operational Guidelines: PySpark Handbook

> **Repository**: [santoshjammi29/pyspark-handbook](https://github.com/santoshjammi29/pyspark-handbook)  
> **Ecosystem Hub**: [moderndataengineer.uk](https://moderndataengineer.uk)  
> **Primary Authority**: `Apache Spark & Distributed Compute`

---

## 1. Repository Architecture & Code-Only Invariants

**PySpark Handbook** is a lightweight, **code-only Apache Spark 4.0+ reference handbook** organized into 6 cleanly separated code directories plus architectural docs:

- `01_foundations/` — Core DataFrame API, complex types, SQL, windows, joins
- `02_production_patterns/` — Ingestion, Medallion/SCD modeling, UDFs, E2E patterns, pytest testing
- `03_performance_and_streaming/` — Catalyst, AQE, skew salting, OOM triage, Structured Streaming
- `04_spark4_and_modern_api/` — Spark Connect, ANSI default, `VARIANT`, Python Data Source API, Polymorphic UDTFs, SQL Pipe syntax, structured JSON logging
- `05_lakehouse_formats/` — Delta Lake ACID, Liquid Clustering, Iceberg vs Delta vs Parquet
- `06_platform_adapters/` — Isolated vendor adapters (Databricks + DABs under `databricks_asset_bundles/`, Microsoft Fabric, AWS EMR, GCP Dataproc, Kubernetes)

### Code Authoring Invariants
1. **5-Part Blueprint**: Every `.py` pattern module follows:
   1. Scenario & Scale docstring
   2. `# ❌ Bad Practice` + `# Otherwise: <blast radius>`
   3. `# ✅ Good Practice` (ANSI-safe PySpark 4.0+)
   4. `explain("formatted")` / Spark UI diagnostic notes (comments)
   5. 2–4 Production / Interview Gotchas
2. **Core vs. Platform Boundary**: Modules in `01_foundations/` .. `05_lakehouse_formats/` must contain **zero** `# Databricks notebook source` headers, **zero** `spark.sparkContext` calls, and **zero** hardcoded `/tmp/` writes.
3. **Zero Bloat**: Never commit `notebooks/`, `scripts/`, `apps/`, `*.csv`, `*.parquet`, `*.orc`, `*.avro`, `*.ipynb`, `Dockerfile*`, `*devcontainer*`, or any file > 500 KB.

---

## 2. CI/CD Quality Gates & Mistake Prevention

- **Zero-JVM Static Pre-Flight Gate**: Before pushing or finalizing changes, always run:
  ```bash
  python -m compileall -q .
  ruff check .
  pytest tests/
  ```
- **52 Whiteboard Blueprint Invariant**: `assets/whiteboards/` strictly enforces `len(assets/whiteboards/*.webp) == 52`. Never add `.jpg` files or non-canonical aliases (`enterprise_edge_client_architecture`, `data_edge_client_architecture`, `open_file_formats_storage`, `open_table_formats_lakehouse`).
- **Mermaid Diagram Parser Rules**:
  - **No Unquoted Parentheses in Link Labels**: Use `-->|"Label (text)"|` or `-->|Label: text|`.
  - **No Raw Comparison Operators**: Never use `<`, `>`, or `<=` in labels; spell out (`under 100 GB`) or escape as `&lt;` / `&gt;`.
  - **No Semicolons in `sequenceDiagram` Notes**: Use commas or colons (`Note over Sorter: Memory full, spilling to disk`).
  - **Escape Literal Dollar Signs**: Use `\$` in prose and diagrams to prevent math-mode parsing conflicts.
- **Markdown Relative Link Resolution**: Verify link paths relative to the document's directory depth (`docs/ROADMAP.md` uses `../01_foundations/...` and `../topics/README.md`). For cross-repo links, use canonical GitHub URLs.
- **Python Pip Cache Invariant**: Workflows with `cache: "pip"` require a valid root `requirements.txt`.
- **Design & Accessibility Standards**: Use `Inter` + `JetBrains Mono`, WCAG 2.2 AA contrast ($\ge 4.5:1$ normal text), and natural text wrapping (`overflow-wrap: anywhere`, `text-wrap: pretty`) for any web/Pages output.
