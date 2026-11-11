---
name: pyspark-handbook
description: >
  Build, review, debug, or tune PySpark and lakehouse data engineering work using the bundled production notebook corpus. Use for Spark DataFrames and SQL, Delta Lake, ingestion, Structured Streaming, Unity Catalog, Databricks, or PySpark learning plans. For general Python or pure BI work, use a more specific skill.
metadata:
  short-description: PySpark and lakehouse engineering
---

# PySpark Handbook

Use this skill for production-oriented PySpark and Databricks engineering. The bundled handbook has 105 source notebooks across ten learning tracks. Start with [the notebook catalog](references/notebook-catalog.md), then open only the matching notebook and any supporting configuration under `resources/`.

## Apply the examples to the user's environment

The source notebooks use Databricks notebook `.py` format and span open-source Spark, Delta Lake, and Databricks features. The pinned source README describes a baseline of Apache Spark 3.5+, Databricks Runtime 15.4+ LTS, and Python 3.9–3.12. Treat that as the source snapshot's stated compatibility, not a guarantee for a different runtime.

- Confirm the requested Spark version and platform when the answer depends on version-specific APIs. Keep Databricks-only APIs such as `dbutils`, Auto Loader, Unity Catalog, and Asset Bundles explicit; adapt them before presenting code for plain PySpark.
- Preserve business semantics for nulls, schemas, decimal precision, time zones, late events, deduplication, and replay behavior.
- Prefer Spark SQL built-ins and DataFrame expressions over Python UDFs when they express the same operation. Consider data size, shuffle, skew, partitioning, and storage before proposing a performance change.
- For streaming work, account for checkpoint location, event-time watermark, state, output mode, restart behavior, and late or duplicate data.
- For Delta writes, state the merge keys and update semantics. Treat retention changes, `VACUUM`, overwrites, and other data-removing actions carefully; make the effect clear before recommending execution.
- Use bundled YAML and project files as reference examples for Databricks deployment patterns. Do not assume they match the user's workspace, credentials, or catalog names.

## Use evidence accurately

Treat notebooks as worked examples to adapt, not as proof that code ran in the user's environment. State when a recipe is unexecuted. Only claim performance gains when baseline and after measurements exist for the relevant workload. For current vendor APIs or compatibility claims, verify against current primary documentation when that information can drift.

## Source and license

The source notebooks and configuration files are from the MIT-licensed [PySpark Handbook repository](https://github.com/santoshjammi29/pyspark-handbook). The imported source snapshot and commit are recorded in [UPSTREAM.md](../UPSTREAM.md); the repository license is included as `LICENSE`.
