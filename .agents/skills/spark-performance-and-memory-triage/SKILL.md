---
name: spark-performance-and-memory-triage
description: |
  Specialized operational runbook and triage skill for diagnosing and resolving Apache Spark and Databricks production bottlenecks, memory errors, and query execution failures.
  Activate when:
  - Diagnosing Spark Driver OutOfMemoryError (`java.lang.OutOfMemoryError: Java heap space`, GC overhead limit exceeded)
  - Diagnosing Executor OOM / Exit Code 137 / container killed by YARN or Kubernetes for exceeding memory limits
  - Investigating Tungsten Shuffle Disk Spill (`Spill (Memory)` and `Spill (Disk)` in Spark UI) and tuning partition sizing
  - Detecting and eliminating Data Skew using two-phase key salting, AQE skew join splitting, and skewed key isolation
  - Analyzing Spark execution plans (`explain(mode="formatted")`), eliminating BroadcastNestedLoopJoin and accidental Cartesian products
  - Tuning Garbage Collection (GC) pauses and G1GC parameters (`-XX:+UseG1GC`, `-XX:InitiatingHeapOccupancyPercent=45`)
  - Configuring Adaptive Query Execution (AQE) dynamic partition coalescing, join switching, and skew mitigation
  - Resolving small file problems and storage metadata latency in Delta Lake tables

  DO NOT trigger for:
  - General non-distributed database tuning (Postgres, MySQL)
  - Basic Python script errors unrelated to Apache Spark / Databricks
---

# Spark Performance & Production Memory Triage Skill

You are a **Master Spark & Databricks Production Systems SRE / Performance Architect**. This skill provides step-by-step diagnostic workflows, calculation formulas, and surgical code fixes for production Spark and Lakehouse incidents.

---

## 1. When to Activate

Activate immediately when the user reports or pastes:
- A Spark or Databricks job failure with memory or timeout errors.
- Driver OOM (`java.lang.OutOfMemoryError: Java heap space`, `driver heartbeat lost`).
- Executor OOM (`Exit code 137`, `Container killed by YARN/K8s for exceeding memory limits`).
- Severe task skew (e.g. 199/200 tasks finish in seconds, 1 task runs for hours).
- High Tungsten Disk Spill in the Spark UI.
- Inquiries about tuning `spark.sql.shuffle.partitions`, `spark.executor.memoryOverhead`, or AQE parameters.

---

## 2. Immediate Diagnostic Triage Workflow

```text
Step 1: Identify Failure Node & Signature
  ├── Driver Node Died? ───> Inspect code for .collect(), .toPandas(), or oversized broadcast joins.
  └── Executor Node Died? ──> Check task duration distribution (Skew) or off-heap native memory leak.

Step 2: Inspect Spark Web UI Metrics
  ├── Stages Tab: Check "Spill (Memory)" and "Spill (Disk)" columns.
  ├── Tasks Table: Compare 75th percentile duration vs. Max duration.
  └── Executors Tab: Check "Task Time (GC Time)" percentage.

Step 3: Apply Targeted Remediation
  ├── Skew Detected? ────────> Two-phase salting (F.rand()) or enable AQE skew join.
  ├── Spill Detected? ───────> Boost spark.sql.shuffle.partitions to achieve 128MB targets.
  └── Container Killed 137? ──> Increase spark.executor.memoryOverhead to 20-30% of heap.
```

---

## 3. Surgical Code Solutions for Top 4 Bottlenecks

### 1. Eliminating Driver OOM (`java.lang.OutOfMemoryError`)
```python
# Anti-pattern: Pulling distributed dataset into single driver JVM
# rows = df.collect()

# Fix 1: Stream directly to storage
df.write.format("delta").mode("append").save("/Volumes/prod/gold/summary")

# Fix 2: Cap driver collection for inspection
sample_df = df.limit(50).toPandas()

# Fix 3: Lower broadcast threshold if a large table was accidentally broadcasted
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "33554432") # 32MB cap
```

### 2. Eliminating Data Skew via Two-Phase Key Salting
```python
from pyspark.sql import functions as F

# Step 1: Add salt (0 to N-1) to skewed left table
SALT_BUCKETS = 16
df_salted = df_large.withColumn("salt", F.floor(F.rand() * SALT_BUCKETS))

# Step 2: Explode right dimension table with all possible salt values
salt_df = spark.range(SALT_BUCKETS).withColumnRenamed("id", "salt")
df_dim_exploded = df_small.crossJoin(salt_df)

# Step 3: Join on compound key (join_key + salt) to distribute work evenly across all 16 workers
joined_df = df_salted.join(
    df_dim_exploded,
    on=["join_key", "salt"],
    how="inner"
).drop("salt")
```

### 3. Sizing Shuffle Partitions to Eliminate Tungsten Disk Spill
```python
# Target partition size = 128 MB (0.128 GB)
# Formula: Total Stage Shuffle Input (GB) / 0.128 GB = Target Shuffle Partitions
# Example: 250 GB shuffle stage -> 250 / 0.128 = 1953 -> set to 2000 partitions
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# Enable AQE dynamic partition coalescing
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728") # 128MB
```

### 4. Mitigating Executor OOM (Exit Code 137 / Container Killed)
```properties
# Allocate 25% of executor heap as off-heap memory overhead for PyArrow/C++ allocations
spark.executor.memory = 16g
spark.executor.memoryOverhead = 4g

# Enable Tungsten off-heap memory
spark.memory.offHeap.enabled = true
spark.memory.offHeap.size = 4g

# Enable G1GC collector for low-pause heap reclamation
spark.executor.extraJavaOptions = -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45
```

---

## 4. Reference Manuals & Diagnostic Tools

- 🚨 **[Production Triage Decision Matrix](./references/production_triage_decision_matrix.md)**: Fast symptom-to-fix lookup table.
- 📊 **[Spark UI & EventLog Analysis Guide](./references/spark_ui_eventlog_analysis.md)**: Reading stage timelines, executor metrics, and query plans.
- ⚙️ **[AQE Configuration & Tuning Reference](./references/aqe_configuration_reference.md)**: Dynamic coalescing, join switching, and skew join configs.
- 🔍 **[Execution Plan Diagnostic Script](./scripts/diagnose_spark_plan.py)**: Static analyzer detecting BroadcastNestedLoopJoin and Cartesian products.
