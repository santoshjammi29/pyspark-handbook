# Spark Web UI & EventLog Performance Analysis Guide

This guide details how to navigate the Spark Web UI, interpret stage timelines, analyze executor metrics, and diagnose performance regressions from event logs.

---

## 1. Key Spark UI Tabs & Metrics

### 1.1 "Jobs" & "Stages" Tabs
- **Duration vs. Total Task Time**:
  If a stage duration is 10 minutes, but "Task Time (all executors)" is 40 hours, the job has high parallelism. If stage duration is 10 minutes and task time is 11 minutes on a 16-core cluster, executors are severely underutilized or idle.
- **Task Duration 75th vs. Max Percentile (Skew Detection)**:
  - If Median task duration = 5 seconds, 75th percentile = 7 seconds, but Max task duration = 25 minutes:
    **You have a severe data skew issue on the max task partition**.

### 1.2 "Spill (Memory)" vs "Spill (Disk)"
- **Spill (Memory)**: The uncompressed size of data in RAM that could not fit into the execution memory fraction (`spark.memory.fraction * (1 - spark.memory.storageFraction)`).
- **Spill (Disk)**: The serialized, compressed size written to local executor SSD.
- **Rule of Thumb**:
  - Memory Spill : Disk Spill ratio is typically 2.5:1 to 4:1 (due to Snappy/LZ4 compression).
  - Any disk spill > 5 GB per stage demands increasing `spark.sql.shuffle.partitions`.

### 1.3 "Executors" Tab
- **Task Time vs. GC Time**:
  - If **GC Time** exceeds 10%–15% of total task time, the JVM is spending excessive CPU cycles reclaiming heap objects.
  - Fix by switching to G1GC (`-XX:+UseG1GC`) or shifting data structures to Tungsten off-heap.
- **Shuffle Read & Write**:
  - Check whether shuffle bytes are balanced across executors. If Executor 02 processes 50 GB and Executor 03 processes 500 MB, input partitions or join keys are severely skewed.

---

## 2. Reading Execution Plans with `explain()`

```python
# Detailed formatted plan
df.explain(mode="formatted")

# Cost-based plan (with table statistics)
df.explain(mode="cost")

# Code generation plan (Tungsten Java source)
df.explain(mode="codegen")
```

### Critical Plan Operators to Watch:
- **`PhotonScan` / `FileScan`**: Check `PushedFilters: [...]` and `PartitionFilters: [...]`. If filters are missing, predicate pushdown did not occur (e.g. Due to casting functions on partition columns).
- **`Exchange`**: Represents an expensive network shuffle (`hashpartitioning(...)` or `roundrobinpartitioning(...)`). Look to minimize unnecessary exchanges.
- **`BroadcastExchange`**: Driver downloads data and broadcasts to executors. Check that input size is small.
- **`SortMergeJoin` vs `BroadcastHashJoin`**: If one table is small (<100MB), verify why it was not converted to `BroadcastHashJoin`.
- **`BroadcastNestedLoopJoin`**: **DANGER**. Indicates non-equi join conditions (e.g. `t1.id > t2.id`) causing $O(M \times N)$ cartesian product comparisons.
