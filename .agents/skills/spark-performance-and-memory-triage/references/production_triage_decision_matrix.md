# Spark Production Triage & Bottleneck Decision Matrix

Use this matrix to rapidly diagnose performance issues and failure modes based on observable symptoms in logs and the Spark Web UI.

---

## 1. Fast Failure Identification Matrix

| Symptom / Log Output | Likely Root Cause | Immediate Verification | Recommended Fix |
| :--- | :--- | :--- | :--- |
| `java.lang.OutOfMemoryError: Java heap space` on Driver | Massive `.collect()`, `.toPandas()`, or oversized broadcast join table | Check driver memory graph and code for `.collect()` | 1. Write direct to Delta<br>2. Reduce `autoBroadcastJoinThreshold`<br>3. Increase `spark.driver.memory` |
| `ExecutorLostFailure (exit code 137)` or `Container killed by YARN/K8s for exceeding memory limits` | Executor off-heap / native memory exceeded or partition skew | Check if non-vectorized Python UDFs or JVM off-heap is leaking | 1. Increase `spark.executor.memoryOverhead` to 20-30%<br>2. Enable `spark.memory.offHeap.enabled`<br>3. Salt skewed partitions |
| Single task taking 95% of stage runtime (e.g. 199 tasks done in 1 min, 1 task takes 1 hour) | Data Skew on join or groupBy key | Check Spark UI Tasks table sorted by "Duration" and "Shuffle Read Size" | 1. Two-phase key salting with `rand()`<br>2. Enable `spark.sql.adaptive.skewJoin.enabled`<br>3. Filter out null/sentinel keys |
| `Spill (Memory): 80 GB`, `Spill (Disk): 25 GB` in Spark UI Stage | Shuffle partition size too large; Tungsten execution memory exhausted | Check Spark UI Stage "Aggregated Metrics by Executor" -> Spill columns | 1. Increase `spark.sql.shuffle.partitions`<br>2. Target partition size 100MB-200MB<br>3. Enable AQE coalescing |
| `BroadcastNestedLoopJoin` or `CartesianProduct` in Physical Plan | Missing or non-equi join condition | Run `df.explain(mode="formatted")` and inspect join physical operators | 1. Ensure join predicates use equality (`=`)<br>2. Refactor inequalities into window or range filters |
| High GC Time (>15% of task execution time) | Excessive short-lived object allocation in JVM heap | Check Spark UI "Task GC Time" column | 1. Enable G1GC: `-XX:+UseG1GC`<br>2. Tune `-XX:InitiatingHeapOccupancyPercent=45`<br>3. Increase executor memory |
| Millions of tiny <1MB files in Delta Lake table | High-frequency streaming or uncompacted appends | Run `DESCRIBE DETAIL table` and check `numFiles` vs `sizeInBytes` | 1. Run `OPTIMIZE table`<br>2. Enable auto-compaction and optimized writes<br>3. Migrate to Liquid Clustering |
| `ConcurrentAppendException` or `ConcurrentTransactionException` | Multiple jobs writing to same Delta partition concurrently | Check job schedules and concurrency isolation level | 1. Enable Deletion Vectors<br>2. Migrate to Liquid Clustering<br>3. Ensure partition pruning on MERGE |

---

## 2. Driver OOM vs. Executor OOM Diagnostics

```text
                     ┌───────────────────────────────────┐
                     │ Spark Job Crashed with Memory OOM │
                     └─────────────────┬─────────────────┘
                                       │
            ┌──────────────────────────┴──────────────────────────┐
            ▼                                                     ▼
┌───────────────────────────┐                         ┌───────────────────────────┐
│     Driver Node Failed    │                         │    Executor Node Killed   │
│  "Java heap space" / GC   │                         │  Exit Code 137 / SIGKILL  │
└───────────┬───────────────┘                         └───────────┬───────────────┘
            │                                                     │
   Did code call:                                         Did Spark UI show:
   • df.collect()?                                        • 1 task duration >> others?
   • df.toPandas()?                                         -> Data Skew! (Use Salting)
   • Broadcast join on huge table?                        • High Spill (Disk)?
     -> Replace collect with write                           -> Partition Count too low!
     -> Lower broadcast threshold                         • Container killed by OS/K8s?
                                                             -> Boost memoryOverhead!
```
