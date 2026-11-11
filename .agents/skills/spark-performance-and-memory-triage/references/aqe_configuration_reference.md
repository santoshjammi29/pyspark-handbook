# Adaptive Query Execution (AQE) Configuration & Tuning Reference

Adaptive Query Execution (AQE) re-optimizes query plans at runtime between stage boundaries based on actual shuffle file statistics.

---

## 1. Core AQE Settings

```properties
# Enable Adaptive Query Execution (Enabled by default in Spark 3.2+)
spark.sql.adaptive.enabled = true

# 1. Dynamic Partition Coalescing
# Automatically merges small post-shuffle partitions into target advisory sizes
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.advisoryPartitionSizeInBytes = 134217728 # 128 MB (Default: 64MB)
spark.sql.adaptive.coalescePartitions.minPartitionSize = 16777216 # 16 MB

# 2. Dynamic Join Strategy Switching
# Converts SortMergeJoin to BroadcastHashJoin if runtime shuffle size is below threshold
spark.sql.adaptive.autoBroadcastJoinThreshold = 104857600 # 100 MB (Default: 10MB)

# 3. Dynamic Skew Join Optimization
# Splits heavily skewed shuffle partitions into smaller sub-partitions
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5 # Partition is skewed if 5x larger than median
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456 # 256 MB minimum size
```

---

## 2. Partition Sizing Heuristic Formula

To calculate the ideal `spark.sql.shuffle.partitions` when AQE is disabled or for baseline initial planning:

$$\text{Ideal Shuffle Partitions} = \frac{\text{Total Input Stage Data (GB)}}{0.128 \text{ GB (Target 128MB Partition Size)}}$$

Example:
- Stage Input Size = 300 GB
- Partitions = $300 / 0.128 \approx 2343 \rightarrow$ Set `spark.sql.shuffle.partitions = 2400`
- Ensure partition count is an exact multiple of total cluster executor CPU cores (e.g. 100 workers $\times$ 4 cores = 400 cores; $2400 = 6 \times 400$) to guarantee even wave distribution without trailing stragglers!
