# 🗂️ PySpark Technical Flashcard Deck (50 Active Recall Questions)

> **Format**: Question -> Immediate Technical Answer & Rationale
> **Coverage**: Modules 01 through 10 of The PySpark Handbook

---

### 1. What is the fundamental difference between a Transformation and an Action in PySpark?

**Answer:**  
Transformations (e.g. `select`, `filter`, `groupBy`) are lazily evaluated and build the Catalyst execution DAG without executing. Actions (e.g. `count`, `collect`, `write.save`) trigger actual distributed execution on cluster executors.

---

### 2. Why is `df.count()` faster than `len(df.collect())`?

**Answer:**  
`df.count()` executes distributed partial counts on every executor and returns a single integer to the driver. `len(df.collect())` serializes the entire dataset over the network to the driver heap, risking fatal Driver OOM.

---

### 3. What are Narrow vs. Wide dependencies?

**Answer:**  
Narrow dependencies (e.g. `map`, `filter`) allow each parent partition to be used by at most one child partition with zero data shuffling across nodes. Wide dependencies (e.g. `groupBy`, `join`, `distinct`) require an expensive network shuffle to re-partition data by hash key.

---

### 4. What does Catalyst Predicate Pushdown achieve?

**Answer:**  
It pushes filter expressions down to the storage layer, allowing Parquet and Delta Lake to skip irrelevant row groups and files using min/max file statistics before reading bytes from disk.

---

### 5. What is the default value of `spark.sql.shuffle.partitions`, and why is it dangerous?

**Answer:**  
The default is 200. For large datasets (>100GB), 200 partitions cause partitions larger than 500MB that trigger severe Tungsten disk spill. For tiny datasets (<10MB), 200 partitions create massive task scheduling overhead and tiny files.

---

### 6. Explain the difference between `repartition()` and `coalesce()`.

**Answer:**  
`repartition(N)` triggers a full network shuffle to redistribute data evenly across N partitions and can increase or decrease partition count. `coalesce(N)` merges adjacent existing partitions on the same worker without a network shuffle, but can only decrease partition count.

---

### 7. What is Whole-Stage Code Generation (WSCG)?

**Answer:**  
A Tungsten optimization that compiles a pipeline of operators (Scan, Filter, Project, Aggregate) into a single optimized Java for-loop using the Janino compiler, keeping intermediate values in CPU registers rather than virtual iterator calls.

---

### 8. What is an `UnsafeRow` in Project Tungsten?

**Answer:**  
A raw, off-heap or on-heap 8-byte word-aligned binary format that stores DataFrame records without Java object headers or boxing, allowing sorting and hashing directly on raw bytes.

---

### 9. What are the two major pools inside Unified Spark Memory?

**Answer:**  
Execution Memory (used for shuffles, joins, sorts, and aggregations) and Storage Memory (used for caching DataFrames and broadcast variables).

---

### 10. Can Storage Memory evict Execution Memory in Spark's Unified Memory Manager?

**Answer:**  
No. Execution Memory has strict eviction priority: execution can evict cached storage blocks to disk, but storage can never evict active execution memory.

---

### 11. Why is `Spill (Memory)` typically 3x to 5x larger than `Spill (Disk)`?

**Answer:**  
`Spill (Memory)` represents the uncompressed, deserialized UnsafeRow data and hash tables in RAM. `Spill (Disk)` is the serialized, LZ4/ZSTD-compressed bytes written to the local scratch NVMe drive.

---

### 12. What is Adaptive Query Execution (AQE)?

**Answer:**  
A dynamic runtime re-optimizer in Spark 3.x+ that inspects actual shuffle stage statistics to dynamically coalesce partitions, switch sort-merge join to broadcast join, and split skewed join partitions.

---

### 13. What is the default threshold for Broadcast Hash Join (`spark.sql.autoBroadcastJoinThreshold`)?

**Answer:**  
10 MB (10,485,760 bytes). If a table is smaller than this threshold at compile time or runtime (AQE), Spark broadcasts it to all executors.

---

### 14. Why can setting `autoBroadcastJoinThreshold` to 2 GB crash the cluster?

**Answer:**  
The driver must collect, deserialize, and serialize the 2GB table to broadcast it to all executors, which exhausts the driver JVM heap and causes Driver OOM.

---

### 15. What join algorithm is selected for non-equi joins (e.g. `t1.id < t2.id`)?

**Answer:**  
Broadcast Nested Loop Join (BNLJ) or Cartesian Product Join, both of which have O(M * N) quadratic computational complexity.

---

### 16. What is the primary cause of single-task stragglers in Sort-Merge Joins?

**Answer:**  
Data skew: a high-cardinality join key (or accumulation of NULLs) causes a single partition to receive millions of rows while other partitions receive only thousands.

---

### 17. How does two-phase key salting solve severe data skew?

**Answer:**  
It appends a random integer (0 to K-1) to the skewed key in the fact table and replicates the dimension table by exploding an array of [0 .. K-1], distributing the skewed key across K parallel tasks.

---

### 18. What does PySpark's `@pandas_udf` use for data exchange between JVM and Python?

**Answer:**  
Apache Arrow IPC (Inter-Process Communication), which shares tabular memory buffers in a columnar binary format with zero per-row deserialization overhead.

---

### 19. What configuration parameter controls PyArrow batch sizes in PySpark?

**Answer:**  
`spark.sql.execution.arrow.maxRecordsPerBatch` (default 10,000). Lowering this parameter prevents Python worker OOM crashes on wide rows.

---

### 20. What is an Event-Time Watermark in Structured Streaming?

**Answer:**  
A trailing threshold defined as `max(event_time) - delay_threshold`. Any incoming data with an event timestamp older than the watermark is discarded, allowing state store cleanup.

---

### 21. What is the difference between HDFS State Store and RocksDB State Store in Structured Streaming?

**Answer:**  
HDFS state store stores all state objects in the JVM heap, causing GC pauses and OOMs beyond ~20M keys. RocksDB stores state off-heap on local NVMe SSDs, scaling to hundreds of millions of keys with flat latency.

---

### 22. What is Delta Lake's `_delta_log`?

**Answer:**  
An append-only transaction log containing JSON commit files (e.g. `000000.json`) and periodic Parquet checkpoints (`.checkpoint.parquet`) providing serializable ACID transactions atop cloud object storage.

---

### 23. What is Delta Lake Time Travel?

**Answer:**  
The ability to query historical versions or timestamps of a table (e.g. `spark.read.option('versionAsOf', 5).table('orders')`) by reconstructing the transaction log state at that commit.

---

### 24. What does `VACUUM` do in Delta Lake?

**Answer:**  
Recursively deletes data files that are no longer referenced in the latest Delta transaction log state and are older than the retention threshold (default 7 days / 168 hours).

---

### 25. Why does running `VACUUM` with retention 0 hours risk breaking concurrent queries?

**Answer:**  
Concurrent readers running long queries that began before the vacuum will fail with `FileNotFoundException` when reading files committed to earlier versions.

---

### 26. What are Deletion Vectors in Delta Lake 3.x?

**Answer:**  
Merge-on-Read optimization: rather than rewriting an entire 500MB Parquet file to delete 1 row, Delta writes a small binary bitmap tracking deleted row IDs, reducing write amplification by 90%+.

---

### 27. What is Liquid Clustering (`CLUSTER BY`) in Delta Lake?

**Answer:**  
A flexible, non-hierarchical layout that replaces fixed Hive partitioning and Z-Order, adapting data layout dynamically without data rewrites and supporting up to 4 clustering keys.

---

### 28. What is Databricks Auto Loader (`cloudFiles`)?

**Answer:**  
An optimized ingestion streaming source that automatically detects and processes new files landing in cloud storage (S3/ADLS/GCS) using file notifications (SNS/SQS, Event Grid) without expensive directory listing.

---

### 29. What is the purpose of the `_rescued_data` column in Auto Loader?

**Answer:**  
It captures unparseable data, schema mismatches, and unexpected types into a dedicated JSON column instead of dropping records or failing the pipeline.

---

### 30. What is the Delta Change Data Feed (CDF)?

**Answer:**  
A feature that exposes row-level change events (`insert`, `update_preimage`, `update_postimage`, `delete`) between table versions, enabling downstream incremental processing.

---

### 31. What does `df.explain(mode='formatted')` show?

**Answer:**  
A structured, human-readable breakdown of the Physical Plan with operator IDs, output schemas, and asterisks (*) indicating Whole-Stage CodeGen.

---

### 32. What is the difference between `rank()` and `dense_rank()` in analytical window functions?

**Answer:**  
`rank()` leaves gaps in rank numbering after ties (e.g. 1, 2, 2, 4). `dense_rank()` does not leave gaps (e.g. 1, 2, 2, 3).

---

### 33. Why should you avoid using `Window.partitionBy().orderBy()` without a frame specification on unbounded windows?

**Answer:**  
By default, `orderBy` without a frame specifies `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which requires expensive sorting and partial state buffering on every row.

---

### 34. How does `explode()` differ from `posexplode()`?

**Answer:**  
`explode(col)` unpacks an array into individual rows. `posexplode(col)` unpacks the array into two columns: the 0-based position integer (`pos`) and the element value (`col`).

---

### 35. What are PySpark Higher-Order Functions?

**Answer:**  
Native SQL/DataFrame functions (e.g. `transform`, `filter`, `aggregate`, `exists`) that manipulate nested arrays and structs directly inside Catalyst without exploding or writing Python UDFs.

---

### 36. What is the difference between `transform()` and `filter()` on array columns?

**Answer:**  
`transform(col, lambda x: expr)` maps an expression to every element of the array. `filter(col, lambda x: condition)` keeps only elements satisfying the boolean condition.

---

### 37. What is the recommended garbage collector for Apache Spark 3.x / 4.0?

**Answer:**  
Garbage-First Garbage Collector (G1GC) configured with `-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35` to prevent stop-the-world Full GC pauses.

---

### 38. What causes `ExecutorLostFailure: Container killed by YARN for exceeding memory limits`?

**Answer:**  
Non-JVM memory (PyArrow IPC buffers, Python worker processes, native Netty buffers) exceeded `spark.executor.memoryOverhead`.

---

### 39. What is the 5-core executor rule?

**Answer:**  
Configuring `--executor-cores 5` is considered the optimal sweet spot: more than 5 cores per executor degrades HDFS/cloud I/O throughput and increases GC pause times, while 1 core loses multi-threading benefits.

---

### 40. What is Dynamic Partition Pruning (DPP)?

**Answer:**  
A physical optimization where Spark uses the results of filtering a dimension table at runtime to prune partitions of the fact table before scanning.

---

### 41. What is the difference between Sort-Merge Join (SMJ) and Shuffle Hash Join (SHJ)?

**Answer:**  
SMJ shuffles both sides, sorts both sides by join key, and merges them sequentially (spill-resistant). SHJ shuffles both sides, builds an in-memory hash table on the smaller side, and probes it with the larger side (faster, but memory intensive).

---

### 42. How do you configure an idempotent streaming sink in PySpark?

**Answer:**  
Write to a Delta Lake table using `.outputMode('append')` with a persistent checkpoint directory; Delta guarantees each micro-batch is committed exactly once via the transaction log.

---

### 43. What is the purpose of `checkpoint()` vs `persist()` on a DataFrame?

**Answer:**  
`persist()` caches the DataFrame in memory/disk while retaining its lineage DAG. `checkpoint()` truncates the execution DAG completely and saves physical files to distributed storage, preventing StackOverflowError in recursive algorithms.

---

### 44. What is `spark.sql.files.maxPartitionBytes`?

**Answer:**  
The maximum number of bytes to pack into a single input partition when reading files (default 128 MB). Lowering it increases parallelism for CPU-intensive uncompression.

---

### 45. What is the difference between Managed and External tables in Unity Catalog?

**Answer:**  
Managed tables: Unity Catalog manages both metadata and physical data files; dropping the table deletes both. External tables: Unity Catalog manages metadata, but physical files reside in customer-defined storage; dropping the table drops only metadata.

---

### 46. What are Unity Catalog Volumes?

**Answer:**  
Governance objects in Unity Catalog that manage non-tabular data files (e.g. PDFs, images, ML model checkpoints, raw landing files) with ACL permissions.

---

### 47. How do you implement Dynamic Row-Level Security (RLS) in Databricks?

**Answer:**  
Create a Row Filter function using `is_account_group_member()` and attach it to the target table via `ALTER TABLE ... SET ROW FILTER`.

---

### 48. What is Delta UniForm (Universal Format)?

**Answer:**  
A Delta Lake feature that automatically generates Apache Iceberg and Hudi metadata alongside Delta transaction logs, allowing Iceberg and Hudi readers to query Delta tables without data copying.

---

### 49. What is Lakehouse Federation?

**Answer:**  
The ability in Unity Catalog to configure read-only foreign catalogs (Postgres, Snowflake, BigQuery, Redshift) and execute federated queries with query pushdown without data replication.

---

### 50. What is the single most critical FinOps rule for Databricks cluster cost control?

**Answer:**  
Never use All-Purpose interactive compute for scheduled automated production workloads; always execute automated pipelines using Lakeflow Job Compute or Serverless Compute (saving 50% to 70% in DBU costs).

---
