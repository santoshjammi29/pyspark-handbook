# 🏛️ Architecture 04: Structured Streaming & Stateful Stream Processing

> **Focus**: Micro-Batch vs. Continuous Processing, Event-Time Watermarking, Stateful Aggregations & Stream-Stream Joins, RocksDB State Store, Checkpointing, and Exactly-Once Guarantees.

---

## 1. Executive Summary & Structured Streaming Philosophy

**Apache Spark Structured Streaming** extends the Catalyst optimizer and Tungsten execution engine to unbounded data streams:
- Treats a live data stream as an **unbounded, continuously appending table**.
- Developers write standard declarative DataFrame / SQL queries. The streaming engine executes the query incrementally using micro-batches (default) or sub-millisecond continuous processing.
- Delivers end-to-end **Exactly-Once Fault-Tolerance** via deterministic replayable sources (Kafka, AWS Kinesis, Delta Lake Auto Loader), idempotent sinks (Delta Lake, ACID storage), and WAL (Write-Ahead Log) checkpointing.

```mermaid
flowchart TD
    subgraph Ingestion["1. Replayable Streaming Ingestion"]
        Kafka["Apache Kafka / Kinesis / Auto Loader<br/>(Partitioned, Offset-Addressable, Replayable)"]
    end

    subgraph Engine["2. Incremental Catalyst Stream Execution Engine"]
        WAL["Write-Ahead Log (WAL)<br/>(Driver Records Offsets to Checkpoint Storage)"]
        
        subgraph MicroBatch["Micro-Batch Execution (Trigger: 500ms - 10s)"]
            MicroPlan["Incremental Catalyst Query Plan<br/>(Reads Offset Range: N to N+K)"]
            Watermark["Event-Time Watermark Engine<br/>max(event_time) - delay_threshold"]
            StateStore["State Store Provider<br/>(HDFS In-Memory vs. RocksDB Off-Heap)"]
        end
    end

    subgraph Sink["3. Idempotent ACID Lakehouse Storage"]
        DeltaSink["Delta Lake Table (ACID Append / MERGE)<br/>(Tracks Streaming Commit ID in _delta_log)"]
    end

    Kafka --> WAL
    WAL --> MicroPlan
    MicroPlan <--> StateStore
    MicroPlan --> Watermark
    Watermark --> StateStore
    MicroPlan -->|"Idempotent Write"| DeltaSink
```

---

## 2. Event-Time Processing & The Watermark Mechanism

In distributed systems, **Event Time** (when the event occurred at the sensor/client) differs significantly from **Processing Time** (when the record arrives at the Spark cluster) due to network jitter, mobile offline mode, and upstream partition backlog.

### How Watermarking Operates:
To prevent intermediate state memory from growing infinitely in stateful aggregations (e.g., `groupBy(window($"timestamp", "10 minutes")).count()`), Spark tracks an **Event-Time Watermark**:

$$\text{Watermark} = \max(\text{Event Time seen so far}) - \text{Delay Threshold}$$

```mermaid
sequenceDiagram
    participant Source as Stream Source (Kafka)
    participant Spark as Spark Engine
    participant State as RocksDB State Store
    participant Sink as Delta Lake Sink

    Note over Spark: Trigger Batch 1 (Delay Threshold: 10 mins)
    Source->>Spark: Events with timestamps: 12:00, 12:05, 12:12
    Note over Spark: max(event_time) = 12:12. Watermark = 12:12 - 10m = 12:02
    Spark->>State: Store aggregates for window [12:00 - 12:10] and [12:10 - 12:20]
    
    Note over Spark: Trigger Batch 2
    Source->>Spark: Event with timestamp 12:01 (Late by 11 mins)
    Note over Spark: Timestamp 12:01 < Watermark (12:02) -> DROPPED AS TOO LATE!
    Source->>Spark: Events with timestamps: 12:25, 12:28
    Note over Spark: max(event_time) = 12:28. New Watermark = 12:28 - 10m = 12:18
    Spark->>State: Window [12:00 - 12:10] is older than 12:18 -> EVICTED FROM STATE!
    Spark->>Sink: Emit finalized window [12:00 - 12:10] in Append Mode
```

---

## 3. State Store Internals: HDFS In-Memory vs. RocksDB State Store

In stateful streaming (session windows, stream-stream joins, deduplication, running metrics), Spark maintains state across micro-batches:

### 1. Default HDFS (Java Heap) State Store Provider
- Stores state records as standard JVM objects in the executor heap (`HDFSBackedStateStoreProvider`).
- **Critical Failure Mode**:
  - As state expands to millions of keys, the JVM heap experiences violent garbage collection thrashing.
  - Executors crash with `java.lang.OutOfMemoryError: Java heap space` or GC pause heartbeat timeouts.
  - State size is strictly limited by executor RAM (typically < 10–20 GB state per cluster).

### 2. Enterprise RocksDB State Store Provider
- Available in Spark 3.2+ and Databricks Runtime:
- Stores state off-heap in a high-performance, embedded C++ key-value store (**RocksDB**) written to local NVMe SSDs.
- Only a configurable LRU memory cache is held in RAM; the rest spills seamlessly to SSD without JVM garbage collection pressure!
- Backed by asynchronous background snapshot uploads to cloud storage checkpoint directories.
- Can easily handle **terabytes of state** (hundreds of millions of active keys) with flat, predictable sub-millisecond latencies.

### Configuration to Enable RocksDB in PySpark:
```python
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
)
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "true")
```

---

## 4. End-to-End Exactly-Once Guarantees & Checkpointing

True Exactly-Once semantics require coordination across the entire streaming pipeline:

| Component | Responsibility for Exactly-Once |
| :--- | :--- |
| **Source** (Kafka, Delta) | Must be offset-addressable and replayable. If an executor fails at offset 450,000, Spark re-requests data starting exactly from offset 450,000. |
| **Engine** (Structured Streaming) | Uses **Write-Ahead Logging (WAL)**: records the offset range for micro-batch $N$ to durable storage (`checkpoint/offsets/`) *before* executing the transformations. |
| **State Store** (RocksDB) | State mutations are versioned atomically by micro-batch ID (`checkpoint/state/0/0/12.delta`). Replaying batch 12 rolls back state to version 11 before re-executing. |
| **Sink** (Delta Lake) | Must be **idempotent**. Delta Lake writes streaming commits transactionally using the micro-batch ID as the atomic transaction identifier in `_delta_log/00000000000000000012.json`. If batch 12 retries, Delta recognizes the duplicate batch ID and discards re-writes! |

---

## 5. Stream-Stream Joins Architecture & Watermark Symmetric Rules

Joining two live streams (e.g., matching ad impressions with ad clicks) requires buffering unmatched events on both sides:

```mermaid
flowchart LR
    subgraph StreamA["Stream A (Impressions)"]
        IA["Event: Impression<br/>Ad ID: 101<br/>Time: 14:00"]
    end

    subgraph StreamB["Stream B (Clicks)"]
        CB["Event: Click<br/>Ad ID: 101<br/>Time: 14:15"]
    end

    subgraph Buffer["State Store Join Buffer (RocksDB)"]
        BufA["Unmatched Impressions Buffer<br/>Retention: Watermark A + Interval"]
        BufB["Unmatched Clicks Buffer<br/>Retention: Watermark B + Interval"]
    end

    subgraph Matched["Output Matched Stream"]
        Out["Joined Tuple: [Ad 101, Imp: 14:00, Click: 14:15]"]
    end

    IA --> BufA
    CB --> BufB
    BufA <-->|"Evaluates Join Condition & Time Constraints"| BufB
    BufA --> Out
    BufB --> Out
```

### The Invariable Rules of Stream-Stream Joins:
1. **Both streams must define watermarks**:
   ```python
   impressions = impressions.withWatermark("impressionTime", "2 hours")
   clicks = clicks.withWatermark("clickTime", "3 hours")
   ```
2. **The join condition must specify an event-time range constraint**:
   ```python
   # Guarantees state store eviction once the 1-hour click window expires!
   joined = impressions.join(
       clicks,
       expr("""
           impressionId = clickId AND
           clickTime >= impressionTime AND
           clickTime <= impressionTime + interval 1 hour
       """),
       joinType="inner" # or leftOuter
   )
   ```
   Without this time window constraint, Spark cannot determine when an impression will never receive a click, causing the state store to **leak memory indefinitely until executor OOM crash**!
