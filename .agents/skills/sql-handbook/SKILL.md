---
name: sql-handbook
description: >
  Master ANSI SQL query tuning, relational database internals, and modern NoSQL architectures. Activate for window functions, CTEs, indexing (B-Tree, GIN, GiST, BRIN), query execution plans (EXPLAIN ANALYZE), profiling (pg_stat_statements, Query Store), PgBouncer connection pooling, NoSQL taxonomy (Document, KV, Wide-Column, Graph, Vector, Time-Series), specialized SQL engines (DuckDB, ClickHouse, pgvector), and production gotchas.
metadata:
  short-description: SQL & NoSQL engineering, query tuning, indexing & database internals
---

# SQL & NoSQL Handbook

Use this skill for production-grade database engineering, SQL query optimization, relational storage internals, and modern NoSQL distributed architecture. The bundled handbook provides 105 comprehensive production SQL exercises across eight mastery tiers, deep-dive architectures, connection pooling and query profiling runbooks, a complete NoSQL taxonomy, and a battle-tested Top 25 production gotchas checklist.

The canonical repository is mirrored at **[santoshjammi29/sql-handbook](https://github.com/santoshjammi29/sql-handbook)** and locally at `file:///Users/santosh/Documents/antigravity/sql-handbook`.

---

## 1. The 8 SQL Mastery Tiers (Exercises 001–105)

1. **Tier 1: Core Foundations & Relational Operators (001–015)**:
   - Three-valued boolean logic (`TRUE`, `FALSE`, `UNKNOWN`), `NULL` comparison invariants (`IS NULL` vs `= NULL`).
   - Grouping operations, multi-column `GROUP BY`, `HAVING` vs `WHERE` execution order, aggregate functions (`COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`).
   - Equi-joins, theta-joins, self-joins, cross-joins, and anti-joins (`NOT EXISTS` vs `NOT IN` with nulls).
2. **Tier 2: Advanced Window Functions & Analytical Framing (016–030)**:
   - Ranking functions: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`.
   - Offset functions: `LEAD()`, `LAG()`, `FIRST_VALUE()`, `LAST_VALUE()`, `NTH_VALUE()`.
   - Analytical window frames: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, `RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW`, cumulative running totals, moving 7-day averages.
   - Gaps and islands analysis, sessionization via lag delta thresholds.
3. **Tier 3: CTEs, Recursion & Hierarchical Trees (031–045)**:
   - Common Table Expressions (CTEs), readability vs physical execution (materialized vs inlined CTEs in PostgreSQL 12+).
   - Recursive CTEs (`WITH RECURSIVE ... UNION ALL ...`): base anchor, recursive member, termination conditions.
   - Hierarchical data traversals: organizational charts, bill-of-materials (BOM), threaded discussion trees, path enumeration (`ARRAY[id]`), cycle detection (`CYCLE id SET is_cycle USING path`).
4. **Tier 4: Indexing Strategies & Physical Schema Design (046–060)**:
   - B-Tree internals: balanced tree traversal, root-to-leaf page splits, fill factor, leftmost prefix rule for composite indexes `(col_a, col_b)`.
   - Specialized index types: Hash indexes (exact equality), GIN (Generalized Inverted Index for JSONB/arrays/full-text), GiST (Generalized Search Tree for geospatial/ranges), BRIN (Block Range Index for append-only timestamp series).
   - Covering indexes (`INCLUDE (col_c)`) enabling pure Index-Only Scans without table heap lookups.
   - Partial indexes (`CREATE INDEX ... WHERE is_active = true`), expression indexes (`CREATE INDEX ... ON LOWER(email)`).
5. **Tier 5: Query Execution Plans & SARGability (061–075)**:
   - Reading `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON)`: actual vs estimated rows, buffer cache hits vs read IO.
   - Physical join algorithms: Nested Loop Join (small outer, indexed inner), Hash Join (in-memory hash table on build input), Sort-Merge Join (presorted large streams).
   - SARGable (Search Argument Able) predicates: avoiding function-wrapped columns e.g. `WHERE DATE(created_at) = '2026-01-01'` (triggers Seq Scan) -> replace with `WHERE created_at >= '2026-01-01' AND created_at < '2026-01-02'`.
   - Implicit type coercion hazards (`WHERE varchar_col = 123` casts column to numeric, dropping B-Tree index).
6. **Tier 6: Transaction Isolation, Locks & Concurrency (076–090)**:
   - ANSI SQL isolation levels: Read Uncommitted, Read Committed, Repeatable Read, Serializable.
   - Concurrency anomalies: Dirty Read, Non-Repeatable Read, Phantom Read, Serialization Anomaly, Write Skew.
   - Multi-Version Concurrency Control (MVCC) mechanics: `xmin`/`xmax`, vacuuming dead tuples, transaction wraparound prevention.
   - Explicit locking: Shared (`FOR SHARE`), Exclusive (`FOR UPDATE`), Non-blocking (`FOR UPDATE SKIP LOCKED`, `NOWAIT`) for distributed queues.
7. **Tier 7: High-Throughput Database Profiling & Connection Pooling (091–100)**:
   - Query telemetry & hotspot identification: `pg_stat_statements` (PostgreSQL), Query Store & DMV traces (SQL Server), `performance_schema` (MySQL), AWR/ASH (Oracle).
   - Connection pool architecture: Forking cost of process-per-connection vs thread pools.
   - PgBouncer pooling modes: Session Pooling (stateful), Transaction Pooling (stateless per transaction, recommended for API scale), Statement Pooling (single query).
   - PostgreSQL / PgBouncer pool sizing formula:
     $$\text{Max Pool Size} = (\text{CPU Cores} \times 2) + \text{Effective Spindle / SSD IO Count}$$
8. **Tier 8: Production Gotchas, Anti-Patterns & Operational Runbooks (101–105)**:
   - The `NOT IN (SELECT ...)` Null trap returning 0 rows.
   - Pagination performance: Keyset pagination (`WHERE id > :last_id LIMIT 50`) vs Offset degradation (`OFFSET 1000000 LIMIT 50` reading 1M unreturned rows).
   - Deadlock detection, transaction timeout configuration (`statement_timeout = '15s'`, `lock_timeout = '3s'`), connection leakage triage.

---

## 2. NoSQL Comprehensive Taxonomy & Decision Matrix

| Model | Canonical DBs | Best Fit / Strengths | CAP / PACELC | Access Pattern & Schema |
| :--- | :--- | :--- | :--- | :--- |
| **Document** | MongoDB, Couchbase, AWS DocumentDB | Content catalogs, user profiles, hierarchical entities, polymorphic schemas | CP / PA/EC | Key lookups, secondary indexes, JSONB document operators, atomic single-doc writes |
| **Key-Value** | Redis, Valkey, AWS DynamoDB, Aerospike | High-speed cache, session state, leaderboards, feature flags, write-heavy counters | AP or CP / PA/EL | Pure primary key lookups (`GET`, `SET`), sub-millisecond in-memory latencies |
| **Wide-Column** | Apache Cassandra, ScyllaDB, Bigtable | Massive IoT telemetry, financial ledger audits, high write concurrency | AP / PA/EL | Partition key (sharding) + Clustering key (disk sort order), linear horizontal scaling |
| **Graph** | Neo4j, Amazon Neptune, Memgraph | Fraud rings, social networks, knowledge graphs, entitlement graphs | CP or AP / PC/EC | Index-free adjacency, recursive relationship traversal, Cypher / Gremlin queries |
| **Vector** | pgvector, Milvus, Qdrant, Pinecone | LLM RAG pipelines, semantic similarity, image/audio embeddings | Engine dependent | HNSW, IVFFlat graph indices; Cosine, L2/Euclidean, Inner Product distance |
| **Time-Series** | TimescaleDB, InfluxDB, ClickHouse | Real-time APM metrics, market tick data, equipment sensors | Engine dependent | Chunking by timestamp hypertables, automated rollups/downsampling, high write density |

### Distributed Consistency Guardrails
- **CAP Theorem**: Choose between Consistency (C) and Availability (A) during network Partition (P).
- **PACELC Theorem**: If Partition (P), trade off Availability (A) and Consistency (C); Else (E), trade off Latency (L) and Consistency (C).
- **Cassandra Quorum Formula**: Strong consistency achieved when $R + W > N$ (where $R$ = read replicas, $W$ = write replicas, $N$ = replication factor).

---

## 3. Specialized SQL Engine Primitives

- **DuckDB**: Fast in-process columnar analytical engine (OLAP). Queries Parquet, Arrow, and CSV zero-copy. Vectorized C++ execution, out-of-core memory streaming, ideal for local transformations and dbt testing.
- **ClickHouse**: Blazing-fast real-time analytical DBMS. Uses the `MergeTree` family engine with sparse primary indexes, vectorized SIMD aggregations, and petabyte-scale streaming inserts.
- **SQLite**: Zero-configuration embedded transactional database (OLTP). Use WAL (Write-Ahead Logging) mode (`PRAGMA journal_mode=WAL;`) for concurrent read-during-write throughput.
- **TimescaleDB**: PostgreSQL extension transforming standard tables into `hypertables` partitioned automatically across time and space dimensions.
- **pgvector**: Native PostgreSQL vector search extension. Provides `vector` datatype with IVFFlat and HNSW indexes supporting RAG and semantic retrieval in standard SQL.

---

## 4. Top 25 Production Gotchas & Operational Checklist

1. **The `NOT IN` Null Trap**: If the subquery in `NOT IN` contains a single `NULL`, the entire predicate evaluates to `UNKNOWN` and returns zero rows. Always use `NOT EXISTS` or `WHERE col IS NOT NULL`.
2. **Function Wrapping on Indexed Columns**: `WHERE YEAR(tx_date) = 2026` invalidates index. Use `WHERE tx_date >= '2026-01-01' AND tx_date < '2027-01-01'`.
3. **Leading Wildcard LIKE**: `LIKE '%text%'` forces a full table scan. Use full-text search (`tsvector` / GIN index) or trigram indexing (`pg_trgm`).
4. **Implicit Type Casting**: Comparing a `VARCHAR` column to an integer literal casts the column, dropping index usage.
5. **Missing Foreign Key Indexes**: Unindexed foreign keys cause full table locks on parent table `DELETE` or `UPDATE`.
6. **Offset Pagination Degradation**: `OFFSET 500000 LIMIT 20` scans 500,020 rows. Use keyset/cursor pagination: `WHERE id > :last_seen_id ORDER BY id ASC LIMIT 20`.
7. **Connection Pool Starvation**: Applications creating direct PostgreSQL connections exceed OS process limits. Enforce PgBouncer in transaction mode.
8. **Unbounded Queries in APIs**: Missing `LIMIT` clause triggers memory exhaustion on large tables.
9. **Deadlock from Inconsistent Lock Order**: Process A locks table 1 then 2; Process B locks table 2 then 1. Standardize transactional acquisition ordering alphabetically.
10. **Autovacuum Starvation & Wraparound**: Long-running transactions prevent autovacuum from clearing dead tuples, leading to table bloat and transaction ID wraparound emergency shutdowns.
11. **PgBouncer Prepared Statement Hazard**: In transaction pooling mode, named prepared statements fail because server connections are shared. Configure `max_prepared_statements` or use unnamed prepared statements.
12. **Lock Timeout Defense**: Always set `SET lock_timeout = '3s';` before running DDL migrations in production to prevent blocking concurrent OLTP traffic.

---

## 5. Reference Repositories & Local Assets

- 📦 **[SQL Handbook Repository](https://github.com/santoshjammi29/sql-handbook)**: Pinned production repository containing 105 SQL exercises, NoSQL architectures, profiling runbooks, and test suite.
- 📖 **[Local Repository Path](file:///Users/santosh/Documents/antigravity/sql-handbook)**: Master local directory with automated test runner `python3 scripts/run_tests.py`.
- 📐 **[Whiteboard Blueprints](file:///Users/santosh/Documents/antigravity/sql-handbook/docs/whiteboard_diagrams/README.md)**: 52 system architecture schematics mirrored in `assets/whiteboards/`.
