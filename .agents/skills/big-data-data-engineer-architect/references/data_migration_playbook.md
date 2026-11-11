# Data Migration Playbook

## Migration Strategy Types

| Strategy | Description | When to Use | Risk |
|---|---|---|---|
| **Lift & Shift** | Move as-is to cloud | Quick win, cost reduction | High — tech debt carried |
| **Re-platform** | Move with minor changes (e.g., SSIS → ADF) | Modernise tooling, keep logic | Medium |
| **Re-architect** | Full redesign (e.g., OLAP → Lakehouse) | Maximum long-term value | High — longer timeline |
| **Parallel Run** | Both systems live simultaneously | Zero-risk cutover | Very High — double cost |
| **Phased Migration** | Domain-by-domain migration waves | Large enterprise, risk mitigation | Medium — coordination overhead |

---

## Phase 1: Assessment & Discovery (Weeks 1–4)

### Source System Inventory
Complete for every source system:

```markdown
| System Name       | Type (RDBMS/File/API/Stream) |
| Database/Schema   | Table Count                  |
| Total Volume (GB) | Daily Growth (GB/day)        |
| Update Frequency  | CDC Available? (Y/N)         |
| Business Owner    | SLA / Criticality            |
| Dependencies      | Downstream consumers         |
| Sensitive Data?   | PII / PCI / HIPAA fields     |
```

### Data Profiling Checklist
For each table to be migrated:
- [ ] Row count per table
- [ ] Null % per column
- [ ] Distinct value counts (cardinality)
- [ ] Min/Max/Avg for numeric columns
- [ ] Date range of data
- [ ] Duplicate record analysis
- [ ] Referential integrity violations
- [ ] Data type mismatches vs. documented schema
- [ ] Encoding issues (special characters, Unicode)

### Schema Mapping Template
```
Source System  | Source Table    | Source Column | Source Type
Target System  | Target Table    | Target Column | Target Type
Transformation | Notes
```

---

## Phase 2: Architecture & Design (Weeks 3–6)

### Migration Architecture Decisions

**Decision 1: Connectivity**
- On-prem → Azure: ExpressRoute (production) or VPN Gateway (dev/test)
- On-prem → Azure: Self-Hosted Integration Runtime (SHIR) for ADF
- Cloud-to-cloud: Direct via MSI/Service Principal, no SHIR needed

**Decision 2: Migration Tool Selection**

| Source | Target | Recommended Tool |
|---|---|---|
| SQL Server → | Azure SQL / Synapse | Azure Database Migration Service (DMS) |
| Oracle → | Azure SQL / Synapse | SSMA for Oracle + ADF |
| Teradata → | Synapse / Databricks | ADF + Qlik Attunity / Striim |
| On-prem Hadoop → | ADLS Gen2 + Databricks | ADF Copy Activity / Hadoop DistCp |
| Any source → | Fabric / Databricks | Airbyte, Fivetran, ADF |
| Files (CSV/Parquet) → | Any | ADF Copy / ADLS Gen2 upload |

**Decision 3: Cutover Type**
- **Big Bang**: Single cutover weekend — high risk, fast
- **Phased**: Domain-by-domain over months — low risk, slow
- **Parallel Run**: Both systems live — zero risk, highest cost

---

## Phase 3: Wave Planning

### Wave Definition Criteria
- Group tables by **business domain** (Sales, Finance, HR)
- Prioritise by **dependency order** (reference data first, facts last)
- Group by **migration complexity** (simple → complex)

### Sample Wave Plan

| Wave | Domain | Tables | Complexity | Duration | Dependencies |
|---|---|---|---|---|---|
| 0 | Reference / Lookup | Country, Currency, Calendar | Low | 1 week | None |
| 1 | Customer | Customer, Address, Contact | Medium | 2 weeks | Wave 0 |
| 2 | Product | Product, Category, Supplier | Medium | 2 weeks | Wave 0 |
| 3 | Sales | Order, OrderLine, Shipment | High | 3 weeks | Waves 1, 2 |
| 4 | Finance | Invoice, Payment, GL | High | 3 weeks | Wave 3 |
| 5 | Derived/Mart | Fact tables, aggregates | High | 2 weeks | Waves 1–4 |

---

## Phase 4: Build & Test

### Migration Pipeline Pattern (ADF)
```
[Source Extract] → [ADLS Gen2 Landing] → [Data Validation] → [Transform] → [Target Load]
      │                    │                      │                              │
  Source system       Bronze layer         Great Expectations              Silver/Gold
```

### Pre-Migration Validation Checks
1. **Schema validation** — target schema matches expected DDL
2. **Constraint validation** — PK uniqueness, FK references intact
3. **Data type validation** — no truncation, precision loss
4. **Encoding validation** — UTF-8 compatible

### Post-Migration Reconciliation Checks
```sql
-- Row count reconciliation
SELECT 
    'source' AS system, COUNT(*) AS row_count FROM source_db.dbo.orders
UNION ALL
SELECT 
    'target' AS system, COUNT(*) AS row_count FROM target_db.dbo.orders;

-- Checksum reconciliation (for smaller tables)
SELECT CHECKSUM_AGG(CHECKSUM(order_id, customer_id, total_amount)) 
FROM source_db.dbo.orders;

-- Column-level aggregate comparison
SELECT
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM orders;
-- Run same query on source and target; values must match
```

---

## Phase 5: Cutover Execution

### Cutover Runbook Template

```markdown
## Cutover Runbook — [System Name]

### Pre-Cutover (T-48 hours)
- [ ] Final full extract from source
- [ ] Validate target environment is ready
- [ ] Notify all stakeholders of maintenance window
- [ ] Take backup of source system
- [ ] Disable non-critical batch jobs

### Cutover Window (T-0)
- [ ] Put source system in read-only mode
- [ ] Run final incremental/CDC extract
- [ ] Execute final data load to target
- [ ] Run reconciliation scripts — GO/NO-GO
- [ ] Update connection strings in applications
- [ ] Smoke test key user journeys

### Post-Cutover (T+2 hours)
- [ ] Monitor error rates for 2 hours
- [ ] Confirm all downstream reports running
- [ ] Decommission source read connections
- [ ] Send go-live confirmation email

### Rollback Trigger
- Reconciliation fails > 0.1% row count difference
- Any P1 business function unavailable > 30 minutes
- Data corruption detected in target
```

---

## Phase 6: Post-Migration Optimisation

### Performance Tuning Checklist
- [ ] Rebuild indexes / collect statistics on target
- [ ] Partition large tables by date
- [ ] Apply Z-Order / clustering on frequently filtered columns (Delta Lake)
- [ ] Enable result caching (Synapse, BigQuery)
- [ ] Right-size compute resources based on observed query patterns
- [ ] Archive or remove migrated data from source after validation period

### Decommission Criteria
- [ ] All downstream consumers confirmed migrated
- [ ] 30-day parallel run completed with zero discrepancies
- [ ] Data retention obligations met (archive/export)
- [ ] Sign-off from data owner and compliance team
- [ ] Source system decommission ticket raised
