# Data Migration Assessment Template

## Project Information
- **Project Name**: _______________
- **Assessment Date**: _______________
- **Lead Architect**: _______________
- **Target Go-Live**: _______________

---

## 1. Source System Inventory

| # | System Name | Type | DB Engine & Version | Tables | Est. Size (GB) | Daily Growth | Owner | Priority |
|---|---|---|---|---|---|---|---|---|
| 1 | | RDBMS/Files/API | | | | | | High/Med/Low |
| 2 | | | | | | | | |

---

## 2. Data Domain Classification

| Domain | Systems | Key Entities | Regulatory? | Migration Wave |
|---|---|---|---|---|
| Customer | CRM, ERP | Customer, Address, Contact | GDPR | Wave 1 |
| Product | ERP, PIM | Product, Category, Price | | Wave 2 |
| Finance | ERP, GL | Invoice, Payment, GL Entry | SOX | Wave 4 |
| Sales | CRM, OMS | Order, Quote, Opportunity | | Wave 3 |

---

## 3. Table-Level Assessment

For each source table:

```
Table Name         : _______________
Source Schema      : _______________
Row Count          : _______________
Size (GB)          : _______________
Update Frequency   : Real-time / Hourly / Daily / Monthly
CDC Available      : Yes / No
PK Column(s)       : _______________
Watermark Column   : _______________
Sensitive Data     : PII / PCI / PHI / None
Null % (key cols)  : _______________
Duplicate %        : _______________
Data Quality Issues: _______________
Migration Complexity: Low / Medium / High
Target Table       : _______________
Transform Required : None / Minor / Major
```

---

## 4. Connectivity & Infrastructure

| Item | Status | Notes |
|---|---|---|
| ExpressRoute / VPN established | | |
| Self-Hosted IR deployed | | |
| Network bandwidth tested | | Mbps measured |
| Firewall rules approved | | |
| Service Principal created | | |
| Key Vault configured | | |
| ADLS Gen2 containers created | | Bronze / Silver / Gold |
| Target compute provisioned | | |

---

## 5. Risk Register

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| Source schema change during migration | Medium | High | Lock source schema post-assessment, add schema validation checks |
| Network throughput insufficient | Low | High | Run bandwidth tests, use staged transfers |
| Data quality worse than profiled | Medium | High | 2-week profiling sprint before go-live |
| CDC not available on source | Low | Medium | Fall back to watermark incremental |
| Key stakeholder unavailability | Medium | Medium | Cross-train backup SME |

---

## 6. Wave Plan

| Wave | Name | Tables | Start | End | Status |
|---|---|---|---|---|---|
| 0 | Reference Data | Country, Currency, Calendar | Week 1 | Week 2 | |
| 1 | Customer Domain | 12 tables | Week 3 | Week 5 | |
| 2 | Product Domain | 8 tables | Week 4 | Week 6 | |
| 3 | Sales Domain | 15 tables | Week 6 | Week 9 | |
| 4 | Finance Domain | 10 tables | Week 8 | Week 11 | |
| 5 | Derived/Marts | 6 tables | Week 11 | Week 13 | |

---

## 7. Cutover Checklist

### Pre-Cutover (T-48h)
- [ ] Full extract completed and validated
- [ ] Target schema verified against DDL spec
- [ ] All reconciliation scripts prepared and tested
- [ ] Stakeholder communication sent
- [ ] Rollback plan reviewed and approved
- [ ] Monitoring dashboards set up

### Cutover Window
- [ ] Source system set to read-only
- [ ] Final delta load executed
- [ ] Reconciliation checks passed (row count, checksum, business KPIs)
- [ ] Application connection strings updated
- [ ] Smoke tests passed (5 key user journeys)
- [ ] Go/No-Go decision confirmed

### Post-Cutover (T+24h)
- [ ] Error rate < 0.1% for 24 hours
- [ ] All reports running without error
- [ ] Data freshness within SLA
- [ ] Performance benchmarks met
- [ ] Decommission plan confirmed with source team

---

## 8. Success Criteria

| KPI | Target | Measurement |
|---|---|---|
| Row count match | ≥ 99.99% | Row count comparison query |
| Checksum match | 100% | Checksum on key financial fields |
| Pipeline SLA | < 4h for full load | Pipeline run duration |
| Query performance | ≤ 110% of baseline | Sample query benchmark |
| Zero data loss | 0 records lost | Source vs. target reconciliation |
| Business KPI match | < 0.01% variance | Finance sign-off on key metrics |
