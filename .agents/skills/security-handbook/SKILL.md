---
name: security-handbook
description: >
  Enterprise cloud perimeter, data pipeline, and lakehouse security engineering. Activate for Zero Trust architecture, keyless OIDC workload identity federation, cloud network isolation (Private Endpoints, Private Link, egress firewalls), envelope encryption & key management (Azure Key Vault, AWS KMS), DevSecOps CI/CD supply chain security (Trivy, Cosign, SLSA, SBOM), Python secure coding & cryptography, ADF managed identities, Databricks Unity Catalog dynamic masking & RLS, and incident triage runbooks.
metadata:
  short-description: Enterprise cloud, data pipeline & lakehouse security engineering
---

# Security Handbook

Use this skill for production-grade security engineering, cloud perimeter defense, keyless workload identity federation, cryptography, DevSecOps supply-chain hardening, data processing pipeline security, and incident response runbooks across cloud and lakehouse platforms.

The canonical repository is mirrored at **[santoshjammi29/security-handbook](https://github.com/santoshjammi29/security-handbook)** and locally at `file:///Users/santosh/Documents/antigravity/security-handbook`.

---

## 1. The 10 Core Production Security Modules (01–10)

1. **Module 01: Authn, OIDC & Zero Trust Architecture**:
   - Zero Trust foundational principles: "Never Trust, Always Verify", assume breach, least privilege.
   - Workload Identity Federation (GitHub Actions / GitLab CI to Azure, AWS, and GCP): Keyless deployment exchanging short-lived OpenID Connect (OIDC) JWT claims for cloud tokens without long-lived secret storage.
   - Enterprise Service Principal lifecycle, certificate-based credential rotation, Microsoft Entra ID Conditional Access.
2. **Module 02: Authz, RBAC, ABAC & Lakehouse Governance**:
   - Role-Based Access Control (RBAC) vs Attribute-Based Access Control (ABAC) with security tags.
   - Privileged Identity Management (PIM) with Just-in-Time (JIT) role activation and peer approvals.
   - Databricks Unity Catalog 3-level securable hierarchy (`catalog.schema.table_or_volume`), ownership transfer, and fine-grained `GRANT / REVOKE` semantics.
3. **Module 03: Cloud Networking & Perimeter Defense**:
   - Hub-and-Spoke Virtual Network (VNet/VPC) topologies with central egress inspection.
   - Azure Private Endpoints & AWS PrivateLink: Eliminating public internet endpoints for ADLS Gen2, Blob Storage, S3, RDS, and Key Vault.
   - Network Security Groups (NSGs), Application Security Groups (ASGs), and User-Defined Routes (UDRs) forcing egress through next-hop Azure Firewall or Palo Alto appliances.
   - Databricks Front-end and Back-end Private Link isolation with dual workspace subnets.
4. **Module 04: Secrets Management & Envelope Encryption**:
   - Secrets management: Azure Key Vault, AWS Secrets Manager, HashiCorp Vault.
   - Envelope Encryption Architecture: Data Encryption Key (DEK) generated locally to encrypt large payloads using AES-256-GCM, wrapped by a Key Encryption Key (KEK) residing in an HSM-backed KMS.
   - Customer-Managed Keys (CMK) vs Platform-Managed Keys (PMK), automated key version rollover, and cryptographic erasure.
5. **Module 05: DevSecOps, CI/CD & Supply Chain Security**:
   - Automated secret leak prevention: Gitleaks and Trufflehog git pre-commit hooks and CI gates.
   - Vulnerability scanning: Static Application Security Testing (SAST), Software Composition Analysis (SCA via Dependency-Check), and container scanning (Trivy).
   - Supply-chain integrity: Software Bill of Materials (SBOM via Syft), cryptographic container signing (Cosign / Sigstore), and SLSA Level 3 provenance attestations.
6. **Module 06: Python Secure Coding & Cryptography**:
   - Production cryptographic implementations using `cryptography` library (AES-256-GCM, Fernet symmetric encryption, RSA-4096 asymmetric key pairs).
   - Parameterized SQL execution preventing SQL injection (`cursor.execute(sql, params)`).
   - Server-Side Request Forgery (SSRF) defense: URL validation, DNS pinning, private IP range blocking (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `169.254.169.254` cloud metadata endpoints).
   - Strict runtime schema validation using Pydantic v2.
7. **Module 07: ETL/ELT & Pipeline Security**:
   - Data pipeline isolation: Write-only landing buckets with non-reusable write permissions.
   - Transient staging lifecycle: Automated object expiration, zero-day staging sanitization.
   - Column-level tokenization and hashing before landing in Silver layer.
   - End-to-end data lineage tracking and audit logging.
8. **Module 08: Azure Data Factory (ADF) & Integration Security**:
   - Credential-less ADF architecture: System-Assigned and User-Assigned Managed Identities (SAMI/UAMI).
   - Integration Runtime (IR) hardening: Self-Hosted Integration Runtime (SHIR) network placement inside on-premises DMZ or secure spoke VNet.
   - Secure Input / Secure Output toggles preventing secret leakage in ADF pipeline activity run history.
9. **Module 09: Databricks Unity Catalog Security**:
   - Dynamic Column Masking (`CREATE FUNCTION mask_pan ... RETURN CASE WHEN is_account_group_member('fraud_analysts') THEN val ELSE concat('***-**-', right(val, 4)) END;`).
   - Dynamic Row-Level Security (RLS) filters: Restricting row visibility based on session user identity (`is_account_group_member(region)`).
   - Auditing with `system.access.audit` and table lineage forensics with `system.access.table_lineage`.
10. **Module 10: Incident Triage, Auditing & Regulatory Compliance**:
    - Compliance frameworks: SOC2 Type II, ISO 27001, HIPAA, GDPR Right to be Forgotten.
    - Security Information and Event Management (SIEM): Azure Sentinel, Datadog Cloud SIEM, AWS GuardDuty.
    - Automated incident playbooks and forensic evidence preservation.

---

## 2. The 6 Battle-Tested Incident Response Runbooks

1. **Runbook 01: Compromised Credential & Secret Revocation**:
   - Immediate invalidation of leaked Azure Service Principal keys, PAT tokens, or AWS IAM access keys.
   - Automated blast-radius audit querying cloud audit logs for all actions taken with compromised token in preceding 72 hours.
   - Regeneration and keyless migration.
2. **Runbook 02: Storage Account Data Exfiltration & Public Exposure**:
   - Emergency perimeter lockdown: Disabling public network access (`publicNetworkAccess = 'Disabled'`), revoking SAS keys (`az storage account revoke-delegation-keys`).
   - Private Link verification and access audit.
3. **Runbook 03: Databricks Unauthorized Data Access & PAT Breach**:
   - Workspace admin token revocation via SCIM API, cluster policy enforcement, querying `system.access.audit` for exfiltrated queries.
4. **Runbook 04: Data Pipeline SSRF & SQL Injection Remediation**:
   - Poisoned pipeline quarantine, parameterized rewrite, blocking outbound egress to internal metadata services (`169.254.169.254`).
5. **Runbook 05: Ransomware & Tampered Lakehouse Disaster Recovery**:
   - Delta Lake Time Travel recovery (`RESTORE TABLE catalog.schema.table TO TIMESTAMP AS OF '2026-11-20 00:00:00'`).
   - ADLS Gen2 immutability policies (WORM storage) and failover.
6. **Runbook 06: Zero-Downtime Certificate & Secret Rotation**:
   - Dual-secret overlapping rollover pattern ensuring 100% pipeline uptime during credential transitions.

---

## 3. Core Security Invariants

- **Zero Hardcoded Credentials**: No passwords, API keys, connection strings, or private keys committed to Git repositories.
- **Keyless OIDC Invariant**: All CI/CD pipelines authenticate via short-lived OIDC workload identity federation.
- **Private Isolation Invariant**: All production databases, key vaults, and lakehouse storage endpoints must reside behind private IP networks without public ingress.
- **Envelope Encryption**: High-volume data must be protected using local DEKs wrapped by HSM-backed KMS keys.
- **Least Privilege Access**: All automated jobs run as dedicated service principals with fine-grained RBAC and temporary elevated JIT access.

---

## 4. Reference Repositories & Local Assets

- 📦 **[Security Handbook Repository](https://github.com/santoshjammi29/security-handbook)**: Master security engineering repository with 10 modules, 4 architectures, 6 incident runbooks, and 50 flashcards.
- 📖 **[Local Repository Path](file:///Users/santosh/Documents/antigravity/security-handbook)**: Local directory with verification test suite `python3 scripts/run_tests.py`.
- 📐 **[Whiteboard Blueprints](file:///Users/santosh/Documents/antigravity/security-handbook/docs/whiteboard_diagrams/README.md)**: 52 architecture blueprints mirrored in `assets/whiteboards/`.
