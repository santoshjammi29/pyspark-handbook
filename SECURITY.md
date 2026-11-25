# Security Policy & Vulnerability Disclosure

## Supported Versions

| Version | Supported |
|---------|-----------|
| `main` (Spark 4.0+ Code-Only Handbook) | :white_check_mark: |
| Legacy `< 2.0` releases | :x: |

## Scope & Architecture

**PySpark Handbook** is a **code-only educational and architectural reference repository** with no live runtime services, no committed datasets, and no production credentials. All static CI checks run without JVM execution or external network access.

## Reporting a Vulnerability

If you discover a security vulnerability, exposed secret, or unsafe pattern in this repository:

- **Security Contact**: [santu.jammi@gmail.com](mailto:santu.jammi@gmail.com)
- **Ecosystem Security Advisory**: `https://moderndataengineer.uk/.well-known/security.txt`

Please do **NOT** open public issues or pull requests for sensitive security disclosures.

### What to Include

1. Affected file path or workflow (e.g., `.github/workflows/*.yml`, `06_platform_adapters/...`).
2. Reproduction steps or minimal proof of concept.
3. Potential impact (e.g., workflow injection, credential exposure, unsafe deserialization).

### Response Timeline

- **Initial Acknowledgment**: Within 48 hours
- **Triage & Remediation**: Within 7 business days
- **Coordinated Disclosure**: 90-day disclosure window

### Hardening Invariants

- Automated secret scanning via `.gitleaks.toml` and dependency auditing (`pip-audit`) in `.github/workflows/security.yml`.
- All sample keys, buckets, and endpoints in code patterns and DAB configs must use RFC-compliant placeholder identifiers.
