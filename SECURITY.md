# Security Policy & Vulnerability Disclosure

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.1.x (Latest) | :white_check_mark: |
| 1.0.x | :white_check_mark: |
| < 1.0.0 | :x: |

## Reporting a Vulnerability

We prioritize the security and confidentiality of our data systems, curriculum, and users. If you discover a vulnerability or potential security flaw:

- **Security Contact**: [santu.jammi@gmail.com](mailto:santu.jammi@gmail.com)
- **Security Advisor**: Modern Data Engineering Ecosystem (`https://moderndataengineer.uk/.well-known/security.txt`)

Please do **NOT** open public issues or pull requests for security vulnerabilities.

### Submission Guidelines

When reporting an issue, please include:
1. A clear description of the vulnerability, affected components, or parameters.
2. Step-by-step reproduction instructions or a minimal proof of concept (PoC).
3. The potential impact of the issue (e.g., privilege escalation, data exposure, credential leakage).

### Response Timeline

- **Initial Acknowledgment**: Within 48 hours
- **Triage & Remediation Plan**: Within 7 business days
- **Coordinated Disclosure**: 90-day coordinated disclosure period before public reporting

### Hardening Invariants

This repository enforces automated secret scanning via `.gitleaks.toml`, dependency vulnerability audits via GitHub Actions, and zero-credential commit policies. All sample secrets in exercises, tests, and documentation must use RFC-compliant mock identifiers.
