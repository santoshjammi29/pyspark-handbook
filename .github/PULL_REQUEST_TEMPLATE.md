## Description

<!-- Provide a concise summary of the change, motivation, and target module/component. -->

## Type of Change

- [ ] 🐛 Bug fix (content correction or tool fix)
- [ ] ✨ New exercise / drill
- [ ] 📝 Documentation update (guides, whiteboards, runbooks)
- [ ] ⚙️ Tooling / CI/CD enhancement

## Code-Test-Doc Triad Checklist

In accordance with [`CONTRIBUTING.md`](../CONTRIBUTING.md), verify the following invariants:

- [ ] **1. Triad Compliance**: Does the change provide theoretical mental model, runnable code, and automated test assertions?
- [ ] **2. Automated Test Assertions**: Does the exercise include automated tests or asserts verifying behavior?
- [ ] **3. Anti-Pattern Demonstrated**: Is there a side-by-side `Bad` vs `Good` comparison with operational consequences?
- [ ] **4. Tests Passing**: Local tests (`python3 scripts/run_tests.py` or test runner) pass 100%.
- [ ] **5. Secret Sanitization**: No credentials, tokens, or live cloud resource identifiers are committed.
