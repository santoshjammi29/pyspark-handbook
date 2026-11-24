---
name: ecosystem-ci-cd-quality-gate
description: >
  Enforce multi-repository CI/CD invariants, pre-flight test gates, and mistake prevention protocols across all 14 ecosystem repositories. Activate before committing, pushing, or synchronizing changes across the Modern Data Engineering ecosystem, during GitHub Actions troubleshooting, Mermaid diagram rendering, Markdown relative link audits, Python AST validation in notebooks, or repository asset syncing.
metadata:
  short-description: Multi-repository CI/CD quality gates & invariant defense
---

# Ecosystem CI/CD Quality Gate & Mistake Prevention Playbook

This skill governs CI/CD integrity, automated verification gates, and mistake prevention across the **14-repository Modern Data Engineering & Lakehouse Architect Ecosystem** (`moderndataengineer.uk` and sister handbooks).

Any agent or engineer making modifications across these repositories **MUST** adhere to the quality gates and invariants documented below. Never push unverified changes to remote branches.

---

## 1. The 6 Deadly CI Mistake Patterns & Defensive Protocols

### Pattern 1: Asset Count Invariant Divergence (The Whiteboard Trap)
- **Failure Symptom**: Satellite repository CI fails with `Expected 52 whiteboard blueprints, found 56` in `test_runbooks_and_architectures.py`.
- **Root Cause**: Web presentation folders (`public/diagrams/`) retain alias names for backwards compatibility (e.g., `enterprise_edge_client_architecture.webp`, `open_file_formats_storage.webp`). When synchronization scripts copy blindly from `public/diagrams/` to `assets/whiteboards/` across sister repos, the asset count increases from 52 to 56, instantly breaking tests.
- **The Golden Rule**: The single source of truth for whiteboard blueprints is `assets/whiteboards/` (exactly 52 blueprints). Never sync from `public/diagrams/` to sibling repositories. Always prune non-canonical aliases when synchronizing.

```python
# ❌ BAD: Copying from web presentation folder with aliases
source_files = glob.glob("public/diagrams/*.webp")  # 56 files!

# ✅ GOOD: Enforcing the 52 canonical blueprints and pruning aliases
canonical_files = glob.glob("assets/whiteboards/*.webp")  # 52 files
assert len(canonical_files) == 52, f"Expected 52, found {len(canonical_files)}"
canonical_names = {os.path.basename(f) for f in canonical_files}

for existing in glob.glob(f"{sibling_repo}/assets/whiteboards/*.webp"):
    if os.path.basename(existing) not in canonical_names:
        os.remove(existing)  # Prune alias
```

---

### Pattern 2: Mermaid Diagram Syntax Failures
- **Failure Symptom**: GitHub markdown rendering breaks or headless Mermaid CLI/linters throw `Parse error on line X`.
- **Common Triggers**:
  1. **Unquoted Parentheses in Link Labels**: `-->|Label (text)|` fails Mermaid parsing.
     - **Fix**: Use `-->|"Label (text)"|` or `-->|Label: text|`.
  2. **Comparison Operators in Labels**: `-->|< 100 GB|` or `-->|Score > 0.85|` or `<2 min`.
     - **Fix**: Mermaid parses `<` and `>` as HTML tags or arrow heads. Use `under 100 GB`, `Score above 0.85`, or escape as `&lt;` / `&gt;`.
  3. **Semicolons in `sequenceDiagram` Notes**: `Note over Sorter: Memory fills; Unified Pool reports OUT_OF_MEMORY`.
     - **Fix**: Semicolons terminate statements in sequence diagrams. Use commas or colons: `Memory fills, Unified Pool reports OUT_OF_MEMORY`.
  4. **Unescaped Dollar Signs in Labels**: `$0.018/GB` triggers math parsing or diagram errors.
     - **Fix**: Use `\$0.018/GB` or `USD 0.018/GB`.
  5. **Unwrapped Long Node Labels**: Labels exceeding 40 characters without breaks cause wide clipping.
     - **Fix**: Insert `<br/>` tags to wrap labels evenly.

---

### Pattern 3: Broken Relative Markdown Links
- **Failure Symptom**: `scripts/validate_markdown.py` fails in CI with `Broken relative link in docs/... -> '...'`.
- **Root Cause**: Authoring links relative to repository root instead of relative to the file's current directory depth.
  - Inside `docs/audits/remediation_plan.md`:
    - `[Guide](docs/README.md)` resolves to `docs/audits/docs/README.md` (404 / Missing!).
    - `[Contributing](CONTRIBUTING.md)` resolves to `docs/audits/CONTRIBUTING.md` (404 / Missing!).
- **Defensive Protocol**:
  - For intra-repo references from nested directories, calculate the correct relative climb (e.g. `../../docs/README.md` or `../../CONTRIBUTING.md`).
  - For cross-repository or root canonical documents, use full HTTPS GitHub URLs:
    `https://github.com/santoshjammi29/moderndataengineer/blob/main/docs/README.md`

---

### Pattern 4: GitHub Actions `cache: pip` Without `requirements.txt`
- **Failure Symptom**: GitHub Actions fails in `actions/setup-python@v5` with `Cache error: No file found matching requirements.txt`.
- **Root Cause**: Specifying `cache: "pip"` in the workflow when the repository root lacks `requirements.txt`.
- **Defensive Protocol**:
  - Every repository with Python workflows MUST have a valid `requirements.txt` in the root (even if it only contains `pytest>=7.0.0` and `ruff>=0.1.0`).
  - Never enable pip caching in a workflow without testing dependency installation.

---

### Pattern 5: Databricks & PySpark Notebook AST Syntax Errors
- **Failure Symptom**: CI fails with `Python SyntaxErrors detected in notebooks` during `validate_notebooks.py`.
- **Root Cause**: Databricks `.py` source notebooks contain special headers (`# Databricks notebook source`), cell delimiters (`# COMMAND ----------`), and magic lines (`# MAGIC %md`, `# MAGIC %sql`). If code cells contain un-escaped SQL or invalid Python syntax, `ast.parse()` throws a fatal `SyntaxError`.
- **Defensive Protocol**:
  - Always run `python scripts/validate_notebooks.py` locally before committing changes to `databricks-handbook` or `pyspark-handbook`.
  - Ensure all 105 notebooks start with `# Databricks notebook source` and contain `# COMMAND ----------` and `# MAGIC %md`.

---

### Pattern 6: Next.js Hub Build & Sharding Pipeline
- **Failure Symptom**: Next.js Cloudflare Worker build fails with OOM, or Vitest integration tests fail due to missing shards.
- **Root Cause**: Tests expect generated content shards in `.build-artifacts/` or `content/`. Running `npm test` without `npm run content:shard` first results in missing data errors.
- **Defensive Protocol**:
  - Always run the 4-step deployment pipeline in `charming-fermi`:
    1. `npm run content:validate && npm run content:shard`
    2. `npm run typecheck` (`tsc --noEmit`)
    3. `npm test` (`vitest run` across all 62 test files)
    4. Set `NODE_OPTIONS="--max-old-space-size=4096"` for `npm run build:worker`.

---

## 2. Pre-Commit / Pre-Push Verification Matrix

Before running `git push` on ANY repository, execute the matching local validation command:

| Repository | Role | Local Verification Command | Pass Criteria |
|:---|:---|:---|:---|
| **`charming-fermi`** | Web Hub (Next.js) | `npm run typecheck && npm run content:validate && npm run content:shard && npm test` | 0 type errors, 11,360+ valid assets, 62 test files passed |
| **`databricks-handbook`** | 105 Databricks Recipes | `python3 scripts/run_tests.py` | 15 passed, 0 failed, 52 whiteboards |
| **`pyspark-handbook`** | 105 PySpark Code Drills | `python3 scripts/run_tests.py` | 15 passed, 0 failed, 52 whiteboards |
| **`sql-handbook`** | 105 SQL Exercises | `python3 scripts/run_tests.py` | 9 passed, 0 failed, 52 whiteboards |
| **`streaming-handbook`** | Kafka & Flink | `python3 scripts/run_tests.py` | 7 passed, 0 failed, 52 whiteboards |
| **`dataops-cicd-handbook`**| GitOps, IaC, CI/CD | `python3 scripts/run_tests.py` | 7 passed, 0 failed, 52 whiteboards |
| **`data-system-design`** | Distributed Storage | `python3 scripts/run_tests.py` | 7 passed, 0 failed, 52 whiteboards |
| **`security-handbook`** | Zero-Trust & Identity | `python3 scripts/run_tests.py` | 7 passed, 0 failed, 52 whiteboards |
| **`dbt-handbook`** | Analytics Engineering | `python3 scripts/run_tests.py` | 7 passed, 0 failed, 52 whiteboards |
| **`azure-cheat-sheet`** | Azure & Fabric | `python3 scripts/run_tests.py` | 6 passed, 0 failed, 52 whiteboards |
| **`azure-databricks`** | Project Blueprints | `python3 scripts/run_tests.py` | 11 passed, 0 failed |
| **`enterprise-banking`** | Risk & ALM Lakehouse | `python3 scripts/run_tests.py` | 10 passed, 0 failed |
| **`spark-experiments`** | Skew & Spill Lab | `python3 scripts/run_tests.py` | 3 passed, 0 failed |

---

## 3. Automated Ecosystem Health Audit Script

Run `scripts/verify_ecosystem_ci.py` from `charming-fermi` to perform a zero-latency check of all 14 repositories:

```bash
python3 scripts/verify_ecosystem_ci.py
```

This verifies:
1. Every repository has exactly 52 whiteboard blueprints in `assets/whiteboards/`.
2. Every test runner (`run_tests.py`, `validate_notebooks.py`, `validate_markdown.py`) passes 100%.
3. No repository has uncommitted unstaged drift or dirty states before syndication.
