# Autonomous Agent Operational Guidelines: PySpark Handbook

> **Repository**: [santoshjammi29/pyspark-handbook](https://github.com/santoshjammi29/pyspark-handbook)  
> **Ecosystem Hub**: [moderndataengineer.uk](https://moderndataengineer.uk)  
> **Primary Authority**: `Apache Spark & Distributed Compute`

---

## 1. Architectural Role & Invariants

You are assisting on **PySpark Handbook**, an authoritative satellite repository within the **Modern Data Engineering Lakehouse Ecosystem**.

1. **Design System & Typography**:
   - **Primary Typeface**: Use `Inter` with system fallbacks (`system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif`).
   - **Monospace Family**: Use `JetBrains Mono, Fira Code, monospace` for code blocks and tabular counters.
   - **WCAG 2.2 AA Contrast**: Ensure all text has $\ge 4.5:1$ contrast against light, dark, and e-ink backgrounds.
   - **Natural Text Wrapping**: Avoid arbitrary truncation (`truncate` or `line-clamp`). Use `overflow-wrap: anywhere` and `text-wrap: pretty`.

2. **Code Authoring Standards (The Triad)**:
   - When authoring code samples or notebooks, always provide:
     1. Conceptual explanation with an ASCII/Mermaid architecture diagram.
     2. Clean, idiomatically typed code.
     3. Executable assertions / tests (doctests or pytest).

3. **Anti-Pattern Standards**:
   - Contrast `❌ Bad Practice` with `✅ Good Practice` and quantify the "Otherwise" operational blast radius.

4. **Skill Reference**:
   - Reference the core agent skill `coding-learning-repo-architect` for curriculum structuring, exercise templates, and 4-tier progressive disclosure models.
---

## 2. Multi-Repository CI/CD Quality Gates & Mistake Prevention
- **The Pre-Flight Test Invariant**: Never execute `git push` on `main` without running the local test runner (`python3 scripts/run_tests.py`, `python3 scripts/validate_notebooks.py`, or `pytest tests/`).
- **52 Whiteboard Blueprint Invariant**: The repository strictly enforces `len(assets/whiteboards/*.webp) == 52`. Never add non-canonical alias files (`enterprise_edge_client_architecture`, `data_edge_client_architecture`, `open_file_formats_storage`, `open_table_formats_lakehouse`) into `assets/whiteboards/`.
- **Mermaid Diagram Parser Rules**:
  - **No Unquoted Parentheses in Link Labels**: `-->|Label (text)|` fails syntax parsing. Use `-->|"Label (text)"|` or `-->|Label: text|`.
  - **No Raw Comparison Operators**: Never use `<`, `>`, or `<=` in labels (`-->|< 100 GB|` fails). Spell out (`under 100 GB`, `above 0.85`) or escape as `&lt;` / `&gt;`.
  - **No Semicolons in sequenceDiagram Notes**: In `sequenceDiagram`, `;` terminates the command. Use commas or colons (`Note over Sorter: Memory full, spilling to disk`).
  - **Escape Literal Dollar Signs**: Use `\$` in prose and diagrams to prevent math-mode parsing conflicts.
- **Markdown Relative Link Resolution**: Verify link path relative to the document's directory depth. Never assume repository root. For cross-repo links, use canonical GitHub URLs.
- **Python Pip Cache Invariant**: Workflows with `cache: "pip"` MUST have a valid root `requirements.txt`.
- **Databricks Notebook AST Validation**: All `.py` source notebooks must parse cleanly via `ast.parse()` using `python3 scripts/validate_notebooks.py`.
- **Skill Reference**: Activate the agent skill `ecosystem-ci-cd-quality-gate` for multi-repository invariant defense protocols.
