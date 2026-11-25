"""
Test Suite: Architectures, Runbooks, Interview Guides & Visual Blueprints Integrity
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def test_architectures_exist(base_dir=None):
    base_dir = base_dir or BASE_DIR
    arch_dir = base_dir / "architectures"
    assert arch_dir.exists() and arch_dir.is_dir()
    arch_files = list(arch_dir.glob("*.md"))
    assert len(arch_files) == 4, f"Expected 4 architecture documents, found {len(arch_files)}"
    for f in arch_files:
        content = f.read_text(encoding="utf-8")
        assert len(content) > 1000, f"{f.name} is too short ({len(content)} bytes)"
        assert "```mermaid" in content, f"{f.name} missing Mermaid architectural diagram"


def test_runbooks_exist(base_dir=None):
    base_dir = base_dir or BASE_DIR
    runbook_dir = base_dir / "runbooks"
    assert runbook_dir.exists() and runbook_dir.is_dir()
    runbook_files = list(runbook_dir.glob("*.md"))
    assert len(runbook_files) == 6, f"Expected 6 incident runbooks, found {len(runbook_files)}"
    for f in runbook_files:
        content = f.read_text(encoding="utf-8")
        assert len(content) > 1000, f"{f.name} is too short ({len(content)} bytes)"
        assert "Emergency Decision Tree" in content, f"{f.name} missing Emergency Decision Tree"
        assert "Remediation" in content or "Recovery" in content, f"{f.name} missing remediation section"


def test_interview_guides_exist(base_dir=None):
    base_dir = base_dir or BASE_DIR
    interview_dir = base_dir / "interview"
    assert interview_dir.exists() and interview_dir.is_dir()
    interview_files = list(interview_dir.glob("*.md"))
    assert len(interview_files) == 5, f"Expected 5 interview guides, found {len(interview_files)}"


def test_flashcards_count(base_dir=None):
    base_dir = base_dir or BASE_DIR
    flashcards_file = base_dir / "interview" / "05_flashcards_deck_50.md"
    assert flashcards_file.exists()
    content = flashcards_file.read_text(encoding="utf-8")
    for i in range(1, 51):
        assert f"{i}." in content, f"Missing flashcard question #{i}"


def test_readme_integrity(base_dir=None):
    base_dir = base_dir or BASE_DIR
    readme_file = base_dir / "README.md"
    assert readme_file.exists()
    content = readme_file.read_text(encoding="utf-8")
    assert len(content.encode("utf-8")) < 35 * 1024, "README.md must stay under 35 KB"
    assert "# ⚡ The PySpark Handbook" in content
    for section in (
        "01_foundations",
        "02_production_patterns",
        "03_performance_and_streaming",
        "04_spark4_and_modern_api",
        "05_lakehouse_formats",
        "06_platform_adapters",
    ):
        assert section in content, f"README.md missing section reference: {section}"


def test_whiteboard_blueprints_integrity(base_dir=None):
    base_dir = base_dir or BASE_DIR
    wb_dir = base_dir / "assets" / "whiteboards"
    assert wb_dir.exists() and wb_dir.is_dir()
    webp_files = list(wb_dir.glob("*.webp"))
    assert len(webp_files) == 52, f"Expected 52 whiteboard blueprints, found {len(webp_files)}"

    docs_readme = base_dir / "docs" / "whiteboard_diagrams" / "README.md"
    assert docs_readme.exists()
    content = docs_readme.read_text(encoding="utf-8")
    assert "52" in content
    assert "assets/whiteboards" in content
