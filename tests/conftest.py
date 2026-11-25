"""
Pytest configuration and shared fixtures.
"""

from pathlib import Path
import pytest

BASE_DIR = Path(__file__).resolve().parent.parent

@pytest.fixture(scope="session")
def base_dir():
    return BASE_DIR
