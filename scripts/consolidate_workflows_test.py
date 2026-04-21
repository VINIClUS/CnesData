"""Teste do consolidate_workflows — dedup step sequences."""
from scripts.consolidate_workflows import DuplicateStep, find_duplicates


def test_find_duplicates_detecta_install_python_dep() -> None:
    workflows = {
        "ci.yml": [
            {"name": "Install deps", "run": "pip install -e packages/cnes_domain"},
        ],
        "shadow-e2e.yml": [
            {"name": "Install Python deps", "run": "pip install -e packages/cnes_domain"},
        ],
    }
    dups = find_duplicates(workflows)
    assert all(isinstance(d, DuplicateStep) for d in dups)
    assert any(d.run.startswith("pip install -e packages/cnes_domain") for d in dups)
