"""Detecta steps duplicados entre GitHub Actions workflows + sugere dedup.

Conservador: só reporta duplicatas, não modifica automaticamente.

Uso:
    python scripts/consolidate_workflows.py .github/workflows/
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class DuplicateStep:
    run: str
    workflows: list[str]


def find_duplicates(workflows: dict[str, list[dict]]) -> list[DuplicateStep]:
    by_run: dict[str, list[str]] = {}
    for wf_name, steps in workflows.items():
        for step in steps:
            run = step.get("run", "").strip()
            if not run:
                continue
            by_run.setdefault(run, []).append(wf_name)
    return [DuplicateStep(run=r, workflows=wfs) for r, wfs in by_run.items() if len(wfs) > 1]


def _collect_steps(workflow_path: Path) -> list[dict]:
    data = yaml.safe_load(workflow_path.read_text()) or {}
    steps: list[dict] = []
    for job in (data.get("jobs") or {}).values():
        steps.extend(job.get("steps") or [])
    return steps


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("workflows_dir", type=Path)
    args = parser.parse_args()

    workflows: dict[str, list[dict]] = {}
    for wf in args.workflows_dir.glob("*.yml"):
        workflows[wf.name] = _collect_steps(wf)

    dups = find_duplicates(workflows)
    if not dups:
        print("no duplicate steps found")
        return 0

    print(f"found {len(dups)} duplicate step sequences:\n")
    for d in dups:
        print(f"  run: {d.run[:60]}...")
        print(f"  in: {', '.join(d.workflows)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
