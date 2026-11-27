"""Confronto de um caller com o workflow_call e as permissões do reusable."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pathlib import Path

USES_PREFIX = "VINIClUS/infra-ansible/.github/workflows/reusable-cnesdata-"
LEVELS = {"none": 0, "read": 1, "write": 2}


def caller_jobs(workflows: Path) -> list[tuple[str, str, dict[str, Any], str, str]]:
    """Returns: (arquivo, job, definição, caminho do reusable, ref) de cada caller."""
    found = []
    for path in sorted(workflows.glob("*.yml")):
        jobs = yaml.safe_load(path.read_text(encoding="utf-8")).get("jobs") or {}
        for name, job in jobs.items():
            uses = str(job.get("uses", ""))
            if uses.startswith(USES_PREFIX):
                target, ref = uses.removeprefix("VINIClUS/infra-ansible/").split("@")
                found.append((path.name, name, job, target, ref))
    return found


def _declared(reusable: dict[str, Any]) -> dict[str, Any]:
    triggers: dict[Any, Any] = reusable
    on = triggers.get("on") or triggers[True]
    declared: dict[str, Any] = on["workflow_call"]
    return declared


def _input_problems(job: dict[str, Any], declared: dict[str, Any]) -> list[str]:
    inputs, given = declared.get("inputs") or {}, job.get("with") or {}
    unknown = [f"with_unknown={key}" for key in given if key not in inputs]
    missing = [f"with_missing={key}" for key, spec in inputs.items()
               if spec.get("required") and "default" not in spec and key not in given]
    return unknown + missing


def _secret_problems(job: dict[str, Any], declared: dict[str, Any]) -> list[str]:
    secrets, given = declared.get("secrets") or {}, job.get("secrets") or {}
    if given == "inherit":
        return []
    unknown = [f"secret_unknown={key}" for key in given if key not in secrets]
    missing = [f"secret_missing={key}" for key, spec in secrets.items()
               if spec.get("required") and key not in given]
    return unknown + missing


def _permission_problems(job: dict[str, Any], reusable: dict[str, Any]) -> list[str]:
    granted = job.get("permissions") or {}
    problems = []
    for name, body in (reusable.get("jobs") or {}).items():
        for scope, level in (body.get("permissions") or {}).items():
            if LEVELS[granted.get(scope, "none")] < LEVELS[level]:
                problems.append(f"permission_missing={scope}:{level} job={name}")
    return problems


def caller_problems(job: dict[str, Any], reusable: dict[str, Any]) -> list[str]:
    """Returns: divergências de with, secrets e permissions do caller (vazio se confere)."""
    declared = _declared(reusable)
    return (_input_problems(job, declared) + _secret_problems(job, declared)
            + _permission_problems(job, reusable))
