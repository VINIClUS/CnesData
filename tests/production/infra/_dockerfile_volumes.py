"""Leitura do estágio final de um Dockerfile para checar VOLUME, chown e USER."""

import json
import re
from collections.abc import Iterable


def instructions(text: str) -> list[tuple[str, str]]:
    """Returns: pares (INSTRUCAO, argumentos) com continuações unidas e comentários removidos."""
    logical: list[str] = []
    pending = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not pending and (not line or line.startswith("#")):
            continue
        if pending and line.startswith("#"):
            continue
        if line.endswith("\\"):
            pending += line[:-1].rstrip() + " "
            continue
        logical.append(pending + line)
        pending = ""
    if pending:
        logical.append(pending.strip())
    pairs = [tuple(item.split(None, 1)) for item in logical]
    return [(p[0].upper(), p[1] if len(p) > 1 else "") for p in pairs]


def final_stage(items: list[tuple[str, str]]) -> list[tuple[str, str]]:
    starts = [index for index, (name, _) in enumerate(items) if name == "FROM"]
    return items[starts[-1] :] if starts else items


def volume_paths(arguments: str) -> list[str]:
    text = arguments.strip()
    if text.startswith("["):
        return [str(item) for item in json.loads(text)]
    return text.split()


def _positions(stage: list[tuple[str, str]], name: str) -> list[int]:
    return [index for index, (kind, _) in enumerate(stage) if kind == name]


def volume_violations(text: str, paths: Iterable[str]) -> list[str]:
    """Returns: violações com key=value; vazio quando todo caminho tem VOLUME após o chown."""
    stage = final_stage(instructions(text))
    chowns = [i for i in _positions(stage, "RUN") if re.search(r"\bchown\b", stage[i][1])]
    users = _positions(stage, "USER")
    declared: dict[str, int] = {}
    for index in _positions(stage, "VOLUME"):
        for path in volume_paths(stage[index][1]):
            declared.setdefault(path, index)
    problems: list[str] = []
    if not chowns:
        problems.append("chown=missing")
    if not users:
        problems.append("user=missing")
    for path in sorted(paths):
        index = declared.get(path)
        if index is None:
            problems.append(f"volume=missing path={path}")
        elif chowns and index < chowns[-1]:
            problems.append(f"volume=before_chown path={path}")
        elif users and index > users[0]:
            problems.append(f"volume=after_user path={path}")
    return problems
