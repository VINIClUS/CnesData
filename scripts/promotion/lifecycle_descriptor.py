"""CLI do descriptor: request de lifecycle de cada passo do catálogo commitado para o GITHUB_OUTPUT."""

from __future__ import annotations

import argparse
import json
import secrets
from pathlib import Path
from typing import Any

from scripts.promotion.lifecycle_request import CallerClaim, descriptor_sha256, request_descriptor

CATALOG_PATH = Path(__file__).resolve().parents[2] / ".github/delivery/lifecycle-steps.json"


def catalog() -> dict[str, Any]:
    """Returns: Catálogo lifecycle_steps commitado em S, o mesmo que a governança fixa."""
    return json.loads(CATALOG_PATH.read_bytes())


def output_key(step: str) -> str:
    """Returns: Prefixo da saída do job descriptor para o passo."""
    return step.replace("-", "_")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Descriptor do request de lifecycle")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--step")
    target.add_argument("--gate-steps", action="store_true")
    for name in ("purpose", "source-sha", "run-id", "run-attempt",
                 "producer-workflow-sha", "producer-closure-sha256", "output"):
        parser.add_argument(f"--{name}", required=True)
    return parser


def _integer(name: str, text: str) -> int:
    if not text.isascii() or not text.isdecimal():
        raise ValueError(f"lifecycle_descriptor field={name}")
    return int(text)


def _descriptor(claim: CallerClaim, step: str, steps: dict[str, Any]) -> bytes:
    if step not in steps:
        raise ValueError("lifecycle_descriptor field=step")
    input_jobs = steps[step]["input_jobs"]
    payloads = {name: {"step": producer["step"]} for name, producer in input_jobs.items()}
    return request_descriptor(claim, step, payloads, frozenset(input_jobs))


def _lines(prefix: str, descriptor: bytes) -> str:
    delimiter = f"LIFECYCLE_{secrets.token_hex(32)}"
    return (f"{prefix}descriptor<<{delimiter}\n{descriptor.decode()}\n{delimiter}\n"
            f"{prefix}descriptor_sha256={descriptor_sha256(descriptor)}\n")


def main(argv: list[str] | None = None) -> int:
    """Raises: ValueError: step fora do catálogo, propósito ou observação malformada."""
    arguments = _parser().parse_args(argv)
    claim = CallerClaim(
        application_purpose=arguments.purpose, source_sha=arguments.source_sha,
        run_id=_integer("run_id", arguments.run_id),
        run_attempt=_integer("run_attempt", arguments.run_attempt),
        producer_workflow_sha=arguments.producer_workflow_sha,
        producer_source_closure_sha256=arguments.producer_closure_sha256)
    steps = catalog()
    if arguments.gate_steps:
        gates = sorted(name for name, step in steps.items() if step["role"] != "application")
        text = "".join(_lines(f"{output_key(name)}_", _descriptor(claim, name, steps))
                       for name in gates)
    else:
        text = _lines("request_", _descriptor(claim, arguments.step, steps))
    with Path(arguments.output).open("a", encoding="utf-8") as stream:
        stream.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
