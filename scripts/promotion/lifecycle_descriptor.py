"""CLI do descriptor do application: gera request_descriptor e o sha256 para o GITHUB_OUTPUT."""

from __future__ import annotations

import argparse
import secrets
from pathlib import Path

from scripts.promotion.lifecycle_request import CallerClaim, descriptor_sha256, request_descriptor

STEPS = frozenset({"application-inventory"})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Descriptor do request de lifecycle")
    for name in ("step", "purpose", "source-sha", "run-id", "run-attempt",
                 "producer-workflow-sha", "producer-closure-sha256", "output"):
        parser.add_argument(f"--{name}", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Raises: ValueError: step fora do conjunto, propósito ou observação malformada."""
    arguments = _parser().parse_args(argv)
    if arguments.step not in STEPS:
        raise ValueError("lifecycle_descriptor field=step")
    claim = CallerClaim(
        application_purpose=arguments.purpose, source_sha=arguments.source_sha,
        run_id=int(arguments.run_id), run_attempt=int(arguments.run_attempt),
        producer_workflow_sha=arguments.producer_workflow_sha,
        producer_source_closure_sha256=arguments.producer_closure_sha256)
    descriptor = request_descriptor(claim, arguments.step, {}, frozenset())
    delimiter = f"LIFECYCLE_{secrets.token_hex(32)}"
    text = (f"request_descriptor<<{delimiter}\n{descriptor.decode()}\n{delimiter}\n"
            f"request_descriptor_sha256={descriptor_sha256(descriptor)}\n")
    with Path(arguments.output).open("a", encoding="utf-8") as stream:
        stream.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
