"""Descriptors da janela: phase-a do infra-plan e do infra-apply e do sign-activation."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.promotion import verify_plan_artifact as admit
from scripts.promotion._canonical import canonical_bytes
from scripts.promotion._plan_contract import closed, validate_reference
from scripts.promotion.lifecycle_request import REQUEST_LIMIT

type Node = dict[str, Any]
SCHEMA_VERSION = 1
REQUEST_VERSION = 1
PURPOSE = "phase-a"
PHASE_A_FIELDS = "source_sha producer_pins baseline_artifacts payload_artifact"
WORKFLOW_PATH = re.compile(r"\.github/workflows/[A-Za-z0-9_-]+\.yml")
MAX_RETAINED = 128
OUTPUT_NAME = "request_descriptor.json"


@dataclass(frozen=True, slots=True)
class Descriptor:
    """Bytes canônicos (com LF final), seu sha256 e os inputs de transporte do dispatch."""

    raw: bytes
    sha256: str
    transport: dict[str, str]


def _refuse(field: str) -> ValueError:
    return ValueError(f"window_descriptor field={field}")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _transport(mode: str, spec: Node) -> dict[str, str]:
    payload = spec["payload_artifact"]
    if mode == "plan":
        return {"phase_inputs_run_id": str(payload["run_id"]),
                "phase_inputs_artifact_id": str(payload["artifact_id"]),
                "phase_inputs_artifact_digest": payload["artifact_digest"]}
    inputs = spec["phase_inputs"]
    return {"plan_run_id": str(payload["run_id"]), "artifact_id": str(payload["artifact_id"]),
            "artifact_digest": payload["artifact_digest"],
            "phase_inputs_run_id": str(inputs["run_id"]),
            "phase_inputs_artifact_id": str(inputs["artifact_id"]),
            "phase_inputs_artifact_digest": inputs["artifact_digest"]}


def _phase_a_value(spec: Node) -> Node:
    baselines = spec["baseline_artifacts"]
    if type(baselines) is not dict or set(baselines) != set(admit.BASELINES):
        raise _refuse("baseline_artifacts")
    pins = spec["producer_pins"]
    if type(pins) is not list:
        raise _refuse("producer_pins")
    ordered = sorted(pins, key=lambda pin: (pin.get("repository"), pin.get("source_sha")))
    return {
        "schema_version": SCHEMA_VERSION, "repository": admit.REPOSITORY,
        "source_sha": spec["source_sha"], "producer_pins": ordered,
        "baseline_artifacts": [{"purpose": name, "reference": baselines[name]}
                               for name in admit.BASELINES],
        "payload_artifact": spec["payload_artifact"],
    }


def phase_a_descriptor(mode: str, spec: Node) -> Descriptor:
    """Returns: descriptor phase-a do infra-<mode>, já aceito pela admissão do verificador.

    Args: plan ou apply; spec com source_sha, producer_pins, baseline_artifacts (seis),
    payload_artifact e, no apply, phase_inputs. Raises: ValueError em campo fora do contrato.
    """
    if mode not in {"plan", "apply"}:
        raise _refuse("mode")
    closed(spec, PHASE_A_FIELDS + (" phase_inputs" if mode == "apply" else ""))
    raw = canonical_bytes(_phase_a_value(spec), REQUEST_LIMIT)
    built = Descriptor(raw, _sha(raw), _transport(mode, spec))
    request: Node = {"mode": mode, "purpose": PURPOSE, "expected_source_sha": spec["source_sha"],
                     "request_descriptor": raw.decode(), "request_descriptor_sha256": built.sha256}
    request.update(built.transport)
    admit.verify_payload(request, admit.verify_descriptor(request))
    if mode == "apply":
        validate_reference(spec["phase_inputs"])
    return built


def _check_activation_reference(reference: Node, source: str) -> None:
    validate_reference(reference)
    if reference["source_sha"] != source or not WORKFLOW_PATH.fullmatch(reference["workflow_path"]):
        raise _refuse("reference")


def activation_descriptor(spec: Node) -> Descriptor:
    """Returns: descriptor de ativação do sign-activation com os cinco campos do consumidor.

    Args: spec com source_sha, candidate_reference, phase_a_reference e as ARNs retidas
    ordenadas e únicas. Raises: ValueError em campo fora do contrato.
    """
    closed(spec, "source_sha candidate_reference phase_a_reference "
                 "retained_unit_task_definition_arns")
    source, retained = spec["source_sha"], spec["retained_unit_task_definition_arns"]
    if type(source) is not str or not re.fullmatch(r"[0-9a-f]{40}", source):
        raise _refuse("source_sha")
    if (type(retained) is not list or len(retained) > MAX_RETAINED
            or any(type(arn) is not str for arn in retained) or retained != sorted(set(retained))):
        raise _refuse("retained_unit_task_definition_arns")
    for name in ("candidate_reference", "phase_a_reference"):
        _check_activation_reference(spec[name], source)
    value = {"request_version": REQUEST_VERSION, "mode": "activation", "source_sha": source,
             "inputs": {name: spec[name] for name in (
                 "candidate_reference", "phase_a_reference", "retained_unit_task_definition_arns")},
             "ext2_post_upload_receipt_v1": None}
    raw = canonical_bytes(value, REQUEST_LIMIT)
    return Descriptor(raw, _sha(raw), {})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Descriptors da janela")
    commands = parser.add_subparsers(dest="command", required=True)
    phase = commands.add_parser("phase-a")
    phase.add_argument("--mode", required=True, choices=("plan", "apply"))
    activation = commands.add_parser("activation")
    for command in (phase, activation):
        command.add_argument("--spec", required=True)
        command.add_argument("--output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Escreve request_descriptor.json (com LF) no diretório e imprime sha e transporte."""
    arguments = _parser().parse_args(argv)
    spec = json.loads(Path(arguments.spec).read_bytes())
    built = (phase_a_descriptor(arguments.mode, spec) if arguments.command == "phase-a"
             else activation_descriptor(spec))
    target = Path(arguments.output)
    target.mkdir(parents=True, exist_ok=True)
    (target / OUTPUT_NAME).write_bytes(built.raw)
    lines = [f"request_descriptor_sha256={built.sha256}"]
    lines += [f"{key}={value}" for key, value in sorted(built.transport.items())]
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
