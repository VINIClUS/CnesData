"""CLI do descriptor: simulação do dispatch de todo passo do catálogo commitado (P27, P28*)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from scripts.promotion import lifecycle_descriptor
from tests.production.delivery import _ia_lifecycle_consumer as ia
from tests.production.delivery._plan_apply_fixtures import PRODUCER, PRODUCER_CLOSURE_SHA256

if TYPE_CHECKING:
    from pathlib import Path

SOURCE = "a" * 40
KEEPER_STEP = "application-inventory"


def _argv(output: Path, **change: str) -> list[str]:
    values = {"--step": KEEPER_STEP, "--purpose": "promotion", "--source-sha": SOURCE,
              "--run-id": "42", "--run-attempt": "1", "--producer-workflow-sha": PRODUCER,
              "--producer-closure-sha256": PRODUCER_CLOSURE_SHA256, "--output": str(output)}
    return [item for pair in (values | change).items() for item in pair]


def _outputs(path: Path) -> dict[str, str]:
    lines = path.read_text(encoding="utf-8").split("\n")
    result: dict[str, str] = {}
    index = 0
    while index < len(lines) and lines[index]:
        key, _, rest = lines[index].partition("<<")
        if rest:
            end = lines.index(rest, index + 1)
            result[key] = "\n".join(lines[index + 1:end])
            index = end + 1
            continue
        key, _, value = lines[index].partition("=")
        result[key] = value
        index += 1
    return result


def _governance(name: str = KEEPER_STEP) -> dict[str, Any]:
    steps = lifecycle_descriptor.catalog()
    return {"source_sha": SOURCE, "lifecycle_steps": steps, "role": steps[name]["role"],
            "purpose": steps[name]["purpose"]}


def test_dispatch_simulado_com_o_catalogo_gera_descriptor_que_o_consumidor_admite(
    tmp_path: Path,
) -> None:
    lifecycle_descriptor.main(_argv(tmp_path / "out"))
    outputs = _outputs(tmp_path / "out")
    raw = outputs["request_descriptor"].encode()
    assert set(outputs) == {"request_descriptor", "request_descriptor_sha256"}
    assert ia.sha(raw) == outputs["request_descriptor_sha256"]
    producer = {"workflow_sha": PRODUCER, "source_closure_sha256": PRODUCER_CLOSURE_SHA256}
    claim = json.loads(raw)
    binding = {"application_purpose": "promotion", "context_kind": "promotion",
               "repository": "VINIClUS/CnesData",
               "workflow_path": ".github/workflows/promote-production.yml",
               "ref": "refs/heads/main", "environment": "production", "source_sha": SOURCE,
               "run_id": 42, "run_attempt": 1, "producer_workflow_sha": PRODUCER,
               "producer_source_closure_sha256": PRODUCER_CLOSURE_SHA256}
    caller = ia.authentic_caller(binding, producer)
    request = ia.admit_request(raw, outputs["request_descriptor_sha256"], _governance(), caller)
    assert request["step"] == KEEPER_STEP
    assert request["payloads"] == {}
    assert claim["caller_binding_sha256"] == ia.sha(ia.canonical(binding))


@pytest.mark.parametrize("change", [
    {"--step": "plan-operational"}, {"--purpose": "outro"}, {"--source-sha": "abc"},
    {"--run-id": "0"}, {"--producer-closure-sha256": "z" * 64},
    {"--run-id": "4x2"}, {"--run-attempt": "-1"}, {"--run-attempt": ""},
], ids=["step", "proposito", "sha", "run", "closure", "run-texto", "attempt-negativo",
        "attempt-vazio"])
def test_cli_recusa_entrada_fora_do_contrato_sem_escrever(
    tmp_path: Path, change: dict[str, str],
) -> None:
    with pytest.raises(ValueError, match=r"^lifecycle_(descriptor|request) field="):
        lifecycle_descriptor.main(_argv(tmp_path / "out", **change))
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("change", [{"--run-id": "4x2"}, {"--run-attempt": "1.5"}],
                         ids=["run-id", "run-attempt"])
def test_cli_recusa_inteiro_malformado_com_key_value(tmp_path: Path,
                                                      change: dict[str, str]) -> None:
    with pytest.raises(ValueError, match=r"^lifecycle_descriptor field=run_(id|attempt)$"):
        lifecycle_descriptor.main(_argv(tmp_path / "out", **change))


def _gate_argv(output: Path) -> list[str]:
    argv = _argv(output)
    index = argv.index("--step")
    return [*argv[:index], "--gate-steps", *argv[index + 2:]]


def test_dispatch_simulado_de_todo_passo_de_gate_resolve_payloads_por_nome(tmp_path: Path) -> None:
    lifecycle_descriptor.main(_gate_argv(tmp_path / "out"))
    outputs = _outputs(tmp_path / "out")
    steps = lifecycle_descriptor.catalog()
    gates = sorted(name for name, step in steps.items() if step["role"] != "application")
    assert len(gates) == 15
    assert set(outputs) == {f"{lifecycle_descriptor.output_key(name)}_descriptor{suffix}"
                            for name in gates for suffix in ("", "_sha256")}
    for name in gates:
        key = lifecycle_descriptor.output_key(name)
        raw = outputs[f"{key}_descriptor"].encode()
        assert ia.sha(raw) == outputs[f"{key}_descriptor_sha256"]
        request = ia.admit_request(raw, outputs[f"{key}_descriptor_sha256"], _governance(name),
                                   _keeper_caller())
        assert request["step"] == name
        assert request["payloads"] == {
            input_name: {"step": producer["step"]}
            for input_name, producer in steps[name]["input_jobs"].items()}, name
    descriptors = {json.loads(outputs[f"{lifecycle_descriptor.output_key(n)}_descriptor"])[
        "caller_binding_sha256"] for n in gates}
    assert len(descriptors) == 1


def _keeper_caller() -> dict[str, Any]:
    binding = {"application_purpose": "promotion", "context_kind": "promotion",
               "repository": "VINIClUS/CnesData",
               "workflow_path": ".github/workflows/promote-production.yml",
               "ref": "refs/heads/main", "environment": "production", "source_sha": SOURCE,
               "run_id": 42, "run_attempt": 1, "producer_workflow_sha": PRODUCER,
               "producer_source_closure_sha256": PRODUCER_CLOSURE_SHA256}
    producer = {"workflow_sha": PRODUCER, "source_closure_sha256": PRODUCER_CLOSURE_SHA256}
    return ia.authentic_caller(binding, producer)


def test_cli_recusa_step_e_gate_steps_juntos(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        lifecycle_descriptor.main([*_argv(tmp_path / "out"), "--gate-steps"])
    assert not (tmp_path / "out").exists()
