"""CLI do descriptor do application: simulação do dispatch com o catálogo gerado (P27)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from scripts.promotion import lifecycle_descriptor
from tests.production.delivery import _ia_lifecycle_consumer as ia
from tests.production.delivery import _ia_lifecycle_steps_catalog as catalog
from tests.production.delivery._plan_apply_fixtures import PRODUCER, PRODUCER_CLOSURE_SHA256

if TYPE_CHECKING:
    from pathlib import Path

SOURCE = "a" * 40


def _argv(output: Path, **change: str) -> list[str]:
    values = {"--step": catalog.KEEPER_STEP, "--purpose": "promotion", "--source-sha": SOURCE,
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


def _governance() -> dict[str, Any]:
    step = catalog.CATALOG[catalog.KEEPER_STEP]
    return {"source_sha": SOURCE, "lifecycle_steps": catalog.CATALOG, "role": step["role"],
            "purpose": step["purpose"]}


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
    assert request["step"] == catalog.KEEPER_STEP
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
