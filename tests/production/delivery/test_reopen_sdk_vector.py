"""Vetor fixado das chamadas SDK do reopen (final e recovery) e do rollback sem publicação."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripts.promotion import promote_api
from tests.production.delivery.test_adapter_handlers import (
    FINAL,
    RECOVERY,
    Context,
    _clock,
    _closed,
    _relogio,
    _reopen_inputs,
    _run,
    canonical,
)
from tests.production.delivery.test_api_static_promotion import RECOVERY as STATIC_RECOVERY
from tests.production.delivery.test_api_static_promotion import (
    FakeStaticPort,
    _null_publication,
    _published,
)
from tests.production.delivery.test_api_static_promotion import _run as static_run
from tests.production.delivery.test_promote_close import NOW, OWNER, _shape

if TYPE_CHECKING:
    import pytest

__all__ = ["_relogio"]
VECTOR = Path(__file__).with_name("reopen_sdk_vector.json")
VECTOR_SHA256 = "86e9d493ebbdd1aedcb4c422416dbcf06d28b9e925dc934966cff7718b35b99f"


def _calls(context: Context) -> list[dict[str, Any]]:
    owner = OWNER["owner_sha256"]
    text = canonical([_shape_values(call) for call in context.calls]).decode()
    return json.loads(text.replace(owner, "<owner_sha256>"))


def _shape_values(call: tuple[str, str, str, dict[str, Any]]) -> dict[str, Any]:
    role, service, operation, parameters = call
    return {"role": role, "service": service, "operation": operation, "parameters": parameters}


def _stage_calls(stage: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW)
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, stage)
        context = Context(raw, probe, _reopen_inputs(close))
        _run(context, "reopen", stage)
    return _calls(context)


def _null_rollback() -> dict[str, Any]:
    port = FakeStaticPort()
    inputs = _published(port)
    before = len(port.calls)
    _, context = static_run(port, "rollback-api-static", _null_publication(inputs),
                            STATIC_RECOVERY)
    [(_, raw)] = context.observed
    return {"result": json.loads(raw), "sdk_calls": len(port.calls) - before}


def generate(monkeypatch: pytest.MonkeyPatch) -> bytes:
    final = _stage_calls(FINAL, monkeypatch)
    recovery = _stage_calls(RECOVERY, monkeypatch)
    return canonical({
        "version": 1,
        "reopen": {"final": final, "recovery": recovery},
        "rollback_api_static": {"publication_null": _null_rollback()},
    })


def test_vetor_do_reopen_e_regenerado_do_codigo_do_produto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert VECTOR.read_bytes() == generate(monkeypatch)


def test_vetor_do_reopen_tem_o_sha256_fixado() -> None:
    assert hashlib.sha256(VECTOR.read_bytes()).hexdigest() == VECTOR_SHA256


def test_vetor_cobre_o_cas_em_final_e_so_leituras_em_recovery() -> None:
    vector = json.loads(VECTOR.read_bytes())
    final, recovery = vector["reopen"]["final"], vector["reopen"]["recovery"]
    [update] = [call for call in final if call["operation"] == "update_item"]
    assert update["parameters"]["ConditionExpression"] == "#version = :expected"
    assert update["parameters"]["ExpressionAttributeValues"][":state"] == {"S": "OPEN"}
    assert {call["operation"] for call in recovery} == {"get_item"}
    assert all(call["role"] == "fence_operator" and call["service"] == "dynamodb"
               for call in final + recovery)


def test_vetor_registra_o_rollback_com_publicacao_null_sem_chamada_sdk() -> None:
    entry = json.loads(VECTOR.read_bytes())["rollback_api_static"]["publication_null"]
    assert entry["sdk_calls"] == 0
    assert (entry["result"]["restored"], entry["result"]["residual"]) == ([], False)
    assert entry["result"]["invalidation_id"] is None


def test_vetor_nao_carrega_dado_privado() -> None:
    text = VECTOR.read_text(encoding="utf-8")
    assert "arn:aws" not in text
    assert hashlib.sha256(b"").hexdigest() not in text


def test_shape_do_vetor_legado_continua_igual_ao_do_close() -> None:
    sample = ("fence_operator", "dynamodb", "get_item", {"TableName": "t"})
    assert _shape(sample)["operation"] == "get_item"
