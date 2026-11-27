"""Elemento phase-c construído pelo produto sobre o que o deploy C do keeper fornece (#465)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from scripts.promotion._phase_c_core import object_hash
from scripts.promotion.phase_c_recovery import PhaseCEvidence, phase_c_evidence

FIELDS = {"version", "kind", "common", "close_drain_sha256", "before", "after", "plan_sha256",
          "audit_sha256", "apply_sha256", "routes", "observed_at"}
CLOSE = {"version": 2, "kind": "close-drain", "common": {"canary_id": "c" * 32,
                                                         "release_id": "r" + "1" * 31}}
LINEAGE = "lineage"
UNIT, RECOVERY, AUDIT = (f"arn:aws:ecs:us-east-2:111122223333:task-definition/{n}:2"
                         for n in ("unit", "recovery", "audit"))


def _evidence(mode: str = "recovery", **changes: Any) -> PhaseCEvidence:
    routes = {"unit": UNIT, "recovery": RECOVERY, "audit": AUDIT if mode == "audit" else None}
    return replace(PhaseCEvidence(
        mode=mode, close=CLOSE, before={LINEAGE: "l-1", "serial": 4},
        after={LINEAGE: "l-1", "serial": 5}, plan_sha256="1" * 64, audit_sha256="2" * 64,
        apply_output=b"Apply complete! Resources: 1 added.\n", routes=routes,
        observed_at=datetime(2026, 11, 13, 12, 20, tzinfo=UTC)), **changes)


@pytest.mark.parametrize("mode", ["recovery", "audit"])
def test_elemento_tem_os_onze_campos_e_bytes_canonicos(mode: str) -> None:
    evidence = _evidence(mode)
    raw = phase_c_evidence(evidence)
    element = json.loads(raw)
    assert set(element) == FIELDS
    assert raw == (json.dumps(element, sort_keys=True, separators=(",", ":")) + "\n").encode()
    assert hashlib.sha256(raw).hexdigest() == object_hash(element)
    assert (element["version"], element["kind"]) == (1, f"phase-c-{mode}")
    assert element["common"] == CLOSE["common"]
    assert element["close_drain_sha256"] == object_hash(CLOSE)
    assert (element["before"], element["after"]) == (evidence.before, evidence.after)
    assert (element["plan_sha256"], element["audit_sha256"]) == ("1" * 64, "2" * 64)
    assert element["routes"] == evidence.routes
    assert element["observed_at"] == "2026-11-13T12:20:00Z"


def test_apply_sha256_e_o_digest_da_saida_do_apply_fornecida_pelo_keeper() -> None:
    element = json.loads(phase_c_evidence(_evidence()))
    expected = hashlib.sha256(b"Apply complete! Resources: 1 added.\n").hexdigest()
    assert element["apply_sha256"] == expected


@pytest.mark.parametrize("after", [
    {LINEAGE: "l-1", "serial": 4}, {LINEAGE: "l-1", "serial": 3},
    {LINEAGE: "l-2", "serial": 9},
])
def test_checkpoint_que_nao_avanca_recusa(after: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=r"^phase_c=checkpoint_not_advanced$"):
        phase_c_evidence(_evidence(after=after))


@pytest.mark.parametrize("routes", [
    {"unit": UNIT, "recovery": RECOVERY},
    {"unit": UNIT, "recovery": RECOVERY, "audit": None, "extra": None},
    {"unit": None, "recovery": RECOVERY, "audit": None},
    {"unit": UNIT, "recovery": None, "audit": None},
    {"unit": UNIT, "recovery": RECOVERY, "audit": 7},
    {"unit": UNIT, "recovery": "", "audit": None},
    [UNIT, RECOVERY],
])
def test_rotas_fora_da_projecao_do_modo_recusam(routes: Any) -> None:
    with pytest.raises(ValueError, match=r"^phase_c=routes$"):
        phase_c_evidence(_evidence(routes=routes))


def test_audit_sem_a_rota_do_audit_aplicada_recusa() -> None:
    routes = {"unit": UNIT, "recovery": RECOVERY, "audit": None}
    with pytest.raises(ValueError, match=r"^phase_c=routes$"):
        phase_c_evidence(_evidence("audit", routes=routes))


def test_modo_fora_da_phase_c_recusa() -> None:
    with pytest.raises(ValueError, match=r"^phase_c=routes$"):
        phase_c_evidence(_evidence(mode="unit"))


def test_relogio_fora_de_utc_recusa() -> None:
    local = datetime(2026, 11, 13, 9, 20, tzinfo=timezone(timedelta(hours=-3)))
    with pytest.raises(ValueError, match="reason=datetime_not_utc"):
        phase_c_evidence(_evidence(observed_at=local))


@pytest.mark.parametrize(("campo", "valor"), [
    ("after", {LINEAGE: "l-1"}), ("after", {LINEAGE: "l-1", "serial": "5"}),
    ("after", {LINEAGE: "l-1", "serial": True}), ("after", {LINEAGE: 1, "serial": 5}),
    ("after", {LINEAGE: "", "serial": 5}), ("before", None), ("before", {"serial": 4}),
    ("before", [4]),
])
def test_checkpoint_malformado_recusa_com_chave_valor(campo: str, valor: Any) -> None:
    with pytest.raises(ValueError, match=r"^phase_c=checkpoint_invalid$"):
        phase_c_evidence(_evidence(**{campo: valor}))


@pytest.mark.parametrize("close", [None, {"version": 2}, {"common": None}, {"common": []}])
def test_close_sem_common_recusa_com_chave_valor(close: Any) -> None:
    with pytest.raises(ValueError, match=r"^phase_c=close_invalid$"):
        phase_c_evidence(_evidence(close=close))


@pytest.mark.parametrize(("campo", "valor", "motivo"), [
    ("apply_output", "Apply complete! Resources: 1 added.\n", "apply_output_invalid"),
    ("apply_output", None, "apply_output_invalid"),
    ("observed_at", "2026-11-13T12:20:00Z", "observed_at_invalid"),
    ("observed_at", None, "observed_at_invalid"),
    ("plan_sha256", 1, "digest_invalid"), ("plan_sha256", "1" * 63, "digest_invalid"),
    ("plan_sha256", "G" * 64, "digest_invalid"), ("audit_sha256", None, "digest_invalid"),
])
def test_insumo_do_deploy_fora_do_tipo_recusa_com_chave_valor(
        campo: str, valor: Any, motivo: str) -> None:
    with pytest.raises(ValueError, match=rf"^phase_c={motivo}$"):
        phase_c_evidence(_evidence(**{campo: valor}))
