"""mode-canary.json do static-inventory: secret do dono, marker do estágio e S (P28*)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.promotion import mode_context_source as source
from scripts.promotion.closed_ready_probe import Target
from scripts.promotion.lifecycle_inputs import inputs_bytes
from scripts.promotion.prepare_mode_canary import check_mode_canary_input
from tests.production.delivery import _ia_lifecycle_consumer as ia
from tests.production.delivery.test_lifecycle_inputs import ACCOUNT, PRINCIPAL, RESOURCES

RAW_RELEASE_ID = "candidate-" + "a" * 40 + "-123456789-1"
ROOT = Path(__file__).resolve().parents[3]
SOURCE = "a" * 40
TARGET = Target(7, 1, "application / execute")
DISTRIBUTION = f"arn:aws:cloudfront::{ACCOUNT}:distribution/E2ABCDEF123456"
OTHER_ACCOUNT = ACCOUNT.replace("1", "9")
CONTEXT: dict[str, Any] = {
    "shared_source_sha": "b" * 40, "product_source_closure_sha256": "3" * 64,
    "shared_source_closure_sha256": "4" * 64, "release_id": "r1", "activation_sha256": "5" * 64,
    "application_principal_arn": PRINCIPAL, "resources": RESOURCES,
    "distribution_arn": DISTRIBUTION,
}
MARKER: dict[str, Any] = {
    "version": 1, "owner_sha256": "2" * 64, "generation": 3, "run_attempt": 1,
    "stage": "application-prepare", "sequence": 1, "checkpoint_sha256": "8" * 64,
    "admitted_at": "2026-11-26T10:00:00Z", "deadline": "2026-11-26T11:00:00Z",
}


def _secret(**change: Any) -> str:
    return json.dumps(CONTEXT | change)


def _marker(**change: Any) -> bytes:
    return ia.canonical(MARKER | change)


def _canary(context: str | None = None, marker: bytes | None = None) -> dict[str, Any]:
    raw = source.mode_canary_file(source.mode_context(context or _secret()),
                                  source.stage_marker(marker or _marker(), TARGET), TARGET, SOURCE)
    assert ia.canonical(json.loads(raw)) == raw
    return json.loads(raw)


def test_secret_de_fixture_monta_o_input_aceito_pelo_oraculo_e_pelo_consumidor() -> None:
    value = _canary()
    check_mode_canary_input(value)
    manifest, _ = ia.decode_mode_context(inputs_bytes({"region": "us-east-2"}, mode_canary=value))
    fixture = (ROOT / source.FIXTURE_MANIFEST).read_bytes()
    assert manifest["product_source_sha"] == SOURCE
    assert manifest["fixture_manifest_sha256"] == hashlib.sha256(fixture).hexdigest()
    assert manifest["owner"] == {"repository": "VINIClUS/CnesData", "run_id": 7, "run_attempt": 1,
                                 "generation": 3, "owner_sha256": "2" * 64}
    assert {key: manifest[key] for key in ("release_id", "activation_sha256")} == {
        "release_id": "r1", "activation_sha256": "5" * 64}
    assert manifest["predecessor_terminal_sha256"] is None
    assert manifest["scenario_plan_sha256"] == source.scenario_plan_sha256()
    assert value["resources"] == RESOURCES
    assert value["application_principal_arn"] == PRINCIPAL


def test_janela_do_contexto_parte_do_marker_e_respeita_o_teto_de_promocao() -> None:
    manifest = json.loads(_canary()["context_manifest"]["canonical_utf8"])
    assert (manifest["admitted_at"], manifest["deadline"]) == (
        "2026-11-26T10:00:00Z", "2026-11-26T10:20:00Z")
    assert _canary()["enabled_until"] == "2026-11-26T16:00:00Z"
    short = json.loads(_canary(marker=_marker(deadline="2026-11-26T10:05:00Z"))
                       ["context_manifest"]["canonical_utf8"])
    assert short["deadline"] == "2026-11-26T10:05:00Z"


@pytest.mark.parametrize("secret", [
    json.dumps(CONTEXT | {"extra": 1}),
    json.dumps({key: value for key, value in CONTEXT.items() if key != "activation_sha256"}),
    "{", "[]",
], ids=["campo-desconhecido", "campo-ausente", "json-invalido", "nao-objeto"])
def test_secret_fora_do_conjunto_fechado_recusa_antes_de_montar(secret: str) -> None:
    with pytest.raises(ValueError, match=r"^mode_context field=fields$"):
        source.mode_context(secret)


@pytest.mark.parametrize(("change", "field"), [
    ({"shared_source_sha": "B" * 40}, "shared_source_sha"),
    ({"activation_sha256": "5" * 63}, "activation_sha256"),
    ({"release_id": "-r1"}, "release_id"),
    ({"application_principal_arn": "arn:aws:iam::1:role/x"}, "application_principal_arn"),
    ({"resources": []}, "resources"),
    ({"distribution_arn": f"arn:aws:cloudfront::{OTHER_ACCOUNT}:distribution/E2X"},
     "distribution_arn"),
    ({"distribution_arn": f"arn:aws:s3:::{ACCOUNT}"}, "distribution_arn"),
], ids=["sha-maiusculo", "sha-curto", "release", "principal", "resources", "outra-conta",
        "nao-cloudfront"])
def test_secret_com_forma_invalida_recusa_com_o_campo(change: dict[str, Any], field: str) -> None:
    with pytest.raises(ValueError, match=rf"^mode_context field={field}$"):
        source.mode_context(_secret(**change))


@pytest.mark.parametrize("change", [
    {"stage": "phase-b"}, {"run_attempt": 2}, {"version": 2}, {"owner_sha256": "x"},
    {"generation": 0}, {"deadline": "2026-11-26T09:00:00Z"}, {"extra": 1},
], ids=["estagio", "attempt", "versao", "owner", "geracao", "prazo", "campo-extra"])
def test_marker_fora_do_run_ou_do_estagio_recusa(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=r"^mode_context field=marker$"):
        source.stage_marker(_marker(**change), TARGET)


def test_nome_do_marker_e_o_do_estagio_application_prepare_do_run() -> None:
    assert source.stage_marker_name(TARGET) == "cnesdata-stage-open-application-prepare-7-1"


def test_mode_context_aceita_o_id_cru_do_candidato_de_ate_128_caracteres() -> None:
    assert len(RAW_RELEASE_ID) > 32
    for release in (RAW_RELEASE_ID, "r" * 128):
        assert source.mode_context(_secret(release_id=release))["release_id"] == release


def test_mode_context_recusa_id_de_release_com_mais_de_128_caracteres() -> None:
    with pytest.raises(ValueError, match=r"^mode_context field=release_id$"):
        source.mode_context(_secret(release_id="r" * 129))


def test_input_do_static_inventory_carrega_o_id_cru_ate_o_oraculo_e_o_consumidor() -> None:
    value = _canary(_secret(release_id=RAW_RELEASE_ID))
    check_mode_canary_input(value)
    manifest, _ = ia.decode_mode_context(inputs_bytes({"region": "us-east-2"}, mode_canary=value))
    assert manifest["release_id"] == RAW_RELEASE_ID
