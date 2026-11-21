"""Gerador de inputs.json por step contra o consumidor do infra-ansible (P25)."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pytest

from scripts.promotion import _api_static_records as records
from scripts.promotion.lifecycle_inputs import (
    StaticBinding,
    StaticPromotion,
    inputs_bytes,
    mode_canary_input,
    static_promotion_input,
)
from scripts.promotion.mode_canary_contract import ContractError, parse_context_manifest
from scripts.promotion.prepare_mode_canary import check_mode_canary_input
from scripts.promotion.publish_dashboard import PublicationError
from tests.production.delivery import _ia_lifecycle_consumer as ia

ARN = "arn:aws:cloudfront::111122223333:distribution/E2SYNTHETIC0"
SHA = "1" * 64
COMMIT = "a" * 40
OWNER = {"repository": "VINIClUS/CnesData", "run_id": 42, "run_attempt": 1, "generation": 3,
         "owner_sha256": "2" * 64}
ACCOUNT = "111122223333"
PRINCIPAL = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-application"
FRAGMENT = {"fragment_version": 4, "account_id": ACCOUNT}
ENABLED = "2027-02-12T00:00:00Z"


def _inventory(**override: Any) -> bytes:
    record = {
        "schema": records.INVENTORY_SCHEMA, "candidate_sha256": SHA, "dashboard_sha256": SHA,
        "baseline": {}, "outcome": "PLANNED",
        "upload_order": ["assets/app.js", "index.html", "theme-init.js"],
        "already_published": [], "retire": [], "retire_latest": [],
        "rollback_versions": [
            {"key": "index.html", "version_id": "Prev.Index_1", "sha256": SHA},
            {"key": "theme-init.js", "version_id": "Prev+Theme=2", "sha256": SHA}],
        "multipart_keys": [], "projected_bytes": 10,
    }
    return records.canonical(record | override)


def _binding(**override: Any) -> StaticBinding:
    values: dict[str, Any] = {"release_id": "r1", "run_id": 42, "run_attempt": 1, "generation": 3,
                              "product_source_sha": COMMIT, "shared_source_sha": "b" * 40}
    return StaticBinding(**(values | override))


def _request(**override: Any) -> StaticPromotion:
    values: dict[str, Any] = {"mode": "operational", "application_promotion": FRAGMENT,
                              "binding": _binding(), "enabled_until": ENABLED}
    return StaticPromotion(**(values | override))


def _plan(inventory: bytes | None = None) -> tuple[bytes, bytes]:
    inventory = inventory or _inventory()
    return records.static_plan(inventory, ARN), inventory


def test_static_promotion_tem_os_nove_campos_fechados_do_contrato_do_produto() -> None:
    plan, inventory = _plan()
    value = static_promotion_input(_request(), plan, inventory)
    assert ia.product_static_promotion(value, plan, records.STATIC_BUCKET) == value


def test_static_promotion_liga_o_binding_ao_sha_do_plano_e_a_inventory() -> None:
    plan, inventory = _plan()
    value = static_promotion_input(_request(), plan, inventory)
    assert value["binding"]["static_plan_sha256"] == ia.sha(plan)
    assert value["inventory_sha256"] == ia.sha(inventory)


def test_static_promotion_binding_e_aceito_pelo_consumidor() -> None:
    plan, inventory = _plan()
    binding = static_promotion_input(_request(), plan, inventory)["binding"]
    assert ia.static_session_binding(binding) == binding


def test_static_promotion_recusa_plano_de_outra_inventory() -> None:
    plan, _ = _plan()
    other = _inventory(upload_order=["index.html", "theme-init.js"])
    with pytest.raises(PublicationError, match="static_plan=binding"):
        static_promotion_input(_request(), plan, other)


def test_static_promotion_recusa_plano_com_um_byte_alterado() -> None:
    plan, inventory = _plan()
    with pytest.raises(PublicationError):
        static_promotion_input(_request(), plan.replace(b"assets/app.js", b"assets/app.JS"),
                               inventory)


@pytest.mark.parametrize("mode", ["observer-bootstrap", "other", ""])
def test_static_promotion_so_gera_o_modo_operational(mode: str) -> None:
    plan, inventory = _plan()
    with pytest.raises(ValueError, match="mode="):
        static_promotion_input(_request(mode=mode), plan, inventory)


@pytest.mark.parametrize("field", ia.STATIC_BINDING_FIELDS.replace(" static_plan_sha256", "")
                         .split())
def test_consumidor_recusa_campo_do_binding_estatico_alterado(field: str) -> None:
    plan, inventory = _plan()
    binding = deepcopy(static_promotion_input(_request(), plan, inventory)["binding"])
    binding[field] = "FORJADO" if type(binding[field]) is str else 0
    with pytest.raises(ia.Refused):
        ia.static_session_binding(binding)


def test_gerador_recusa_binding_estatico_sem_run_id() -> None:
    plan, inventory = _plan()
    with pytest.raises(ValueError, match="run_id="):
        static_promotion_input(_request(binding=_binding(run_id=None)), plan, inventory)


@pytest.mark.parametrize("field", ["release_id", "product_source_sha", "shared_source_sha"])
def test_gerador_recusa_binding_estatico_malformado(field: str) -> None:
    plan, inventory = _plan()
    with pytest.raises(ValueError, match=f"{field}="):
        static_promotion_input(_request(binding=_binding(**{field: "A!"})), plan, inventory)


def test_gerador_recusa_enabled_until_fora_do_formato() -> None:
    plan, inventory = _plan()
    with pytest.raises(ValueError, match="enabled_until="):
        static_promotion_input(_request(enabled_until="2027-02-12"), plan, inventory)


def test_contrato_do_produto_recusa_campo_do_static_promotion_adulterado() -> None:
    plan, inventory = _plan()
    value = static_promotion_input(_request(), plan, inventory)
    forged = value | {"candidate_keys": ["index.html"]}
    with pytest.raises(ia.Refused, match="static plan fields"):
        ia.product_static_promotion(forged, plan, records.STATIC_BUCKET)


def test_contrato_do_produto_recusa_static_promotion_com_campo_extra() -> None:
    plan, inventory = _plan()
    value = static_promotion_input(_request(), plan, inventory) | {"extra": 1}
    with pytest.raises(ia.Refused, match="closed object fields"):
        ia.product_static_promotion(value, plan, records.STATIC_BUCKET)


RESOURCES: dict[str, Any] = {
    "table_arn": f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/cnes",
    "index_arn": f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/cnes/index/gsi5",
    "data_bucket_arn": "arn:aws:s3:::cnesdata-prod-data",
    "audit_bucket_arn": "arn:aws:s3:::cnesdata-prod-audit",
    "cluster_arn": f"arn:aws:ecs:us-east-2:{ACCOUNT}:cluster/cnes",
    "machine_arn": f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:cnes",
    "execution_role_arn": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-exec",
    "log_group_arn": f"arn:aws:logs:us-east-2:{ACCOUNT}:log-group:cnes",
    "revisions": {k: f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/{k}:1"
                  for k in ("audit", "recovery", "unit")},
    "task_role_arns": {k: f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-{k}"
                       for k in ("audit", "recovery", "unit")},
    "network": {"public_subnet_ids": ["subnet-1"], "security_group_id": "sg-1"},
    "schedule_arns": {k: f"arn:aws:scheduler:us-east-2:{ACCOUNT}:schedule/p-{k}/p-{k}"
                      for k in ("audit", "recovery")},
    "alarm_arns": {k: f"arn:aws:cloudwatch:us-east-2:{ACCOUNT}:alarm:p-{k}"
                   for k in ("audit_outbox_backlog", "audit_repeated_failure",
                             "recovery_repeated_failure")},
    "scope": {"audit_object_keys": ["a-1"], "output_object_keys": ["o-1"],
              "raw_object_keys": ["r-1"], "run_ids": ["run-1"], "tenant_id": "tenant-1"},
}


def _manifest_wrapper(**override: Any) -> dict[str, str]:
    manifest: dict[str, Any] = {
        "schema_version": 2, "context_kind": "promotion", "owner": OWNER,
        "product_source_sha": COMMIT, "product_source_closure_sha256": "3" * 64,
        "shared_source_sha": "b" * 40, "shared_source_closure_sha256": "4" * 64,
        "release_id": "r1", "activation_sha256": "5" * 64,
        "resources_sha256": ia.sha(ia.canonical(RESOURCES)), "fixture_manifest_sha256": "6" * 64,
        "scenario_plan_sha256": "7" * 64, "predecessor_terminal_sha256": None,
        "admitted_at": "2026-11-20T10:00:00Z", "deadline": "2026-11-20T10:15:00Z",
    } | override
    text = ia.canonical(manifest).decode()
    return {"canonical_utf8": text, "sha256": ia.sha(text.encode())}


def _mode(wrapper: dict[str, str] | None = None, **override: Any) -> dict[str, Any]:
    manifest = parse_context_manifest(wrapper or _manifest_wrapper())
    return mode_canary_input(manifest, RESOURCES, PRINCIPAL, ENABLED) | override


def test_mode_canary_tem_os_oito_campos_e_passa_no_oraculo_do_produto() -> None:
    value = _mode()
    assert set(value) == {"schema_version", "context_kind", "application_principal_arn", "owner",
                          "context_manifest", "delegation_sessions", "resources",
                          "enabled_until"}
    check_mode_canary_input(value)


def test_mode_canary_inputs_sao_decodificados_pelo_consumidor() -> None:
    raw = inputs_bytes({"region": "us-east-2"}, mode_canary=_mode())
    manifest, inner = ia.decode_mode_context(raw)
    assert manifest["release_id"] == "r1"
    assert json.loads(raw)["mode_canary"]["context_manifest"]["sha256"] == ia.sha(inner)


def test_mode_canary_usa_o_codec_utf8_sem_escape_ascii() -> None:
    wrapper = _manifest_wrapper(release_id="r1")
    raw = inputs_bytes({"nota": "ação"}, mode_canary=_mode(wrapper))
    assert "ação".encode() in raw
    ia.decode_mode_context(raw)


def test_consumidor_recusa_input_de_modo_com_um_byte_extra() -> None:
    raw = inputs_bytes({}, mode_canary=_mode())
    with pytest.raises(ia.Refused, match="mode context original request bytes"):
        ia.decode_mode_context(raw + b"\n")


def test_consumidor_recusa_digest_do_manifest_alterado() -> None:
    value = _mode()
    value["context_manifest"] = value["context_manifest"] | {"sha256": "0" * 64}
    raw = ia.canonical({"mode_canary": value})
    with pytest.raises(ia.Refused, match="mode context inner bytes digest"):
        ia.decode_mode_context(raw)


def test_consumidor_recusa_wrapper_do_manifest_com_campo_extra() -> None:
    value = _mode()
    value["context_manifest"] = value["context_manifest"] | {"x": "1"}
    with pytest.raises(ia.Refused, match="closed object fields"):
        ia.decode_mode_context(ia.canonical({"mode_canary": value}))


def test_mode_canary_recusa_sessoes_de_delegacao_adulteradas() -> None:
    manifest = parse_context_manifest(_manifest_wrapper())
    with pytest.raises(ContractError):
        mode_canary_input(manifest, RESOURCES | {"table_arn": "x"}, PRINCIPAL, ENABLED)


def test_mode_canary_recusa_principal_de_outra_forma() -> None:
    manifest = parse_context_manifest(_manifest_wrapper())
    with pytest.raises(ContractError):
        mode_canary_input(manifest, RESOURCES, "arn:aws:iam::1:role/x", ENABLED)


def test_inputs_bytes_recusa_chaves_geradas_ja_presentes_na_base() -> None:
    with pytest.raises(ValueError, match="inputs=reserved_key"):
        inputs_bytes({"mode_canary": None}, mode_canary=_mode())


def test_inputs_bytes_inclui_static_promotion_e_mode_canary_no_mesmo_arquivo() -> None:
    plan, inventory = _plan()
    static = static_promotion_input(_request(), plan, inventory)
    raw = inputs_bytes({"region": "us-east-2"}, static_promotion=static, mode_canary=_mode())
    value = json.loads(raw)
    assert set(value) == {"region", "static_promotion", "mode_canary"}
    assert raw == ia.canonical(value)
    ia.decode_mode_context(raw)


def test_inputs_bytes_sem_modo_emite_null_explicito() -> None:
    value = json.loads(inputs_bytes({}, static_promotion=None, mode_canary=None))
    assert value == {"static_promotion": None, "mode_canary": None}


def test_inputs_bytes_recusa_arquivo_acima_do_limite_do_consumidor() -> None:
    with pytest.raises(ValueError, match="inputs=size"):
        inputs_bytes({"blob": "x" * 140000}, mode_canary=_mode())
