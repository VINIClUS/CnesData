"""Seeds gravam o record do estágio e o control do owner pelas triplas que já usam (#465)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from cnes_domain.orchestration.mode_canary import ModeCanaryControl, SyntheticContextRequired
from cnes_infra.aws.mode_canary import GENESIS_LEDGER_SHA256, canonical_text
from cnes_infra.control_plane.dynamodb_keys import tenant_partition
from scripts.promotion import _adapter_abi as abi
from scripts.promotion import promote_api
from scripts.promotion.mode_canary_contract import ContractError
from tests.production.delivery import test_adapter_mode_effects as fx
from tests.production.delivery.test_adapter_handlers import API, SHA
from tests.production.delivery.test_promote_close import OWNER, sha

PK = tenant_partition(fx.TENANT)
OTHER = OWNER | {"owner_sha256": "8" * 64}


def _record(mode: str, owner: str = fx.OWNER_SHA, **changes: Any) -> bytes:
    run_ids = [run.replace(fx.OWNER_SHA[:16], owner[:16]) for run in fx.runs(mode)]
    return fx.record(mode, owner_sha256=owner, run_ids=run_ids, **changes)


def _seed(mp: pytest.MonkeyPatch, name: str, keeper: fx.Keeper,
          owner: dict[str, Any] = OWNER) -> Any:
    epoch = int(fx.NOW.timestamp())
    stage = API | {"stage": abi.MODE_OPERATIONS[name][1], "close": epoch - 120,
                   "fixed_deadline": epoch + 900, "admitted_at": epoch - 5, "deadline": epoch + 600}
    request = {"abi_version": 3, "operation": name, "repository": owner["repository"],
               "source_sha": SHA, "owner": owner, "checkpoint_sha256": API["checkpoint_sha256"],
               "activation_sha256": "a" * 64, "stage": stage,
               "inputs": {key: {"sha256": sha(raw), "size": len(raw)}
                          for key, raw in sorted(keeper.inputs.items())}}
    mp.setattr(promote_api, "CLOCK", lambda: fx.NOW)
    return promote_api.execute_shared_operation(name, request, keeper)


def _keeper(mode: str, owner: str = fx.OWNER_SHA, **changes: Any) -> fx.Keeper:
    return fx.Keeper(fx.seed_inputs(mode) | {"record": _record(mode, owner, **changes)})


def _puts(keeper: fx.Keeper) -> list[dict[str, Any]]:
    return [call[3] for call in keeper.calls if call[2] == "put_item"]


def _control_key(owner: str = fx.OWNER_SHA) -> tuple[str, str]:
    return PK, f"PROD_CANARY_CONTROL#{owner}"


@pytest.mark.parametrize("mode", ["unit", "recovery"])
def test_seed_grava_o_record_insert_only_com_os_bytes_exatos_do_input(
        monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    keeper = _keeper("unit")
    _seed(monkeypatch, "unit-seed", keeper)
    if mode == "recovery":
        keeper.inputs = fx.seed_inputs(mode) | {"record": _record(mode)}
        _seed(monkeypatch, "recovery-seed", keeper)
    key = (PK, f"PROD_CANARY#{fx.OWNER_SHA}#{mode}")
    assert keeper.items[key] == {"pk": {"S": key[0]}, "sk": {"S": key[1]},
                                 "entity": {"S": "MODECANARYRECORD"},
                                 "payload": {"S": keeper.inputs["record"].decode()}}
    [put] = [p for p in _puts(keeper) if p["Item"]["sk"]["S"] == key[1]]
    assert put["ConditionExpression"] == "attribute_not_exists(pk)"
    reads = [call[3]["Key"] for call in keeper.calls if call[2] == "get_item"]
    assert {"pk": {"S": key[0]}, "sk": {"S": key[1]}} in reads


@pytest.mark.parametrize("mode", ["unit", "recovery"])
def test_record_fica_fora_do_manifesto_do_seed(monkeypatch: pytest.MonkeyPatch,
                                               mode: str) -> None:
    keeper = _keeper("unit")
    if mode == "recovery":
        _seed(monkeypatch, "unit-seed", keeper)
        keeper.inputs = fx.seed_inputs(mode) | {"record": _record(mode)}
    result = _seed(monkeypatch, f"{mode}-seed", keeper)
    expected = fx.seed(mode)
    assert keeper.observed[-1] == ("mode-seed", expected.manifest)
    assert result["observations"][-1]["sha256"] == expected.sha256
    assert all(not entry["sk"].startswith("PROD_CANARY")
               for entry in json.loads(expected.manifest)["items"])


def test_replay_do_seed_com_record_identico_nao_muda_nada(monkeypatch: pytest.MonkeyPatch) -> None:
    keeper = _keeper("unit")
    _seed(monkeypatch, "unit-seed", keeper)
    before = dict(keeper.items)
    _seed(monkeypatch, "unit-seed", keeper)
    assert keeper.items == before


def test_record_divergente_ja_gravado_recusa_seed_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    keeper = _keeper("unit")
    key = (PK, f"PROD_CANARY#{fx.OWNER_SHA}#unit")
    keeper.items[key] = {"pk": {"S": key[0]}, "sk": {"S": key[1]},
                         "entity": {"S": "MODECANARYRECORD"}, "payload": {"S": "{}\n"}}
    with pytest.raises(ContractError, match=f"reason=seed_conflict sk={key[1]}"):
        _seed(monkeypatch, "unit-seed", keeper)


def test_unit_seed_inicializa_o_control_unico_do_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    keeper = _keeper("unit")
    _seed(monkeypatch, "unit-seed", keeper)
    item = keeper.items[_control_key()]
    assert item["entity"] == {"S": "MODECANARYCONTROL"}
    text = item["payload"]["S"]
    assert canonical_text(json.loads(text)) == text
    control = ModeCanaryControl.from_mapping(json.loads(text))
    record = json.loads(keeper.inputs["record"])
    assert (control.owner_sha256, control.generation) == (fx.OWNER_SHA,
                                                          record["promotion_generation"])
    assert (control.context_kind, control.context_manifest_sha256) == (
        "promotion", record["context_manifest_sha256"])
    assert (control.admission_state, control.sealed_at) == ("ACTIVE", None)
    assert control.admitted_at == datetime(2026, 11, 13, 12, 1, tzinfo=UTC)
    assert control.deadline == datetime.fromisoformat(record["fixed_deadline"])
    assert control.ledger_sha256 == GENESIS_LEDGER_SHA256
    assert control.admitted_counts == {"unit_tasks": 0, "recovery_resumes": 0,
                                       "recovery_hangs": 0}


def test_recovery_seed_so_le_o_control_e_grava_o_proprio_record(
        monkeypatch: pytest.MonkeyPatch) -> None:
    keeper = _keeper("unit")
    _seed(monkeypatch, "unit-seed", keeper)
    control = dict(keeper.items[_control_key()])
    keeper.events.clear()
    keeper.inputs = fx.seed_inputs("recovery") | {"record": _record("recovery")}
    _seed(monkeypatch, "recovery-seed", keeper)
    assert keeper.items[_control_key()] == control
    assert all(p["Item"]["sk"]["S"] != _control_key()[1] for p in _puts(keeper))
    reads = [call[3]["Key"]["sk"]["S"] for call in keeper.calls if call[2] == "get_item"]
    assert reads[0] == _control_key()[1]


def test_recovery_seed_sem_control_recusa_antes_de_qualquer_put(
        monkeypatch: pytest.MonkeyPatch) -> None:
    keeper = _keeper("recovery")
    with pytest.raises(SyntheticContextRequired, match="reason=control_missing"):
        _seed(monkeypatch, "recovery-seed", keeper)
    assert _puts(keeper) == []


def test_recovery_seed_com_control_de_outro_contexto_recusa(
        monkeypatch: pytest.MonkeyPatch) -> None:
    keeper = _keeper("unit", context_manifest_sha256="5" * 64)
    _seed(monkeypatch, "unit-seed", keeper)
    keeper.events.clear()
    keeper.inputs = fx.seed_inputs("recovery") | {"record": _record("recovery")}
    with pytest.raises(SyntheticContextRequired, match="reason=control_mismatch"):
        _seed(monkeypatch, "recovery-seed", keeper)
    assert _puts(keeper) == []


@pytest.mark.parametrize("name", ["unit-seed", "recovery-seed"])
def test_escritas_dos_seeds_ficam_nas_triplas_ja_existentes(
        monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    keeper = _keeper("unit")
    _seed(monkeypatch, "unit-seed", keeper)
    if name == "recovery-seed":
        keeper.inputs = fx.seed_inputs("recovery") | {"record": _record("recovery")}
        _seed(monkeypatch, name, keeper)
    assert {(role, service, op) for role, service, op, _ in keeper.calls} <= (
        abi.MODE_EFFECT_CALLS[name])
    assert abi.MODE_EFFECT_CALLS[name] == frozenset({
        ("mode_seed", "dynamodb", "put_item"), ("mode_seed", "dynamodb", "get_item"),
        ("mode_seed", "s3", "put_object"), ("mode_seed", "s3", "get_object")})


def test_owner_novo_no_mesmo_tenant_grava_chaves_disjuntas_sem_seed_conflict(
        monkeypatch: pytest.MonkeyPatch) -> None:
    keeper = _keeper("unit")
    _seed(monkeypatch, "unit-seed", keeper)
    keeper.inputs = fx.seed_inputs("recovery") | {"record": _record("recovery")}
    _seed(monkeypatch, "recovery-seed", keeper)
    first = set(keeper.items) | set(keeper.objects)
    keeper.inputs = fx.seed_inputs("unit") | {"record": _record("unit", OTHER["owner_sha256"])}
    _seed(monkeypatch, "unit-seed", keeper, OTHER)
    keeper.inputs = fx.seed_inputs("recovery") | {
        "record": _record("recovery", OTHER["owner_sha256"])}
    _seed(monkeypatch, "recovery-seed", keeper, OTHER)
    second = (set(keeper.items) | set(keeper.objects)) - first
    assert len(second) == len(first)
    assert {key for key in keeper.items if key[0] == PK
            and key[1].startswith("PROD_CANARY")} == {
        (PK, f"{prefix}#{owner}{suffix}") for owner in (fx.OWNER_SHA, OTHER["owner_sha256"])
        for prefix, suffix in (("PROD_CANARY", "#unit"), ("PROD_CANARY", "#recovery"),
                               ("PROD_CANARY_CONTROL", ""))}
