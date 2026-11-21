"""Efeitos de modo do adapter ABI2: seed e start pelos aliases e métodos do keeper (#465)."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from cnes_domain.orchestration.mode_canary import ModeCanaryBinding
from scripts.promotion import _adapter_abi as abi
from scripts.promotion import mode_canary_seed, promote_api
from scripts.promotion.mode_canary import RESULT_FIELDS, RESULT_SCHEMA, ModeCanaryDenied
from scripts.promotion.mode_canary_contract import ContractError
from scripts.promotion.mode_canary_seed import SeedBinding, build_mode_seed
from scripts.promotion.promote_mode_effects import ModeCalls
from tests.production.delivery import test_mode_canary as mc
from tests.production.delivery.test_adapter_handlers import API, SHA
from tests.production.delivery.test_promote_close import OWNER, canonical, sha

EFFECTS = ("unit-seed", "unit-start", "recovery-seed", "recovery-start", "audit-start")
VECTOR = json.loads(Path(__file__).with_name("mode_effect_call_vector.json").read_text("utf-8"))
FIXTURES = Path(__file__).resolve().parents[3] / "tests/production/fixtures/mode_canary"
FILES = {"fixture-manifest": "fixture-manifest.json", "fixture-cnes-local": "cnes-local-v1.parquet",
         "fixture-cnes-nacional": "cnes-nacional-v1.parquet"}
NOW = mc._NOW
STARTED = "2026-11-13T12:01:00Z"
OWNER_SHA = OWNER["owner_sha256"]
TENANT = f"prod-canary-{mc._CANARY}"


def runs(mode: str) -> list[str]:
    prefixes = ["unit"] if mode == "unit" else ["recovery-a", "recovery-b"]
    return [f"{prefix}-{OWNER_SHA[:16]}" for prefix in prefixes]


def fixtures() -> dict[str, bytes]:
    return {name: (FIXTURES / file).read_bytes() for name, file in FILES.items()}


def record(mode: str, **changes: Any) -> bytes:
    fields = {"owner_sha256": OWNER_SHA, "run_ids": runs(mode),
              "fixture_manifest_sha256": sha(fixtures()["fixture-manifest"])}
    return canonical(mc._record(mode, **fields) | changes)


def start_doc(frozen: bool = False, started: str = STARTED) -> bytes:
    return canonical({"version": 1, "kind": "mode-start", "frozen": frozen, "started_at": started})


def seed(mode: str, owner: str = OWNER_SHA) -> Any:
    files = {FILES[name]: raw for name, raw in fixtures().items()}
    moment = mc._NOW.replace(minute=1)
    run_ids = tuple(run.replace(OWNER_SHA[:16], owner[:16]) for run in runs(mode))
    return build_mode_seed(SeedBinding(TENANT, run_ids, owner, moment, "cnesdata-prod-data"),
                           files)


def seed_inputs(mode: str, frozen: bool = False) -> dict[str, bytes]:
    return {"record": record(mode), "mode-start": start_doc(frozen), **fixtures()}


def start_inputs(mode: str, frozen: bool = False, **changes: bytes) -> dict[str, bytes]:
    inputs = {"record": record(mode), "readbacks": canonical(mc._readbacks()),
              "previous": b"null\n", "mode-seed": seed("unit" if mode == "unit" else "recovery")
              .manifest, "mode-start": start_doc(frozen)}
    if mode == "audit":
        inputs["audit-event-ids"] = canonical([f"run.publishing.{runs(mode)[0]}"])
    return inputs | changes


@dataclass
class Keeper:
    """Context do keeper: ABI2 original mais os métodos de efeito de modo."""

    inputs: dict[str, bytes]
    responses: dict[str, Any] = field(default_factory=dict)
    unit: dict[str, Any] | None = None
    after: Any = None
    events: list[tuple[Any, ...]] = field(default_factory=list)
    observed: list[tuple[str, bytes]] = field(default_factory=list)
    items: dict[tuple[str, str], Any] = field(default_factory=dict)
    objects: dict[tuple[str, str], bytes] = field(default_factory=dict)

    @property
    def calls(self) -> list[tuple[Any, ...]]:
        return [event[1:] for event in self.events if event[0] == "call"]

    def read(self, name: str) -> bytes:
        return self.inputs[name]

    def guard(self) -> None:
        self.events.append(("guard",))

    def observe(self, kind: str, raw: bytes) -> str:
        self.observed.append((kind, raw))
        return sha(raw)

    def evidence_snapshot(self) -> dict[str, Any]:
        return {"call_receipts_sha256": "e" * 64, "pending": [], "unknown": []}

    def call(self, role: str, service: str, operation: str, parameters: dict[str, Any]) -> Any:
        self.events.append(("call", role, service, operation, parameters))
        if service in {"dynamodb", "s3"}:
            return getattr(self, f"_{operation}")(parameters)
        response = self.responses[operation]
        if isinstance(response, Exception):
            raise response
        return deepcopy(response)

    def _put_item(self, request: dict[str, Any]) -> dict[str, Any]:
        key = (request["Item"]["pk"]["S"], request["Item"]["sk"]["S"])
        if key in self.items:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self.items[key] = deepcopy(request["Item"])
        return {}

    def _get_item(self, request: dict[str, Any]) -> dict[str, Any]:
        found = self.items.get((request["Key"]["pk"]["S"], request["Key"]["sk"]["S"]))
        return {} if found is None else {"Item": deepcopy(found)}

    def _put_object(self, request: dict[str, Any]) -> dict[str, Any]:
        key = (request["Bucket"], request["Key"])
        if key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[key] = request["Body"]
        return {}

    def _get_object(self, request: dict[str, Any]) -> dict[str, Any]:
        return {"Body": self.objects[(request["Bucket"], request["Key"])]}

    def ledger_admit_effect(self, kind: str, request: str, target: str, admitted_at: str) -> str:
        self.events.append(("admit", kind, request, target, admitted_at))
        return hashlib.sha256(f"{kind}{request}".encode()).hexdigest()

    def ledger_mark_unknown(self, effect_id: str) -> None:
        self.events.append(("unknown", effect_id))

    def ledger_settle(self, effect_id: str, receipt: str) -> None:
        self.events.append(("settle", effect_id, receipt))

    def dispatch_unit(self, locator: str) -> dict[str, Any]:
        self.events.append(("dispatch", locator))
        return deepcopy(self.unit or {})

    def mode_readbacks(self) -> Any:
        self.events.append(("readbacks",))
        return deepcopy(self.after)


def request(name: str, inputs: dict[str, bytes], late: bool = False) -> dict[str, Any]:
    epoch = int(NOW.timestamp())
    stage = API | {"stage": abi.MODE_OPERATIONS[name][1], "close": epoch - 120,
                   "fixed_deadline": epoch + 900, "admitted_at": epoch - 5,
                   "deadline": epoch - 1 if late else epoch + 600}
    return {"abi_version": 2, "operation": name, "repository": OWNER["repository"],
            "source_sha": SHA, "owner": OWNER, "checkpoint_sha256": API["checkpoint_sha256"],
            "activation_sha256": "a" * 64, "stage": stage,
            "inputs": {key: {"sha256": sha(raw), "size": len(raw)}
                       for key, raw in sorted(inputs.items())}}


def keeper(mode: str, inputs: dict[str, bytes]) -> Keeper:
    unit = {"execution": {"status": "SUCCEEDED", "stateMachineArn": mc._MACHINE},
            "tasks": mc._stopped("unit")["tasks"], "outputs": [{"sha256": "6" * 64, "size": 10}]}
    responses = {} if mode == "unit" else {"run_task": mc._launched(mode),
                                           "describe_tasks": mc._stopped(mode)}
    return Keeper(inputs, responses, unit, mc._readbacks())


def run(mp: pytest.MonkeyPatch, name: str, context: Keeper, late: bool = False) -> Any:
    mp.setattr(promote_api, "CLOCK", lambda: NOW)
    return promote_api.execute_shared_operation(name, request(name, context.inputs, late), context)


def untouched(context: Keeper) -> bool:
    return all(event[0] in {"guard"} for event in context.events) and not context.observed


@pytest.mark.parametrize("name", EFFECTS)
def test_efeito_de_modo_valida_o_request_em_vez_de_recusar_sem_ancora(name: str) -> None:
    with pytest.raises(ValueError, match="request=fields"):
        promote_api.execute_shared_operation(name, {}, Keeper({}))


@pytest.mark.parametrize("name", EFFECTS)
def test_tabela_de_efeitos_e_o_vetor_do_contrato_do_keeper(name: str) -> None:
    row = VECTOR["operations"][name]
    assert abi.MODE_INPUTS[name] == frozenset(row["inputs"])
    assert abi.MODE_STAGES[name] == frozenset({row["stage"]})
    assert abi.MODE_OBSERVATIONS[name] == row["observation"]
    assert abi.MODE_EFFECT_STEPS[name] == frozenset(row["steps"])
    assert abi.MODE_EFFECT_CALLS[name] == frozenset(tuple(call) for call in row["calls"])
    assert abi.MODE_EFFECT_METHODS[name] == frozenset(row["methods"])


def test_erros_sdk_fechados_sao_os_tolerados_pelo_seed() -> None:
    errors = VECTOR["sdk_errors"]
    assert frozenset(errors["dynamodb:put_item"]) == mode_canary_seed.ITEM_EXISTS
    assert frozenset(errors["s3:put_object"]) == mode_canary_seed.OBJECT_EXISTS


@pytest.mark.parametrize("mode", ["unit", "recovery"])
def test_seed_grava_pelo_alias_mode_seed_e_observa_o_manifesto(
        monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    context = Keeper(seed_inputs(mode))
    result = run(monkeypatch, f"{mode}-seed", context)
    expected = seed(mode)
    assert context.observed == [("mode-seed", expected.manifest)]
    assert result["observations"] == [{"kind": "mode-seed", "sha256": expected.sha256,
                                       "size": len(expected.manifest)}]
    assert {call[:3] for call in context.calls} <= abi.MODE_EFFECT_CALLS[f"{mode}-seed"]
    puts = [call[3] for call in context.calls if call[2] == "put_item"]
    assert [put["Item"] for put in puts] == list(expected.items)
    assert all(put["TableName"] == "cnesdata-prod-control-plane" for put in puts)
    assert {key for _, key in context.objects} == {obj.key for obj in expected.objects}
    replay = Keeper(context.inputs, items=context.items, objects=context.objects)
    assert run(monkeypatch, f"{mode}-seed", replay) == result


@pytest.mark.parametrize("name", ["unit-seed", "recovery-seed", "unit-start", "recovery-start"])
def test_seed_e_start_congelados_sao_negados_antes_de_qualquer_efeito(
        monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    mode = name.split("-", 1)[0]
    inputs = seed_inputs(mode, True) if name.endswith("seed") else start_inputs(mode, True)
    context = keeper(mode, inputs)
    with pytest.raises(ModeCanaryDenied, match=f"reason=freeze_denied operation={name}"):
        run(monkeypatch, name, context)
    assert untouched(context)


def test_audit_start_congelado_e_admitido_com_prova_do_freeze(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = keeper("audit", start_inputs("audit", frozen=True))
    run(monkeypatch, "audit-start", context)
    result = json.loads(context.observed[-1][1])
    assert result["freeze_proof_sha256"] is not None
    assert [call[:3] for call in context.calls] == [("mode_audit_launch", "ecs", "run_task"),
                                                    ("mode_observe", "ecs", "describe_tasks")]
    environment = context.calls[0][3]["overrides"]["containerOverrides"][0]["environment"]
    assert [item["name"] for item in environment] == ["PROD_CANARY_AUDIT_OBSERVE"]
    assert not any(event[0] in {"admit", "settle", "unknown"} for event in context.events)


@pytest.mark.parametrize("name", ["unit-start", "recovery-start", "audit-start"])
def test_start_emite_mode_result_com_os_21_campos(monkeypatch: pytest.MonkeyPatch,
                                                  name: str) -> None:
    mode = name.split("-", 1)[0]
    context = keeper(mode, start_inputs(mode, frozen=mode == "audit"))
    result = run(monkeypatch, name, context)
    kind, raw = context.observed[-1]
    document = json.loads(raw)
    assert (kind, raw) == ("mode-result", canonical(document))
    assert tuple(sorted(document)) == tuple(sorted(RESULT_FIELDS))
    assert len(RESULT_FIELDS) == 21
    assert (document["schema"], document["mode"]) == (RESULT_SCHEMA, mode)
    assert document["seed_sha256"] == sha(context.inputs["mode-seed"])
    assert (document["started_at"], document["pending"], document["unknown"]) == (STARTED, [], [])
    assert result["observations"] == [{"kind": kind, "sha256": sha(body), "size": len(body)}
                                      for kind, body in context.observed]


def test_recovery_start_admite_e_liquida_o_efeito_no_ledger_do_keeper(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = keeper("recovery", start_inputs("recovery"))
    run(monkeypatch, "recovery-start", context)
    ledger = [event for event in context.events if event[0] in {"admit", "settle", "unknown"}]
    assert [event[0] for event in ledger] == ["admit", "settle"]
    kind, _, target, admitted_at = ledger[0][1:]
    assert (kind, target, admitted_at) == ("run-recovery", sha(mc._REV["recovery"].encode()),
                                           "2026-11-13T12:05:00Z")
    assert ledger[1][2] == sha(context.observed[0][1])
    assert context.observed[0][0] == "recovery-task"


def test_recovery_start_marca_unknown_quando_o_lancamento_falha(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = keeper("recovery", start_inputs("recovery"))
    context.responses["run_task"] = RuntimeError("native SDK outcome unknown")
    run(monkeypatch, "recovery-start", context)
    document = json.loads(context.observed[-1][1])
    assert [event[0] for event in context.events if event[0] in {"admit", "unknown"}] == [
        "admit", "unknown"]
    assert len(document["unknown"]) == 1
    assert document["unknown"][0].startswith("run_task:mc-")


def test_unit_start_despacha_pelo_keeper_com_o_locator_do_binding(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = keeper("unit", start_inputs("unit"))
    run(monkeypatch, "unit-start", context)
    [locator] = [event[1] for event in context.events if event[0] == "dispatch"]
    binding = ModeCanaryBinding.from_locator(locator)
    assert (binding.tenant_id, binding.run_ids, binding.owner_sha256) == (
        TENANT, tuple(runs("unit")), OWNER_SHA)
    assert (binding.record_sha256, binding.phase) == (sha(context.inputs["record"]), "unit")
    assert [kind for kind, _ in context.observed] == [
        "unit-execution", "unit-task", "unit-output", "mode-result"]
    assert context.calls == []


def test_routing_after_vem_da_releitura_do_keeper_depois_do_efeito(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = keeper("recovery", start_inputs("recovery"))
    context.after["schedules"]["recovery"]["Target"]["EcsParameters"]["TaskCount"] = 2
    run(monkeypatch, "recovery-start", context)
    names = [event[0] for event in context.events if event[0] != "guard"]
    assert names.index("readbacks") > names.index("settle")
    document = json.loads(context.observed[-1][1])
    assert document["pending"] == ["routing_changed"]
    assert document["routing_after_sha256"] == sha(canonical(context.after))


@pytest.mark.parametrize(("name", "change"), [
    ("unit-seed", {"owner_sha256": "8" * 64}), ("recovery-start", {"activation_sha256": "b" * 64}),
    ("audit-start", {"owner_sha256": "8" * 64}),
])
def test_efeito_recusa_record_de_outro_dono_antes_de_qualquer_chamada(
        monkeypatch: pytest.MonkeyPatch, name: str, change: dict[str, str]) -> None:
    mode = name.split("-", 1)[0]
    inputs = seed_inputs(mode) if name.endswith("seed") else start_inputs(mode)
    context = keeper(mode, inputs | {"record": record(mode, **change)})
    with pytest.raises(ValueError, match="mode=record_binding"):
        run(monkeypatch, name, context)
    assert untouched(context)


def test_seed_recusa_fixture_fora_do_record(monkeypatch: pytest.MonkeyPatch) -> None:
    context = Keeper(seed_inputs("unit") | {"record": record(
        "unit", fixture_manifest_sha256="9" * 64)})
    with pytest.raises(ValueError, match="mode=fixture_binding"):
        run(monkeypatch, "unit-seed", context)
    assert untouched(context)


def test_start_recusa_seed_de_outro_contexto(monkeypatch: pytest.MonkeyPatch) -> None:
    other = seed("recovery", owner="8" * 64).manifest
    context = keeper("recovery", start_inputs("recovery") | {"mode-seed": other})
    with pytest.raises(ValueError, match="mode=seed_binding"):
        run(monkeypatch, "recovery-start", context)
    assert untouched(context)


@pytest.mark.parametrize("name", EFFECTS)
def test_efeito_recusa_depois_do_prazo_do_estagio(monkeypatch: pytest.MonkeyPatch,
                                                  name: str) -> None:
    mode = name.split("-", 1)[0]
    inputs = seed_inputs(mode) if name.endswith("seed") else start_inputs(mode)
    context = keeper(mode, inputs)
    with pytest.raises(ValueError, match="mode=stage_deadline"):
        run(monkeypatch, name, context, late=True)
    assert untouched(context)


@pytest.mark.parametrize(("raw", "error"), [
    (canonical({"version": 1, "kind": "mode-start", "frozen": False}), "mode=fields"),
    (canonical({"version": 1, "kind": "mode-start", "frozen": 0, "started_at": STARTED}),
     "mode=mode_start"),
    (start_doc(started="2026-11-13 12:01:00"), "mode=mode_start"),
    (start_doc(started="2026-13-01T00:00:00Z"), "mode=mode_start"),
])
def test_start_recusa_documento_de_inicio_fora_do_contrato(
        monkeypatch: pytest.MonkeyPatch, raw: bytes, error: str) -> None:
    context = keeper("recovery", start_inputs("recovery") | {"mode-start": raw})
    with pytest.raises(ValueError, match=error):
        run(monkeypatch, "recovery-start", context)
    assert untouched(context)


def test_start_reentra_com_o_resultado_anterior(monkeypatch: pytest.MonkeyPatch) -> None:
    first = keeper("recovery", start_inputs("recovery"))
    first.responses["run_task"] = RuntimeError("timeout")
    run(monkeypatch, "recovery-start", first)
    previous = first.observed[-1][1]
    again = keeper("recovery", start_inputs("recovery", previous=previous))
    run(monkeypatch, "recovery-start", again)
    document = json.loads(again.observed[-1][1])
    assert document["unknown"] == json.loads(previous)["unknown"]
    late = start_inputs("recovery", previous=previous) | {"mode-start": start_doc(
        started="2026-11-13T12:02:00Z")}
    with pytest.raises(ValueError, match=r"reentry\.started_at"):
        run(monkeypatch, "recovery-start", keeper("recovery", late))


def test_ponte_recusa_chamada_metodo_e_observacao_fora_da_operacao(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW)
    context = Keeper({})
    bridge = ModeCalls(context, "recovery-start")
    with pytest.raises(ValueError, match="mode=call alias=mode_seed operation=dynamodb:PutItem"):
        bridge.call("mode_seed", "dynamodb:PutItem", {})
    with pytest.raises(ValueError, match="mode=call alias=mode_dispatch operation=ecs:StopTask"):
        bridge.call("mode_dispatch", "ecs:StopTask", {})
    with pytest.raises(ValueError, match="mode=observation name=unit-task"):
        bridge.observe("unit-task", b"{}\n")
    with pytest.raises(ValueError, match="mode=method name=dispatch_unit"):
        bridge.dispatch_unit(ModeCanaryBinding(TENANT, tuple(runs("unit")), OWNER_SHA,
                                               "1" * 64, "unit", "promotion"))
    assert context.events == []
    assert bridge.now() == NOW


class _Tampered(Keeper):
    def observe(self, kind: str, raw: bytes) -> str:
        self.observed.append((kind, raw))
        return "0" * 64

    def _get_object(self, request: dict[str, Any]) -> dict[str, Any]:
        return {"Body": self.objects[(request["Bucket"], request["Key"])].decode("latin-1")}


def test_ponte_recusa_digest_de_observacao_divergente_do_keeper() -> None:
    bridge = ModeCalls(_Tampered({}), "recovery-start")
    with pytest.raises(ValueError, match="mode=observation_digest name=recovery-task"):
        bridge.observe("recovery-task", b"{}\n")


def test_seed_recusa_releitura_de_objeto_sem_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="mode=object_body"):
        run(monkeypatch, "unit-seed", _Tampered(seed_inputs("unit")))


def test_seed_recusa_depois_do_prazo_do_canary(monkeypatch: pytest.MonkeyPatch) -> None:
    late = record("unit", canary_deadline="2026-11-13T12:04:00Z")
    context = Keeper(seed_inputs("unit") | {"record": late})
    with pytest.raises(ContractError, match="field=canary_deadline"):
        run(monkeypatch, "unit-seed", context)
    assert untouched(context)


@pytest.mark.parametrize("raw", [b"[]\n", canonical({"files": {"x": {"source_type": "SIHD"}}})])
def test_seed_recusa_manifesto_de_fixture_sem_as_fontes_do_cnes(
        monkeypatch: pytest.MonkeyPatch, raw: bytes) -> None:
    inputs = seed_inputs("unit") | {"fixture-manifest": raw,
                                    "record": record("unit", fixture_manifest_sha256=sha(raw))}
    context = Keeper(inputs)
    with pytest.raises(ContractError, match="field=fixture_manifest"):
        run(monkeypatch, "unit-seed", context)
    assert untouched(context)


class _DeniedItem(Keeper):
    def _put_item(self, request: dict[str, Any]) -> dict[str, Any]:
        raise ClientError({"Error": {"Code": "AccessDeniedException"}}, "PutItem")


class _DeniedObject(Keeper):
    def _put_object(self, request: dict[str, Any]) -> dict[str, Any]:
        raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")


@pytest.mark.parametrize("denied", [_DeniedItem, _DeniedObject])
def test_seed_propaga_erro_sdk_fora_dos_conflitos_fechados(
        monkeypatch: pytest.MonkeyPatch, denied: type[Keeper]) -> None:
    with pytest.raises(ClientError, match="AccessDenied"):
        run(monkeypatch, "unit-seed", denied(seed_inputs("unit")))


def test_seed_recusa_objeto_ja_gravado_com_outros_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    first = Keeper(seed_inputs("unit"))
    run(monkeypatch, "unit-seed", first)
    key = next(key for key in first.objects if key[1].endswith("manifest.json"))
    tampered = first.objects | {key: b"{}"}
    replay = Keeper(first.inputs, items=first.items, objects=tampered)
    with pytest.raises(ContractError, match=f"reason=seed_conflict key={key[1]}"):
        run(monkeypatch, "unit-seed", replay)


def test_releitura_ausente_do_keeper_deixa_o_resultado_pendente(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = keeper("recovery", start_inputs("recovery"))
    context.after = None
    run(monkeypatch, "recovery-start", context)
    assert json.loads(context.observed[-1][1])["pending"] == ["routing_after_missing"]
