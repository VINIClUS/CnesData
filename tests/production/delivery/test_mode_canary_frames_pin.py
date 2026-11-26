"""Formatos C2 do ledger de modo fixados por vetor: o keeper vendoriza e compara os bytes."""

from __future__ import annotations

import ast
import hashlib
import json
import sys
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from botocore.exceptions import ClientError

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    ModeCanaryRecord,
    SyntheticContextRequired,
)
from cnes_infra.aws.mode_canary import ModeCanaryContext
from cnes_infra.aws.mode_canary_frames import (
    ATTEMPTS,
    Effect,
    LedgerState,
    admission,
    dispatch_effect,
    initial_control,
    ledger_effect,
    record_item,
    transition,
)
from cnes_infra.control_plane.dynamodb_keys import tenant_partition

ROOT = Path(__file__).resolve().parents[3]
VECTOR: dict[str, Any] = json.loads(
    Path(__file__).with_name("mode_canary_frames_vector.json").read_text(encoding="utf-8"))
TABLE: str = VECTOR["table"]
CASES: dict[str, dict[str, Any]] = {case["name"]: case for case in VECTOR["cases"]}
BUILT = [name for name, case in CASES.items() if "refusal" not in case]
REFUSED = [name for name, case in CASES.items() if "refusal" in case]
ENTRIES = ("cnes_infra.aws.mode_canary_frames", "cnes_infra.aws.mode_canary_dispatch")
FIRST_PARTY = frozenset({"cnes_domain", "cnes_infra", "cnes_contracts"})


def _record(text: str) -> ModeCanaryRecord:
    return ModeCanaryRecord.from_mapping(json.loads(text))


def _binding(text: str) -> ModeCanaryBinding:
    record = _record(text)
    return ModeCanaryBinding(f"prod-canary-{record.canary_id}", record.run_ids,
                             record.owner_sha256, sha256(text.encode()).hexdigest(),
                             record.phase, record.context_kind)


def _effect(case: dict[str, Any], binding: ModeCanaryBinding) -> Effect:
    value = case["effect"]
    if "dispatch" in value:
        holder = PermitHolder(**value["dispatch"])
        return dispatch_effect(binding, _record(case["record"]), holder, tuple(value["unit_ids"]))
    return ledger_effect(value["kind"], value["request_sha256"], value["target_sha256"])


def _build(case: dict[str, Any]) -> Any:
    builder, text = case["builder"], case["record"]
    if builder == "record_item":
        return record_item(_record(text), text)
    if builder == "initial_control":
        return initial_control(_record(text), datetime.fromisoformat(case["admitted_at"]))
    binding = _binding(text)
    if builder == "transition":
        return transition(TABLE, binding, (case["effect_id"], case["ticket"]),
                          tuple(case["change"]))
    state = LedgerState(binding, _record(text), case["control"], case["ticket"])
    plan = admission(TABLE, state, _effect(case, binding), datetime.fromisoformat(case["now"]))
    return {"effect_id": plan.effect_id, "items": plan.items}


class _Dispatches:
    def __init__(self, dispatch: str, units: tuple[str, ...]) -> None:
        self._dispatch = SimpleNamespace(dispatch_id=dispatch, unit_ids=units)

    def get_run_dispatch(self, tenant_id: str, run_id: str) -> Any:
        return self._dispatch


def _cancelled(codes: list[str]) -> ClientError:
    return ClientError({"Error": {"Code": "TransactionCanceledException"},
                        "CancellationReasons": [{"Code": code} for code in codes]},
                       "TransactWriteItems")


class _Table:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.writes: list[tuple[str, dict[str, Any]]] = []

    def get_item(self, **request: Any) -> dict[str, Any]:
        key = (request["Key"]["pk"]["S"], request["Key"]["sk"]["S"])
        item = self.items.get(key)
        return {} if item is None else {"Item": item}

    def _admits(self, put: dict[str, Any]) -> bool:
        stored = self.items.get((put["Item"]["pk"]["S"], put["Item"]["sk"]["S"]))
        if put["ConditionExpression"] == "attribute_not_exists(pk)":
            return stored is None
        return stored is not None and stored["payload"] == put["ExpressionAttributeValues"][":old"]

    def _store(self, put: dict[str, Any]) -> None:
        self.items[(put["Item"]["pk"]["S"], put["Item"]["sk"]["S"])] = put["Item"]

    def put_item(self, **request: Any) -> None:
        self.writes.append(("put_item", request))
        if not self._admits(request):
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self._store(request)

    def transact_write_items(self, **request: Any) -> None:
        self.writes.append(("transact_write_items", request))
        puts = [item["Put"] for item in request["TransactItems"]]
        if not all(self._admits(put) for put in puts):
            raise _cancelled(["None" if self._admits(put) else "ConditionalCheckFailed"
                              for put in puts])
        for put in puts:
            self._store(put)


def _module_path(module: str) -> Path:
    parts = module.split(".")
    return ROOT / "packages" / parts[0] / "src" / Path(*parts).with_suffix(".py")


def _runtime_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    typing_only = {id(node) for block in ast.walk(tree) if isinstance(block, ast.If)
                   and "TYPE_CHECKING" in ast.unparse(block.test)
                   for statement in block.body for node in ast.walk(statement)}
    names: set[str] = set()
    for node in ast.walk(tree):
        if id(node) in typing_only:
            continue
        if isinstance(node, ast.Import):
            names |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names.add(node.module)
    return names


def _closure() -> tuple[set[str], set[str]]:
    pending, seen, outside = list[str](ENTRIES), set[str](), set[str]()
    while pending:
        module = pending.pop()
        if module in seen:
            continue
        seen.add(module)
        for name in _runtime_imports(_module_path(module)):
            root = name.split(".")[0]
            if root in FIRST_PARTY:
                pending.append(name)
            else:
                outside.add(root)
    return {_module_path(module).relative_to(ROOT).as_posix() for module in seen}, outside


def test_closure_dos_formatos_e_a_do_vetor_com_o_sha256_de_cada_arquivo() -> None:
    files, _ = _closure()
    assert set(VECTOR["closure"]) == files
    for path, digest in VECTOR["closure"].items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == digest


def test_closure_dos_formatos_so_importa_a_biblioteca_padrao() -> None:
    _, outside = _closure()
    assert outside <= sys.stdlib_module_names


def test_fonte_dos_formatos_tem_o_sha256_fixado_no_vetor() -> None:
    data = (ROOT / VECTOR["source"]["path"]).read_bytes()
    assert hashlib.sha256(data).hexdigest() == VECTOR["source"]["sha256"]


def test_vetor_cobre_dispatch_ledger_record_control_e_as_recusas() -> None:
    assert set(BUILT) == {"dispatch-unit", "dispatch-unit-replay", "ledger-admit",
                          "ledger-unknown", "ledger-settle", "ledger-settle-replay",
                          "record-item", "initial-control"}
    reasons = {CASES[name]["refusal"] for name in REFUSED}
    assert reasons == {f"reason={reason} field=tenant_id"
                       for reason in ("ticket_conflict", "budget_exhausted", "ticket_settled",
                                      "ticket_mismatch")}
    hangs = [CASES[name] for name in REFUSED if "hangs" in name]
    assert [json.loads(case["control"])["admitted_counts"]["recovery_hangs"]
            for case in hangs] == [1]


@pytest.mark.parametrize("name", BUILT)
def test_construtor_reproduz_os_bytes_do_vetor(name: str) -> None:
    case = CASES[name]
    built = _build(case)
    assert built == case["result"]
    assert json.dumps(built, sort_keys=True) == json.dumps(case["result"], sort_keys=True)


@pytest.mark.parametrize("name", REFUSED)
def test_construtor_recusa_como_o_vetor(name: str) -> None:
    with pytest.raises(SyntheticContextRequired) as refused:
        _build(CASES[name])
    assert str(refused.value) == CASES[name]["refusal"]


@pytest.mark.parametrize("phase", ["dispatch-unit", "ledger-admit"])
def test_container_grava_os_mesmos_frames_que_o_vetor(phase: str) -> None:
    case = CASES[phase]
    effect = case["effect"]
    dispatch = effect["dispatch"]["dispatch_id"] if "dispatch" in effect else "0" * 16
    units = tuple(effect.get("unit_ids", ()))
    binding = _binding(case["record"])
    table = _Table()
    pk = tenant_partition(binding.tenant_id)
    for sort_key, text in ((f"PROD_CANARY#{binding.owner_sha256}#{binding.phase}",
                            case["record"]),
                           (f"PROD_CANARY_CONTROL#{binding.owner_sha256}", case["control"])):
        table.items[(pk, sort_key)] = {"pk": {"S": pk}, "sk": {"S": sort_key},
                                       "entity": {"S": "X"}, "payload": {"S": text}}
    dispatches = _Dispatches(dispatch, units)
    context = ModeCanaryContext(table, TABLE, binding, dispatches)
    now = datetime.fromisoformat(case["now"])
    if phase == "dispatch-unit":
        context.binding_for(PermitHolder(**case["effect"]["dispatch"]), now)
        expected = {"TransactItems": case["result"]["items"]}
        assert table.writes == [("transact_write_items", expected)]
        return
    effect = context.admit_effect("run-recovery", "1" * 64, "2" * 64, now)
    context.mark_unknown(effect)
    context.settle(effect, "3" * 64)
    assert table.writes == [
        ("transact_write_items", {"TransactItems": case["result"]["items"]}),
        ("put_item", CASES["ledger-unknown"]["result"]),
        ("put_item", CASES["ledger-settle"]["result"]),
    ]


def test_teto_de_tentativas_do_ledger_e_fixado_no_vetor() -> None:
    assert VECTOR["attempts"] == ATTEMPTS == 3


class _Cancelling(_Table):
    def transact_write_items(self, **request: Any) -> None:
        self.writes.append(("transact_write_items", request))
        raise _cancelled(["None", "TransactionConflict"])


def test_container_desiste_com_ledger_contended_no_teto_do_vetor() -> None:
    case = CASES["ledger-admit"]
    binding = _binding(case["record"])
    table = _Cancelling()
    pk = tenant_partition(binding.tenant_id)
    for sort_key, text in ((f"PROD_CANARY#{binding.owner_sha256}#{binding.phase}",
                            case["record"]),
                           (f"PROD_CANARY_CONTROL#{binding.owner_sha256}", case["control"])):
        table.items[(pk, sort_key)] = {"pk": {"S": pk}, "sk": {"S": sort_key},
                                       "entity": {"S": "X"}, "payload": {"S": text}}
    context = ModeCanaryContext(table, TABLE, binding, _Dispatches("0" * 16, ()))
    with pytest.raises(SyntheticContextRequired, match="reason=ledger_contended"):
        context.admit_effect("run-recovery", "1" * 64, "2" * 64,
                             datetime.fromisoformat(case["now"]))
    assert len(table.writes) == VECTOR["attempts"]


def test_container_recusa_ticket_gravado_na_chave_de_outro_effect_id_sem_escrever() -> None:
    case = CASES["ledger-unknown"]
    binding = _binding(case["record"])
    table = _Table()
    pk = tenant_partition(binding.tenant_id)
    other = "f" * 64
    sort_key = f"PROD_CANARY_EFFECT#{binding.owner_sha256}#{other}"
    table.items[(pk, sort_key)] = {"pk": {"S": pk}, "sk": {"S": sort_key},
                                   "entity": {"S": "X"}, "payload": {"S": case["ticket"]}}
    context = ModeCanaryContext(table, TABLE, binding, _Dispatches("0" * 16, ()))
    with pytest.raises(SyntheticContextRequired,
                       match=r"^reason=ticket_mismatch field=tenant_id$"):
        context.mark_unknown(other)
    assert table.writes == []
