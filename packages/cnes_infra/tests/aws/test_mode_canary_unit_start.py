"""Requests do start nativo do unit: construtores do keeper com os bytes do caminho do produto."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import ClosePromotionFence
from cnes_domain.orchestration.mode_canary import ModeCanaryBinding, SyntheticContextRequired
from cnes_infra.aws.mode_canary_dispatch import (
    StartPermit,
    UnitStart,
    acquire_request,
    attempt_request,
    bind_request,
    claim_request,
    confirm_requests,
    request_targets,
    reserve_request,
    start_claim,
    start_execution,
    start_request_sha256,
    step_reads,
    unit_dispatch,
)
from cnes_infra.aws.settings import _PRODUCTION_LEASE_SECONDS  # pyright: ignore[reportPrivateUsage]
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import run_partition, tenant_partition
from packages.cnes_infra.tests.aws.mode_canary_native import (
    NativeCase,
    native_start,
    seed_unit_run,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

TENANT = "prod-canary-" + "c" * 32
RUN = "unit-aaaaaaaaaaaaaaaa"
OWNER = "a" * 64
LOCATOR = {"tenant_id": TENANT, "run_ids": [RUN], "owner_sha256": OWNER,
           "record_sha256": "1" * 64, "phase": "unit", "context_kind": "promotion"}
CLOCKS = {
    "producao": (datetime(2026, 11, 13, 12, 5, tzinfo=UTC), _PRODUCTION_LEASE_SECONDS, True),
    "fim-do-mes": (datetime(2026, 12, 31, 23, 59, 30, tzinfo=UTC), _PRODUCTION_LEASE_SECONDS,
                   False),
    "inicio-do-mes": (datetime(2027, 1, 1, tzinfo=UTC), _PRODUCTION_LEASE_SECONDS, False),
    "microssegundos": (datetime(2026, 11, 13, 12, 5, 0, 250000, tzinfo=UTC), 90, False),
}


def binding(locator: Mapping[str, Any]) -> ModeCanaryBinding:
    return ModeCanaryBinding(locator["tenant_id"], tuple(locator["run_ids"]),
                             locator["owner_sha256"], locator["record_sha256"],
                             locator["phase"], locator["context_kind"])


def _reads(start: UnitStart, step: str) -> list[list[Any]]:
    return [["get_item", request] for request in step_reads(start, step)]


def _requests(start: UnitStart, permit: StartPermit, case: Mapping[str, Any]) -> dict[str, Any]:
    execution, run_item = case["result"]["execution_ref"], case["run_item"]
    return {
        "reserve": [*_reads(start, "reserve"), ["transact_write_items",
                                                reserve_request(start, run_item,
                                                                case["unit_items"])]],
        "admit": [["transact_write_items", acquire_request(start)], *_reads(start, "gate"),
                  ["update_item", attempt_request(start)], *_reads(start, "gate")],
        "claim": [*_reads(start, "claim"), ["transact_write_items",
                                            claim_request(start, permit)]],
        "bind": [*_reads(start, "bind"), ["transact_write_items",
                                          bind_request(start, permit, execution, run_item)]],
        "confirm": [["update_item", request]
                    for request in confirm_requests(start, permit, execution)],
    }


def built_start(case: Mapping[str, Any]) -> dict[str, Any]:
    """Returns: Resultado do start montado só pelos construtores sobre as entradas do caso."""
    now = datetime.fromisoformat(case["now"])
    dispatch = unit_dispatch(binding(case["locator"]), case["units"], now)
    start = UnitStart(dispatch, case["table"], now, case["lease_seconds"], case["promotion_id"])
    observed = case["result"]["permit"]
    permit = StartPermit(observed["token"], observed["fence_version"])
    return {
        "dispatch_id": dispatch.dispatch_id, "wave_id": dispatch.wave_id,
        "unit_ids": list(dispatch.unit_ids), "max_concurrency": dispatch.max_concurrency,
        "start_execution": start_execution(dispatch),
        "start_request_sha256": start_request_sha256(dispatch),
        "claim": start_claim(dispatch, permit.token, permit.fence_version, start.promotion_id),
        "permit": dict(observed), "execution_ref": case["result"]["execution_ref"],
        "requests": _requests(start, permit, case),
    }


@pytest.fixture
def client() -> Iterator[Any]:
    with mock_aws():
        dynamo = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(dynamo)
        yield dynamo


def native_case(client: Any, name: str) -> dict[str, Any]:
    now, lease, closed = CLOCKS[name]
    start = NativeCase(TENANT, RUN, now, lease, OWNER)
    seed_unit_run(client, _TABLE_NAME, start)
    if closed:
        plane = DynamoDBControlPlane(client, _TABLE_NAME, lambda: now)
        assert plane.close_promotion_fence(
            ClosePromotionFence("prod", OWNER, now - timedelta(minutes=1)))
    native = native_start(client, _TABLE_NAME, start)
    return {"name": name, "table": _TABLE_NAME, "locator": LOCATOR, "now": now.isoformat(),
            "lease_seconds": lease, "promotion_id": OWNER, **native}


@pytest.mark.parametrize("name", list(CLOCKS))
def test_construtores_reproduzem_byte_a_byte_os_requests_do_caminho_nativo(
        client: Any, name: str) -> None:
    case = native_case(client, name)
    assert json.dumps(built_start(case)) == json.dumps(case["result"])


def test_producao_usa_o_fence_fechado_pelo_owner_e_a_concorrencia_um(client: Any) -> None:
    result = native_case(client, "producao")["result"]
    assert result["permit"] == {"token": 1, "fence_version": 1}
    assert (result["max_concurrency"], len(result["unit_ids"])) == (1, 2)


def _attempt(case: dict[str, Any]) -> dict[str, Any]:
    [request] = [kwargs for name, kwargs in case["result"]["requests"]["admit"]
                 if name == "update_item"]
    return request


@pytest.mark.parametrize(("name", "expected"), [
    ("fim-do-mes", ("MONTH#2026-12", "2027-01-01T00:59:30.000000+00:00",
                    datetime(2026, 12, 1, tzinfo=UTC))),
    ("inicio-do-mes", ("MONTH#2027-01", "2027-01-01T01:00:00.000000+00:00",
                       datetime(2027, 1, 1, tzinfo=UTC))),
])
def test_mes_e_expires_do_attempts_vem_do_relogio_do_estagio_e_nao_do_lease(
        client: Any, name: str, expected: tuple[str, str, datetime]) -> None:
    month, lease, first_day = expected
    case = native_case(client, name)
    attempt = _attempt(case)
    assert attempt["Key"]["sk"] == {"S": month}
    expires = int((first_day + timedelta(days=400)).timestamp())
    assert attempt["ExpressionAttributeValues"][":expires"] == {"N": str(expires)}
    [acquire, *_] = case["result"]["requests"]["admit"]
    semaphore = acquire[1]["TransactItems"][1]["Update"]["ExpressionAttributeValues"]
    assert semaphore[":lease"] == {"S": lease}


def test_alvos_ficam_nas_particoes_do_run_e_nos_itens_do_gate(client: Any) -> None:
    case = native_case(client, "producao")
    requests = [request for step in built_start(case)["requests"].values() for request in step]
    targets = request_targets(requests)
    assert {(action, pk, sk) for action, _, pk, sk in targets if pk.startswith("ENV#")} == {
        ("ConditionCheckItem", "ENV#prod#FENCE", "FENCE"), ("GetItem", "ENV#prod#FENCE", "FENCE"),
        ("GetItem", "ENV#prod#SEMAPHORE", "UNIT"), ("UpdateItem", "ENV#prod#SEMAPHORE", "UNIT"),
        ("UpdateItem", "ENV#prod#ATTEMPTS", "MONTH#2026-11")}
    tenant = {(action, pk) for action, _, pk, _ in targets if not pk.startswith("ENV#")}
    partitions = {tenant_partition(TENANT), run_partition(TENANT, RUN)}
    assert {pk for _, pk in tenant} == partitions
    assert {action for action, _ in tenant} == {"GetItem", "ConditionCheckItem", "PutItem"}
    assert {table for _, table, _, _ in targets} == {_TABLE_NAME}


@pytest.mark.parametrize("request_", [
    ("delete_item", {"TableName": "t", "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}}),
    ("transact_write_items", {"TransactItems": [
        {"Delete": {"TableName": "t", "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}}}}]}),
])
def test_alvo_de_operacao_fora_do_start_recusa(request_: tuple[str, Any]) -> None:
    with pytest.raises(SyntheticContextRequired, match=r"^reason=dispatch_request "):
        request_targets([request_])


def _start(**changes: Any) -> UnitStart:
    now = datetime(2026, 11, 13, 12, 5, tzinfo=UTC)
    units = [{"tenant_id": TENANT, "run_id": RUN, "unit_id": unit_id, "state": "PENDING",
              "depends_on_unit_ids": [], "lease_until": None} for unit_id in ("n-a", "n-b")]
    values = {"dispatch": unit_dispatch(binding(LOCATOR), units, now), "table": _TABLE_NAME,
              "now": now, "lease_seconds": 90, "promotion_id": OWNER} | changes
    return UnitStart(**values)


@pytest.mark.parametrize(("changes", "reason"), [
    ({"now": datetime(2026, 11, 13, 9, 5, tzinfo=timezone(timedelta(hours=-3)))},
     "clock_not_utc"),
    ({"lease_seconds": 0}, "dispatch_lease"), ({"lease_seconds": True}, "dispatch_lease"),
    ({"promotion_id": ""}, "dispatch_promotion"), ({"promotion_id": None}, "dispatch_promotion"),
])
def test_start_fora_do_contrato_recusa(changes: dict[str, Any], reason: str) -> None:
    with pytest.raises(SyntheticContextRequired, match=rf"^reason={reason} "):
        _start(**changes)


@pytest.mark.parametrize(("token", "fence"), [(0, 0), (True, 0), (1, -1), (1, False)])
def test_permit_fora_do_contrato_recusa(token: Any, fence: Any) -> None:
    with pytest.raises(SyntheticContextRequired, match=r"^reason=dispatch_permit "):
        StartPermit(token, fence)


def test_passo_de_leitura_desconhecido_recusa() -> None:
    with pytest.raises(SyntheticContextRequired, match=r"^reason=dispatch_step "):
        step_reads(_start(), "release")


def _item(pk: str, sk: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"pk": {"S": pk}, "sk": {"S": sk}, "payload": {"S": json.dumps(payload)}}


def _run_item(state: str = "PROCESSING", sk: str | None = None) -> dict[str, Any]:
    pk = "TENANT#" + TENANT.encode().hex()
    return _item(pk, sk or "RUN#" + RUN.encode().hex(), {"state": state})


def _unit_items(*unit_ids: str) -> list[dict[str, Any]]:
    pk = "TENANT#" + TENANT.encode().hex() + "#RUN#" + RUN.encode().hex()
    return [_item(pk, "UNIT#" + unit_id.encode().hex(), {}) for unit_id in unit_ids]


@pytest.mark.parametrize(("run_item", "unit_items", "reason"), [
    (_run_item(sk="RUN#00"), _unit_items("n-a", "n-b"), "dispatch_run"),
    (_run_item("WAITING_INPUTS"), _unit_items("n-a", "n-b"), "dispatch_run"),
    (_run_item(), _unit_items("n-a"), "dispatch_units"),
    (_run_item(), _unit_items("n-b", "n-a"), "dispatch_units"),
])
def test_reserva_sobre_itens_lidos_fora_do_dispatch_recusa(
        run_item: dict[str, Any], unit_items: list[dict[str, Any]], reason: str) -> None:
    with pytest.raises(SyntheticContextRequired, match=rf"^reason={reason} "):
        reserve_request(_start(), run_item, unit_items)


def test_bind_sobre_item_de_outro_run_recusa() -> None:
    with pytest.raises(SyntheticContextRequired, match=r"^reason=dispatch_run "):
        bind_request(_start(), StartPermit(1, 0), "arn:exec", _run_item(sk="RUN#00"))
