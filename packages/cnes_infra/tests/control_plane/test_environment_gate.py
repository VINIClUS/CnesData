"""Mixin DynamoDB do gate de ambiente sobre moto: chaves, transação de acquire e CAS."""

from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    AcquisitionStatus,
    BindUnitPermit,
    ClosePromotionFence,
    EnvironmentGateState,
    FenceState,
    PermitAcquisition,
    PermitHolder,
    PromotionFence,
    ReanchorUnitPermit,
    ReleaseUnitPermit,
    RenewUnitPermit,
    ReopenPromotionFence,
    UnitPermit,
    open_fence,
)
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.ports.environment_gate import EnvironmentGatePort
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_environment_gate import (
    OUTBOX_CURSOR_KEY,
    attempts_key,
    fence_key,
    semaphore_key,
)
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _TABLE_NAME, _create_table

NOW = datetime(2026, 10, 11, 12, tzinfo=UTC)
LATER = NOW + timedelta(minutes=30)
ENV = "prod"
PROMOTION = "release-2026-10-11"
OTHER_PROMOTION = "release-2026-10-12"
HOLDER = PermitHolder("354130", "run-01", "0123456789abcdef")
OTHER = PermitHolder("354130", "run-02", "fedcba9876543210")
_GATE_OPERATIONS = {"get_item", "update_item", "transact_write_items"}
type _Gate = tuple[DynamoDBControlPlane, "ScriptedClient"]


class ScriptedClient:
    """Repassa ao moto, registra chamadas e executa efeitos injetados antes de cada uma."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.effects: dict[str, list[Callable[[], object]]] = {}

    def inject(self, operation: str, *effects: Callable[[], object]) -> None:
        self.effects.setdefault(operation, []).extend(effects)

    def operations(self) -> list[str]:
        return [name for name, _ in self.calls]

    def requests(self, operation: str) -> list[dict[str, Any]]:
        return [request for name, request in self.calls if name == operation]

    def __getattr__(self, name: str) -> Any:
        operation = getattr(self.client, name)

        def call(**kwargs: Any) -> Any:
            self.calls.append((name, kwargs))
            pending = self.effects.get(name)
            if pending:
                pending.pop(0)()
            return operation(**kwargs)

        return call


def _raise(code: str, *reasons: str) -> Callable[[], object]:
    def effect() -> object:
        response: dict[str, Any] = {"Error": {"Code": code, "Message": code}}
        if reasons:
            response["CancellationReasons"] = [{"Code": reason} for reason in reasons]
        raise ClientError(cast("Any", response), "GateOperation")

    return effect


@pytest.fixture
def gate() -> Iterator[_Gate]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        scripted = ScriptedClient(client)
        yield DynamoDBControlPlane(scripted, _TABLE_NAME, lambda: NOW), scripted


def _acquire(
    holder: PermitHolder = HOLDER, *, canary: str | None = None, now: datetime = NOW,
) -> AcquireUnitPermit:
    return AcquireUnitPermit(ENV, holder, now, 300, canary)


def _permit(acquisition: PermitAcquisition) -> UnitPermit:
    assert acquisition.permit is not None
    return acquisition.permit


def _raw(client: ScriptedClient, key: tuple[str, str]) -> dict[str, Any]:
    response = client.client.get_item(TableName=_TABLE_NAME, Key=item_key(*key))
    return response["Item"]


def _rival(client: ScriptedClient) -> DynamoDBControlPlane:
    return DynamoDBControlPlane(client.client, _TABLE_NAME, lambda: NOW)


def test_chaves_do_gate_sao_texto_puro_em_particoes_distintas() -> None:
    assert fence_key("prod") == ("ENV#prod#FENCE", "FENCE")
    assert semaphore_key("prod") == ("ENV#prod#SEMAPHORE", "UNIT")
    assert attempts_key("prod", "2026-10") == ("ENV#prod#ATTEMPTS", "MONTH#2026-10")
    assert OUTBOX_CURSOR_KEY == ("OUTBOX#CURSOR", "CURSOR")


@pytest.mark.parametrize("environment", ["", "Prod", "prod#x", "prod/x", "1prod", "a" * 33])
@pytest.mark.parametrize(
    "build", [fence_key, semaphore_key, lambda environment: attempts_key(environment, "2026-10")],
)
def test_ambiente_invalido_nao_vira_chave(
    build: Callable[[str], tuple[str, str]], environment: str,
) -> None:
    with pytest.raises(ValueError, match="reason=invalid_environment"):
        build(environment)


@pytest.mark.parametrize("period", ["2026-13", "2026-00", "2026-1", "202610", "2026-10-01", ""])
def test_periodo_invalido_nao_vira_chave(period: str) -> None:
    with pytest.raises(ValueError, match="reason=invalid_period"):
        attempts_key("prod", period)


def test_adapter_implementa_a_porta_do_gate(gate: _Gate) -> None:
    adapter, _ = gate

    assert isinstance(adapter, EnvironmentGatePort)


def test_ambiente_sem_itens_e_fence_aberto_sem_permit(gate: _Gate) -> None:
    adapter, client = gate

    assert adapter.observe_environment_gate(ENV) == EnvironmentGateState(
        ENV, open_fence(ENV), None,
    )
    assert client.operations() == ["get_item", "get_item"]
    assert [request["Key"] for request in client.requests("get_item")] == [
        item_key(*fence_key(ENV)), item_key(*semaphore_key(ENV)),
    ]
    assert all(request["ConsistentRead"] is True for request in client.requests("get_item"))


def test_acquire_e_uma_transacao_de_fence_e_semaforo(gate: _Gate) -> None:
    adapter, client = gate

    acquisition = adapter.acquire_unit_permit(_acquire())

    expected = UnitPermit(ENV, 1, HOLDER, None, NOW, NOW + timedelta(seconds=300), None)
    assert acquisition == PermitAcquisition(AcquisitionStatus.ACQUIRED, expected)
    (transaction,) = client.requests("transact_write_items")
    check, claim = (next(iter(action.values())) for action in transaction["TransactItems"])
    assert check["Key"] == item_key(*fence_key(ENV))
    assert check["ConditionExpression"] == "attribute_not_exists(pk) OR #state = :open"
    assert claim["Key"] == item_key(*semaphore_key(ENV))
    assert claim["ConditionExpression"] == "attribute_not_exists(#holder_dispatch)"
    assert claim["UpdateExpression"].endswith(" ADD #token :one")
    assert adapter.observe_environment_gate(ENV).permit == expected


def test_mesmo_dispatch_reentra_mesmo_com_fence_fechado(gate: _Gate) -> None:
    adapter, _ = gate
    first = _permit(adapter.acquire_unit_permit(_acquire()))

    assert adapter.close_promotion_fence(ClosePromotionFence(ENV, PROMOTION, NOW)) is True
    again = adapter.acquire_unit_permit(_acquire(now=LATER))
    other = adapter.acquire_unit_permit(_acquire(OTHER))

    assert again == PermitAcquisition(AcquisitionStatus.REENTERED, first)
    assert other == PermitAcquisition(AcquisitionStatus.FENCE_CLOSED, first)


def test_lease_vencido_nao_permite_takeover(gate: _Gate) -> None:
    adapter, _ = gate
    first = _permit(adapter.acquire_unit_permit(_acquire()))

    late = adapter.acquire_unit_permit(_acquire(OTHER, now=NOW + timedelta(days=3)))

    assert late == PermitAcquisition(AcquisitionStatus.HELD, first)
    assert adapter.observe_environment_gate(ENV).permit == first


def test_fence_fechado_so_admite_o_canary_vinculado(gate: _Gate) -> None:
    adapter, _ = gate
    adapter.close_promotion_fence(ClosePromotionFence(ENV, PROMOTION, NOW))

    blocked = adapter.acquire_unit_permit(_acquire())
    foreign = adapter.acquire_unit_permit(_acquire(canary=OTHER_PROMOTION))
    canary = adapter.acquire_unit_permit(_acquire(OTHER, canary=PROMOTION))

    assert blocked == PermitAcquisition(AcquisitionStatus.FENCE_CLOSED, None)
    assert foreign == PermitAcquisition(AcquisitionStatus.FENCE_CLOSED, None)
    permit = _permit(canary)
    assert (canary.status, permit.holder, permit.canary_promotion_id) == (
        AcquisitionStatus.ACQUIRED, OTHER, PROMOTION,
    )
    assert adapter.bind_unit_permit(BindUnitPermit(ENV, permit.token, "exec-canary", NOW)) is True
    assert adapter.observe_environment_gate(ENV).permit == replace(
        permit, execution_ref="exec-canary",
    )


def test_bind_renew_release_sao_condicionais_no_token(gate: _Gate) -> None:
    adapter, _ = gate
    permit = _permit(adapter.acquire_unit_permit(_acquire()))
    token = permit.token

    assert adapter.bind_unit_permit(BindUnitPermit(ENV, token, "exec-1", NOW)) is True
    assert adapter.bind_unit_permit(BindUnitPermit(ENV, token, "exec-1", LATER)) is True
    assert adapter.bind_unit_permit(BindUnitPermit(ENV, token, "exec-2", LATER)) is False
    assert adapter.bind_unit_permit(BindUnitPermit(ENV, token + 1, "exec-1", LATER)) is False
    assert adapter.renew_unit_permit(RenewUnitPermit(ENV, token + 1, LATER, 600)) is False
    assert adapter.renew_unit_permit(RenewUnitPermit(ENV, token, LATER, 600)) is True
    assert adapter.observe_environment_gate(ENV).permit == replace(
        permit, execution_ref="exec-1", lease_until=LATER + timedelta(seconds=600),
    )
    assert adapter.release_unit_permit(ReleaseUnitPermit(ENV, token + 1, LATER)) is False
    assert adapter.release_unit_permit(ReleaseUnitPermit(ENV, token, LATER)) is True
    assert adapter.release_unit_permit(ReleaseUnitPermit(ENV, token, LATER)) is False
    assert adapter.renew_unit_permit(RenewUnitPermit(ENV, token, LATER, 600)) is False
    assert adapter.observe_environment_gate(ENV).permit is None


def test_reanchor_e_condicional_no_token_e_no_permit_sem_bind(gate: _Gate) -> None:
    adapter, client = gate
    permit = _permit(adapter.acquire_unit_permit(_acquire()))
    token = permit.token

    assert adapter.reanchor_unit_permit(ReanchorUnitPermit(ENV, token + 1, LATER)) is False
    assert adapter.reanchor_unit_permit(ReanchorUnitPermit(ENV, token, LATER)) is True
    assert adapter.observe_environment_gate(ENV).permit == replace(permit, acquired_at=LATER)
    adapter.bind_unit_permit(BindUnitPermit(ENV, token, "exec-1", LATER))
    assert adapter.reanchor_unit_permit(ReanchorUnitPermit(ENV, token, LATER)) is False
    adapter.release_unit_permit(ReleaseUnitPermit(ENV, token, LATER))
    assert adapter.reanchor_unit_permit(ReanchorUnitPermit(ENV, token, LATER)) is False
    write = client.requests("update_item")[1]
    assert write["UpdateExpression"] == "SET #acquired = :now, #updated = :now"
    assert write["ConditionExpression"].endswith(" AND attribute_not_exists(#execution_ref)")
    assert {"put_item", "delete_item", "scan"}.isdisjoint(client.operations())


def test_release_por_ausencia_exige_o_anchor_e_o_permit_sem_bind(gate: _Gate) -> None:
    adapter, _ = gate
    token = _permit(adapter.acquire_unit_permit(_acquire())).token
    adapter.reanchor_unit_permit(ReanchorUnitPermit(ENV, token, LATER))

    assert adapter.release_unit_permit(ReleaseUnitPermit(ENV, token, LATER, NOW)) is False
    assert adapter.release_unit_permit(ReleaseUnitPermit(ENV, token, LATER, LATER)) is True
    bound = _permit(adapter.acquire_unit_permit(_acquire(OTHER)))
    adapter.bind_unit_permit(BindUnitPermit(ENV, bound.token, "exec-2", NOW))
    assert adapter.release_unit_permit(ReleaseUnitPermit(ENV, bound.token, LATER, NOW)) is False
    assert adapter.observe_environment_gate(ENV).permit == replace(bound, execution_ref="exec-2")


def test_release_preserva_item_e_token_monotonico_sem_ttl(gate: _Gate) -> None:
    adapter, client = gate
    adapter.acquire_unit_permit(_acquire(canary=PROMOTION))
    adapter.bind_unit_permit(BindUnitPermit(ENV, 1, "exec-1", NOW))

    assert adapter.release_unit_permit(ReleaseUnitPermit(ENV, 1, LATER)) is True
    assert set(_raw(client, semaphore_key(ENV))) == {"pk", "sk", "entity", "token", "updated_at"}
    successor = _permit(adapter.acquire_unit_permit(_acquire(OTHER)))
    assert (successor.token, successor.canary_promotion_id) == (2, None)
    assert adapter.bind_unit_permit(BindUnitPermit(ENV, 1, "exec-1", LATER)) is False
    assert adapter.release_unit_permit(ReleaseUnitPermit(ENV, 1, LATER)) is False
    assert "delete_item" not in client.operations()


def test_close_e_reopen_usam_cas_de_versao_no_fence(gate: _Gate) -> None:
    adapter, client = gate

    assert adapter.close_promotion_fence(ClosePromotionFence(ENV, PROMOTION, NOW)) is True
    assert client.requests("update_item")[-1]["ConditionExpression"] == "attribute_not_exists(pk)"
    assert adapter.close_promotion_fence(ClosePromotionFence(ENV, OTHER_PROMOTION, LATER)) is False
    assert adapter.reopen_promotion_fence(
        ReopenPromotionFence(ENV, OTHER_PROMOTION, LATER),
    ) is False
    assert adapter.reopen_promotion_fence(ReopenPromotionFence(ENV, PROMOTION, LATER)) is True
    last_write = client.requests("update_item")[-1]
    assert last_write["ConditionExpression"] == "#version = :expected"
    assert last_write["ExpressionAttributeValues"][":expected"] == {"N": "1"}
    assert adapter.observe_environment_gate(ENV).fence == PromotionFence(
        ENV, FenceState.OPEN, PROMOTION, 2, LATER,
    )
    assert set(_raw(client, fence_key(ENV))) == {
        "pk", "sk", "entity", "state", "promotion_id", "version", "changed_at",
    }
    assert len(client.requests("update_item")) == 2


def test_replay_de_close_e_reopen_nao_regrava_o_fence(gate: _Gate) -> None:
    adapter, client = gate
    close = ClosePromotionFence(ENV, PROMOTION, NOW)
    reopen = ReopenPromotionFence(ENV, PROMOTION, LATER)

    assert adapter.close_promotion_fence(close) is True
    assert adapter.close_promotion_fence(close) is True
    assert adapter.reopen_promotion_fence(reopen) is True
    assert adapter.reopen_promotion_fence(reopen) is True
    assert len(client.requests("update_item")) == 2


def test_close_atrasado_apos_reopen_da_mesma_promocao_nao_refecha(gate: _Gate) -> None:
    adapter, client = gate
    close = ClosePromotionFence(ENV, PROMOTION, NOW)

    assert adapter.close_promotion_fence(close) is True
    assert adapter.reopen_promotion_fence(ReopenPromotionFence(ENV, PROMOTION, LATER)) is True
    assert adapter.close_promotion_fence(close) is False
    assert adapter.observe_environment_gate(ENV).fence == PromotionFence(
        ENV, FenceState.OPEN, PROMOTION, 2, LATER,
    )
    assert len(client.requests("update_item")) == 2


def test_reopen_de_fence_nunca_fechado_e_rejeitado_sem_escrita(gate: _Gate) -> None:
    adapter, client = gate

    assert adapter.reopen_promotion_fence(ReopenPromotionFence(ENV, PROMOTION, NOW)) is False
    assert "update_item" not in client.operations()


@pytest.mark.parametrize(("rival", "expected"), [(PROMOTION, True), (OTHER_PROMOTION, False)])
def test_cas_perdido_rele_e_reavalia_o_fence(gate: _Gate, rival: str, expected: bool) -> None:
    adapter, client = gate
    competitor = _rival(client)
    client.inject(
        "update_item",
        lambda: competitor.close_promotion_fence(ClosePromotionFence(ENV, rival, NOW)),
    )

    assert adapter.close_promotion_fence(ClosePromotionFence(ENV, PROMOTION, NOW)) is expected
    assert adapter.observe_environment_gate(ENV).fence.promotion_id == rival
    assert len(client.requests("update_item")) == 1


def test_conflito_transacional_no_update_e_retentado(gate: _Gate) -> None:
    adapter, client = gate
    adapter.acquire_unit_permit(_acquire())
    client.inject("update_item", _raise("TransactionConflictException"))

    assert adapter.bind_unit_permit(BindUnitPermit(ENV, 1, "exec-1", NOW)) is True
    assert len(client.requests("update_item")) == 2


@pytest.mark.parametrize(
    "change",
    [
        lambda adapter: adapter.release_unit_permit(ReleaseUnitPermit(ENV, 1, NOW)),
        lambda adapter: adapter.close_promotion_fence(ClosePromotionFence(ENV, PROMOTION, NOW)),
    ],
)
def test_conflito_transacional_persistente_esgota_tentativas(
    gate: _Gate, change: Callable[[DynamoDBControlPlane], bool],
) -> None:
    adapter, client = gate
    adapter.acquire_unit_permit(_acquire())
    client.inject("update_item", *(_raise("TransactionConflictException") for _ in range(3)))

    with pytest.raises(Conflict) as error:
        change(adapter)

    assert error.value.code is ErrorCode.TRANSACTION_CONFLICT
    assert adapter.observe_environment_gate(ENV).permit is not None


def test_erro_inesperado_no_update_propaga(gate: _Gate) -> None:
    adapter, client = gate
    client.inject("update_item", _raise("ProvisionedThroughputExceededException"))

    with pytest.raises(ClientError, match="ProvisionedThroughputExceededException"):
        adapter.renew_unit_permit(RenewUnitPermit(ENV, 1, NOW, 300))


@pytest.mark.parametrize(
    "reasons", [("None", "TransactionConflict"), ("ConditionalCheckFailed", "None")],
)
def test_acquire_cancelado_com_semaforo_livre_rele_e_retenta(
    gate: _Gate, reasons: tuple[str, ...],
) -> None:
    adapter, client = gate
    client.inject("transact_write_items", _raise("TransactionCanceledException", *reasons))

    acquisition = adapter.acquire_unit_permit(_acquire())

    assert (acquisition.status, _permit(acquisition).token) == (AcquisitionStatus.ACQUIRED, 1)
    assert len(client.requests("transact_write_items")) == 2


def test_acquire_com_conflito_persistente_esgota_tentativas(gate: _Gate) -> None:
    adapter, client = gate
    conflict = _raise("TransactionCanceledException", "TransactionConflict", "None")
    client.inject("transact_write_items", conflict, conflict, conflict)

    with pytest.raises(Conflict) as error:
        adapter.acquire_unit_permit(_acquire())

    assert error.value.code is ErrorCode.TRANSACTION_CONFLICT
    assert adapter.observe_environment_gate(ENV).permit is None


@pytest.mark.parametrize(
    "error",
    [_raise("InternalServerError"), _raise("TransactionCanceledException", "ThrottlingError")],
)
def test_acquire_com_erro_nao_conflitante_propaga(
    gate: _Gate, error: Callable[[], object],
) -> None:
    adapter, client = gate
    client.inject("transact_write_items", error)

    with pytest.raises(ClientError):
        adapter.acquire_unit_permit(_acquire())

    assert client.operations() == ["transact_write_items"]


def test_acquire_commitado_e_ja_tomado_reporta_o_detentor_atual(gate: _Gate) -> None:
    adapter, client = gate
    competitor = _rival(client)

    def takeover() -> None:
        competitor.release_unit_permit(ReleaseUnitPermit(ENV, 1, NOW))
        competitor.acquire_unit_permit(_acquire(OTHER))

    client.inject("get_item", takeover)

    acquisition = adapter.acquire_unit_permit(_acquire())

    assert acquisition.status is AcquisitionStatus.HELD
    assert (_permit(acquisition).holder, _permit(acquisition).token) == (OTHER, 2)


def test_gate_usa_so_leitura_forte_e_escrita_condicional(gate: _Gate) -> None:
    adapter, client = gate
    permit = _permit(adapter.acquire_unit_permit(_acquire()))
    adapter.bind_unit_permit(BindUnitPermit(ENV, permit.token, "exec-1", NOW))
    adapter.renew_unit_permit(RenewUnitPermit(ENV, permit.token, LATER, 300))
    adapter.close_promotion_fence(ClosePromotionFence(ENV, PROMOTION, NOW))
    adapter.acquire_unit_permit(_acquire(OTHER))
    adapter.release_unit_permit(ReleaseUnitPermit(ENV, permit.token, LATER))
    adapter.reopen_promotion_fence(ReopenPromotionFence(ENV, PROMOTION, LATER))

    assert set(client.operations()) == _GATE_OPERATIONS
    assert all(request["ConsistentRead"] is True for request in client.requests("get_item"))
    assert all("ConditionExpression" in request for request in client.requests("update_item"))
