"""Fence de promoção e semáforo de unidade do ambiente no DynamoDB do control plane."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final, cast

from botocore.exceptions import ClientError

from cnes_domain.control_plane.environment_gate import (
    AcquisitionStatus,
    EnvironmentGateState,
    FenceState,
    PermitAcquisition,
    PermitHolder,
    PromotionFence,
    UnitPermit,
    close_fence,
    decide_acquisition,
    open_fence,
    reopen_fence,
    require_environment,
)
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.dynamodb_host import DynamoDBHost
from cnes_infra.control_plane.dynamodb_keys import item_key, timestamp

if TYPE_CHECKING:
    from collections.abc import Callable

    from cnes_domain.control_plane.environment_gate import (
        AcquireUnitPermit,
        BindUnitPermit,
        ClosePromotionFence,
        ReanchorUnitPermit,
        ReleaseUnitPermit,
        RenewUnitPermit,
        ReopenPromotionFence,
    )
    from cnes_infra.control_plane.dynamodb_codec import Action, Item

OUTBOX_CURSOR_KEY: Final = ("OUTBOX#CURSOR", "CURSOR")
_PERIOD = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_ATTEMPTS = 3
_FENCE_ENTITY = "PROMOTIONFENCE"
_SEMAPHORE_ENTITY = "UNITSEMAPHORE"
_HOLDER_DISPATCH = "holder_dispatch_id"
_HELD_CONDITION = "#token = :token AND attribute_exists(#holder_dispatch)"
_UNBOUND_CONDITION = "attribute_not_exists(#execution_ref)"
_NAMES: Final = {
    "#entity": "entity",
    "#token": "token",
    "#updated": "updated_at",
    "#holder_tenant": "holder_tenant_id",
    "#holder_run": "holder_run_id",
    "#holder_dispatch": _HOLDER_DISPATCH,
    "#acquired": "acquired_at",
    "#lease": "lease_until",
    "#execution_ref": "execution_ref",
    "#canary": "canary_promotion_id",
    "#state": "state",
    "#promotion": "promotion_id",
    "#version": "version",
    "#changed": "changed_at",
}


def fence_key(environment: str) -> tuple[str, str]:
    """Chave do fence de promoção, em partição própria do ambiente.

    Raises: ValueError: Ambiente fora de `^[a-z][a-z0-9-]{0,31}$`.
    """
    return f"ENV#{require_environment(environment)}#FENCE", "FENCE"


def semaphore_key(environment: str) -> tuple[str, str]:
    """Chave do semáforo de unidade, em partição própria do ambiente.

    Raises: ValueError: Ambiente fora de `^[a-z][a-z0-9-]{0,31}$`.
    """
    return f"ENV#{require_environment(environment)}#SEMAPHORE", "UNIT"


def attempts_key(environment: str, period: str) -> tuple[str, str]:
    """Chave do contador mensal de tentativas de start do ambiente.

    Args: environment: Nome do ambiente; period: Mês UTC `YYYY-MM`.
    Raises: ValueError: Ambiente ou período inválido.
    """
    partition = f"ENV#{require_environment(environment)}#ATTEMPTS"
    if not _PERIOD.fullmatch(period):
        raise ValueError("reason=invalid_period field=period")
    return partition, f"MONTH#{period}"


@dataclass(frozen=True, slots=True)
class _Update:
    expression: str
    condition: str
    values: dict[str, dict[str, str]]

    def request(self) -> dict[str, Any]:
        return {
            "UpdateExpression": self.expression,
            "ConditionExpression": self.condition,
            "ExpressionAttributeNames": _names(self.expression, self.condition),
            "ExpressionAttributeValues": self.values,
        }


def _names(*expressions: str) -> dict[str, str]:
    used = " ".join(expressions)
    return {placeholder: name for placeholder, name in _NAMES.items() if placeholder in used}


def _text(value: str) -> dict[str, str]:
    return {"S": value}


def _number(value: int) -> dict[str, str]:
    return {"N": str(value)}


def _optional_text(item: Item, name: str) -> str | None:
    return item[name]["S"] if name in item else None


def _decode_fence(item: Item | None, environment: str) -> PromotionFence:
    if item is None:
        return open_fence(environment)
    return PromotionFence(
        environment=environment,
        state=FenceState(item["state"]["S"]),
        promotion_id=_optional_text(item, "promotion_id"),
        version=int(item["version"]["N"]),
        changed_at=datetime.fromisoformat(item["changed_at"]["S"]),
    )


def _decode_permit(item: Item | None, environment: str) -> UnitPermit | None:
    if item is None or _HOLDER_DISPATCH not in item:
        return None
    holder = PermitHolder(
        item["holder_tenant_id"]["S"], item["holder_run_id"]["S"], item[_HOLDER_DISPATCH]["S"],
    )
    return UnitPermit(
        environment=environment,
        token=int(item["token"]["N"]),
        holder=holder,
        execution_ref=_optional_text(item, "execution_ref"),
        acquired_at=datetime.fromisoformat(item["acquired_at"]["S"]),
        lease_until=datetime.fromisoformat(item["lease_until"]["S"]),
        canary_promotion_id=_optional_text(item, "canary_promotion_id"),
    )


def _fence_check(table_name: str, command: AcquireUnitPermit) -> Action:
    condition = "attribute_not_exists(pk) OR #state = :open"
    values = {":open": _text(FenceState.OPEN.value)}
    if command.canary_promotion_id is not None:
        condition += " OR #promotion = :canary"
        values[":canary"] = _text(command.canary_promotion_id)
    return {"ConditionCheck": {
        "TableName": table_name,
        "Key": item_key(*fence_key(command.environment)),
        "ConditionExpression": condition,
        "ExpressionAttributeNames": _names(condition),
        "ExpressionAttributeValues": values,
    }}


def _claim(table_name: str, command: AcquireUnitPermit) -> Action:
    holder = command.holder
    sets = [
        "#entity = :entity", "#holder_tenant = :tenant", "#holder_run = :run",
        "#holder_dispatch = :dispatch", "#acquired = :now", "#lease = :lease", "#updated = :now",
    ]
    values = {
        ":entity": _text(_SEMAPHORE_ENTITY), ":tenant": _text(holder.tenant_id),
        ":run": _text(holder.run_id), ":dispatch": _text(holder.dispatch_id),
        ":now": _text(timestamp(command.now)), ":lease": _text(timestamp(command.lease_until)),
        ":one": _number(1),
    }
    if command.canary_promotion_id is not None:
        sets.append("#canary = :canary")
        values[":canary"] = _text(command.canary_promotion_id)
    update = _Update(
        f"SET {', '.join(sets)} ADD #token :one", "attribute_not_exists(#holder_dispatch)", values,
    )
    key = item_key(*semaphore_key(command.environment))
    return {"Update": {"TableName": table_name, "Key": key, **update.request()}}


def _bind(command: BindUnitPermit) -> _Update:
    bound = "(attribute_not_exists(#execution_ref) OR #execution_ref = :ref)"
    return _Update(
        "SET #execution_ref = :ref, #updated = :now", f"{_HELD_CONDITION} AND {bound}",
        {
            ":token": _number(command.token), ":ref": _text(command.execution_ref),
            ":now": _text(timestamp(command.now)),
        },
    )


def _renew(command: RenewUnitPermit) -> _Update:
    return _Update(
        "SET #lease = :lease, #updated = :now", _HELD_CONDITION,
        {
            ":token": _number(command.token), ":lease": _text(timestamp(command.lease_until)),
            ":now": _text(timestamp(command.now)),
        },
    )


def _reanchor(command: ReanchorUnitPermit) -> _Update:
    return _Update(
        "SET #acquired = :now, #updated = :now", f"{_HELD_CONDITION} AND {_UNBOUND_CONDITION}",
        {":token": _number(command.token), ":now": _text(timestamp(command.now))},
    )


def _release(command: ReleaseUnitPermit) -> _Update:
    holder = "#holder_tenant, #holder_run, #holder_dispatch, #acquired, #lease"
    condition = _HELD_CONDITION
    values = {":token": _number(command.token), ":now": _text(timestamp(command.now))}
    if command.anchor is not None:
        condition += f" AND #acquired = :anchor AND {_UNBOUND_CONDITION}"
        values[":anchor"] = _text(timestamp(command.anchor))
    return _Update(
        f"REMOVE {holder}, #execution_ref, #canary SET #updated = :now", condition, values,
    )


def _fence_write(current: PromotionFence, updated: PromotionFence) -> _Update:
    values = {
        ":entity": _text(_FENCE_ENTITY),
        ":state": _text(updated.state.value),
        ":promotion": _text(cast("str", updated.promotion_id)),
        ":version": _number(updated.version),
        ":changed": _text(timestamp(cast("datetime", updated.changed_at))),
    }
    condition = "attribute_not_exists(pk)"
    if current.version:
        condition = "#version = :expected"
        values[":expected"] = _number(current.version)
    expression = (
        "SET #entity = :entity, #state = :state, #promotion = :promotion, "
        "#version = :version, #changed = :changed"
    )
    return _Update(expression, condition, values)


def _acquisition(
    state: EnvironmentGateState, command: AcquireUnitPermit, committed: bool,
) -> PermitAcquisition | None:
    permit = state.permit
    if committed and permit is not None and permit.holder == command.holder:
        return PermitAcquisition(AcquisitionStatus.ACQUIRED, permit)
    status = decide_acquisition(state, command)
    if status is AcquisitionStatus.ACQUIRED:
        return None
    return PermitAcquisition(status, permit)


def _transaction_conflict(error: ClientError) -> bool:
    reasons = error.response.get("CancellationReasons") or ()
    return any(reason.get("Code") == "TransactionConflict" for reason in reasons)


class DynamoDBEnvironmentGate(DynamoDBHost):
    """Fence e semáforo do ambiente em itens próprios: sem Scan, sem TTL, nunca apagados."""

    def observe_environment_gate(self, environment: str) -> EnvironmentGateState:
        """Lê fence e semáforo com leitura fortemente consistente; fence ausente é aberto.

        Raises: ValueError: Ambiente inválido.
        """
        fence = _decode_fence(self._get_item(fence_key(environment)), environment)
        permit = _decode_permit(self._get_item(semaphore_key(environment)), environment)
        return EnvironmentGateState(environment, fence, permit)

    def acquire_unit_permit(self, command: AcquireUnitPermit) -> PermitAcquisition:
        """Adquire o semáforo numa transação que exige o fence admissível.

        Returns: ACQUIRED/REENTERED com o permit do dispatch; FENCE_CLOSED/HELD com o atual.
        Raises: Conflict: Conflito transacional persistente.
        """
        for _ in range(_ATTEMPTS):
            committed = self._gate_claim(command)
            state = self.observe_environment_gate(command.environment)
            outcome = _acquisition(state, command, committed)
            if outcome is not None:
                return outcome
        raise Conflict(ErrorCode.TRANSACTION_CONFLICT)

    def bind_unit_permit(self, command: BindUnitPermit) -> bool:
        """Vincula a execução ao permit do token; False com token ou vínculo divergente."""
        return self._gate_semaphore(command.environment, _bind(command))

    def renew_unit_permit(self, command: RenewUnitPermit) -> bool:
        """Renova o lease do permit do token; False com token divergente."""
        return self._gate_semaphore(command.environment, _renew(command))

    def release_unit_permit(self, command: ReleaseUnitPermit) -> bool:
        """Remove o detentor sem apagar o item; False com token divergente ou já liberado.

        Com anchor, False também se o permit foi vinculado ou reancorado depois da prova.
        """
        return self._gate_semaphore(command.environment, _release(command))

    def reanchor_unit_permit(self, command: ReanchorUnitPermit) -> bool:
        """Reancora a carência do permit do token; False com token divergente ou bind."""
        return self._gate_semaphore(command.environment, _reanchor(command))

    def close_promotion_fence(self, command: ClosePromotionFence) -> bool:
        """Fecha o fence por CAS de versão.

        Returns: False se outra promoção o mantém fechado ou se a promoção já o reabriu.
        """
        return self._gate_fence(command.environment, lambda fence: close_fence(fence, command))

    def reopen_promotion_fence(self, command: ReopenPromotionFence) -> bool:
        """Reabre o fence por CAS de versão; False sem a promoção correspondente."""
        return self._gate_fence(command.environment, lambda fence: reopen_fence(fence, command))

    def _gate_claim(self, command: AcquireUnitPermit) -> bool:
        actions = (_fence_check(self._table_name, command), _claim(self._table_name, command))
        try:
            self._transact(actions)
        except Conflict:
            return False
        except ClientError as error:
            if not _transaction_conflict(error):
                raise
            return False
        return True

    def _gate_semaphore(self, environment: str, update: _Update) -> bool:
        key = semaphore_key(environment)
        for _ in range(_ATTEMPTS):
            applied = self._gate_write(key, update)
            if applied is not None:
                return applied
        raise Conflict(ErrorCode.TRANSACTION_CONFLICT)

    def _gate_fence(
        self, environment: str, change: Callable[[PromotionFence], PromotionFence | None],
    ) -> bool:
        key = fence_key(environment)
        for _ in range(_ATTEMPTS):
            current = _decode_fence(self._get_item(key), environment)
            updated = change(current)
            if updated is None:
                return False
            if updated == current or self._gate_write(key, _fence_write(current, updated)):
                return True
        raise Conflict(ErrorCode.TRANSACTION_CONFLICT)

    def _gate_write(self, key: tuple[str, str], update: _Update) -> bool | None:
        try:
            self._client.update_item(
                TableName=self._table_name, Key=item_key(*key), **update.request(),
            )
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code == "TransactionConflictException":
                return None
            if code != "ConditionalCheckFailedException":
                raise
            return False
        return True
