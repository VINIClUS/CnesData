"""Contador mensal de tentativas de StartExecution do ambiente no DynamoDB do control plane."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, cast

from botocore.exceptions import ClientError

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.execution_quota import (
    AttemptOutcome,
    AttemptStatus,
    decide_attempt,
)
from cnes_infra.control_plane.dynamodb_environment_gate import attempts_key
from cnes_infra.control_plane.dynamodb_host import DynamoDBHost
from cnes_infra.control_plane.dynamodb_keys import item_key

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cnes_domain.control_plane.execution_quota import ConsumeExecutionAttempt
    from cnes_infra.control_plane.dynamodb_codec import Item

_ENTITY: Final = "EXECUTIONATTEMPTS"
_ATTEMPTS: Final = "attempts"
_DISPATCH_IDS: Final = "dispatch_ids"
_WRITES: Final = 3
# Only garbage collection: no decision ever reads expires_at.
_RETENTION: Final = timedelta(days=400)
_UPDATE: Final = "SET #entity = :entity, #expires = :expires ADD #count :one, #ids :idset"
_CONDITION: Final = (
    "(attribute_not_exists(#count) OR #count < :limit) "
    "AND (attribute_not_exists(#ids) OR NOT contains(#ids, :id))"
)
_NAMES: Final = {
    "#entity": "entity", "#expires": "expires_at", "#count": _ATTEMPTS, "#ids": _DISPATCH_IDS,
}
_EMPTY_MONTH: Final[Item] = {_ATTEMPTS: {"N": "0"}, _DISPATCH_IDS: {"SS": []}}


def _expires_at(command: ConsumeExecutionAttempt) -> int:
    start = command.now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return int((start + _RETENTION).timestamp())


def _attempt_update(command: ConsumeExecutionAttempt) -> dict[str, Any]:
    return {
        "UpdateExpression": _UPDATE,
        "ConditionExpression": _CONDITION,
        "ExpressionAttributeNames": _NAMES,
        "ExpressionAttributeValues": {
            ":entity": {"S": _ENTITY},
            ":expires": {"N": str(_expires_at(command))},
            ":one": {"N": "1"},
            ":idset": {"SS": [command.dispatch_id]},
            ":id": {"S": command.dispatch_id},
            ":limit": {"N": str(command.limit)},
        },
        # Both outcomes return the item the atomic write saw: the decision reads only that.
        "ReturnValues": "ALL_OLD",
        "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
    }


def _ledger(item: Item) -> tuple[int, frozenset[str]]:
    month = {**_EMPTY_MONTH, **item}
    return int(month[_ATTEMPTS]["N"]), frozenset(month[_DISPATCH_IDS]["SS"])


def _rejected(item: Item, command: ConsumeExecutionAttempt) -> AttemptOutcome:
    # A rejected write never counts, even if its old item does not explain the rejection.
    attempts, counted = _ledger(item)
    replay = command.dispatch_id in counted
    status = AttemptStatus.ALREADY_COUNTED if replay else AttemptStatus.EXHAUSTED
    return AttemptOutcome(status, attempts, command.limit)


class DynamoDBExecutionQuota(DynamoDBHost):
    """Tentativas do mês por ambiente num item próprio: um ADD condicional, sem ler antes."""

    def consume_execution_attempt(self, command: ConsumeExecutionAttempt) -> AttemptOutcome:
        """Conta a tentativa do dispatch uma vez no mês UTC com um UpdateItem condicional.

        Returns: Escrita aplicada: `decide_attempt` sobre o item anterior. Rejeitada:
            ALREADY_COUNTED com o dispatch no conjunto, senão EXHAUSTED; nunca COUNTED.
        Raises: Conflict: Conflito transacional persistente.
            ClientError: Outro erro do DynamoDB, inclusive rejeição sem o item antigo.
        """
        key = item_key(*attempts_key(command.environment, command.period))
        for _ in range(_WRITES):
            outcome = self._attempt_write(key, command)
            if outcome is not None:
                return outcome
        raise Conflict(ErrorCode.TRANSACTION_CONFLICT)

    def _attempt_write(
        self, key: dict[str, dict[str, str]], command: ConsumeExecutionAttempt,
    ) -> AttemptOutcome | None:
        try:
            response = self._client.update_item(
                TableName=self._table_name, Key=key, **_attempt_update(command),
            )
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code == "TransactionConflictException":
                return None
            rejected = cast("Mapping[str, Any]", error.response).get("Item")
            if code != "ConditionalCheckFailedException" or rejected is None:
                raise
            return _rejected(rejected, command)
        return decide_attempt(*_ledger(response.get("Attributes", {})), command)
