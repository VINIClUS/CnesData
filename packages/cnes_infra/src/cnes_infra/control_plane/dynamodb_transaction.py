"""Transações DynamoDB de chaves únicas, sem dependência de pydantic."""

from collections.abc import Iterable
from typing import Any, cast

from botocore.exceptions import ClientError

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode

type Item = dict[str, dict[str, Any]]
type Action = dict[str, dict[str, Any]]


def _action_key(action: Action) -> tuple[str, str]:
    request = next(iter(action.values()))
    values = cast("Item", request.get("Item", request.get("Key")))
    return str(values["pk"]["S"]), str(values["sk"]["S"])


def _validate_actions(actions: tuple[Action, ...]) -> None:
    if len(actions) > 100:
        raise Conflict(ErrorCode.TRANSACTION_LIMIT)
    keys = tuple(_action_key(action) for action in actions)
    if len(set(keys)) != len(keys):
        raise Conflict(ErrorCode.DUPLICATE_TRANSACTION_KEY)


def execute_transaction(
    client: Any, actions: Iterable[Action], client_request_token: str | None = None
) -> None:
    """Valida e envia uma transação de chaves únicas."""
    normalized = tuple(actions)
    _validate_actions(normalized)
    request: dict[str, Any] = {"TransactItems": list(normalized)}
    if client_request_token is not None:
        request["ClientRequestToken"] = client_request_token
    try:
        client.transact_write_items(**request)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        reasons = error.response.get("CancellationReasons") or ()
        reason_codes = {reason.get("Code") for reason in reasons} - {None, "None"}
        conditional = code == "TransactionCanceledException" and reason_codes == {
            "ConditionalCheckFailed"
        }
        if conditional:
            raise Conflict(ErrorCode.TRANSACTION_CONFLICT) from error
        raise
