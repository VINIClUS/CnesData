"""Handler ABI2 close-drain: fecha e drena o fence prod pelo shim de context.call (#473)."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from cnes_infra.control_plane.dynamodb_environment_gate import DynamoDBEnvironmentGate
from cnes_infra.control_plane.dynamodb_keys import item_key
from cnes_infra.control_plane.dynamodb_transaction import execute_transaction
from cnes_infra.executor.step_functions_probe import StepFunctionsExecutionProbe
from release.verify_activation import FIELDS
from scripts.promotion._api_static_records import canonical
from scripts.promotion.environment_gate import close_and_drain

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from cnes_domain.control_plane.environment_gate import EnvironmentGateState
    from cnes_infra.control_plane.dynamodb_codec import Action, Item
    from scripts.promotion.environment_gate import DrainClock
    from scripts.promotion.promote_api import AdapterContext

ROLE = "fence_operator"
ENVIRONMENT = "prod"
# infra/opentofu/env/prod/main.tf:4 and modules/processing/step_functions.tf:13 (name_prefix).
TABLE = "cnesdata-prod-control-plane"
MACHINE = "cnesdata-prod"
EXPIRATION_KINDS = frozenset({"artifact", "platform", "cost", "credential", "activation", "plan"})
DRAIN_CLOCK: DrainClock | None = None

_COMMON = ("repository", "release_id", "source_sha", "candidate_sha256", "canary_id")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_INSTANT = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ")
_TASK = re.compile(r"arn:aws:ecs:us-east-2:(\d{12}):task-definition/\S+")
_DISPATCH = re.compile(r"[0-9a-f]{16}")
_FENCE = item_key(f"ENV#{ENVIRONMENT}#FENCE", "FENCE")
_SEMAPHORE = item_key(f"ENV#{ENVIRONMENT}#SEMAPHORE", "UNIT")
_OPERATIONS = frozenset({
    ("dynamodb", "get_item"), ("dynamodb", "update_item"),
    ("dynamodb", "transact_write_items"), ("stepfunctions", "describe_execution"),
})
_WRITES = frozenset({"update_item", "transact_write_items"})
_UPDATE_FIELDS = frozenset({"TableName", "Key", "UpdateExpression", "ConditionExpression",
                            "ExpressionAttributeNames", "ExpressionAttributeValues"})
_CONFIRM = "SET #confirmed = :confirmed"


def close_drain(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                context: AdapterContext) -> bytes:
    """Returns:
    Documento close-drain canônico, com o fence fechado e drenado pelo shim do contexto.
    Raises:
    ValueError: activation, expirations ou prazo de preparação fora do contrato.
    DrainError: common, fence ou drenagem divergente do gate.
    """
    activation, account = _activation(inputs["activation"], request)
    expirations = _expirations(inputs["expirations"])
    identity = owner_identity(request)
    now = _now().replace(microsecond=0)
    if now.timestamp() >= request["stage"]["preparation_deadline"]:
        raise ValueError("close_drain=preparation_deadline")
    machine = f"arn:aws:states:us-east-2:{account}:stateMachine:{MACHINE}"
    shim = FenceShim(context, machine)
    common = {**{key: activation[key] for key in _COMMON},
              "activation_sha256": request["activation_sha256"], **identity}
    probe = StepFunctionsExecutionProbe(cast("Any", shim.client("stepfunctions")), machine)
    document = close_and_drain(_Gate(shim), probe, None, ENVIRONMENT, identity["promotion_id"],
                               now, expirations, clock=DRAIN_CLOCK, common=common)
    return canonical(document)


def owner_identity(request: Mapping[str, Any]) -> dict[str, Any]:
    """Returns: caller, promotion_generation e promotion_id do owner autenticado do Keeper."""
    from scripts.promotion.promote_api import _caller  # promote_api registers this handler

    owner = request["owner"]
    return {"caller": _caller(request), "promotion_generation": owner["generation"],
            "promotion_id": owner["owner_sha256"]}


class FenceShim:
    """Restringe context.call ao vetor do close-drain; escrita só até o confirm do close."""

    def __init__(self, context: AdapterContext, machine_arn: str) -> None:
        self._context = context
        self._execution = machine_arn.replace(":stateMachine:", ":execution:") + ":"
        self._sealed = False

    def client(self, service: str) -> Any:
        return _Client(self, service)

    def seal(self) -> None:
        self._sealed = True

    def invoke(self, service: str, operation: str, parameters: dict[str, Any]) -> Any:
        """Returns: resposta do context.call. Raises: ValueError: chamada fora do vetor."""
        if (service, operation) not in _OPERATIONS:
            raise ValueError(f"shim=operation service={service} name={operation}")
        if self._sealed and operation in _WRITES:
            raise ValueError(f"shim=sealed name={operation}")
        if not self._allowed(operation, parameters):
            raise ValueError(f"shim=parameters name={operation}")
        self._context.guard()
        return self._context.call(ROLE, service, operation, parameters)

    def _allowed(self, operation: str, parameters: Mapping[str, Any]) -> bool:
        if operation == "get_item":
            return any(parameters == {"TableName": TABLE, "Key": key, "ConsistentRead": True}
                       for key in (_FENCE, _SEMAPHORE))
        if operation == "update_item":
            return (set(parameters) == _UPDATE_FIELDS and parameters["TableName"] == TABLE
                    and parameters["Key"] == _FENCE and parameters["UpdateExpression"] == _CONFIRM)
        if operation == "transact_write_items":
            return _close_transaction(parameters)
        arn = parameters.get("executionArn")
        return (set(parameters) == {"executionArn", "includedData"}
                and parameters["includedData"] == "METADATA_ONLY" and isinstance(arn, str)
                and arn.startswith(self._execution)
                and _DISPATCH.fullmatch(arn[len(self._execution):]) is not None)


class _Client:
    def __init__(self, shim: FenceShim, service: str) -> None:
        self._shim, self._service = shim, service

    def __getattr__(self, name: str) -> Callable[..., Any]:
        def call(**parameters: Any) -> Any:
            if (self._service, name) == ("stepfunctions", "describe_execution"):
                parameters = parameters | {"includedData": "METADATA_ONLY"}
            return self._shim.invoke(self._service, name, parameters)

        return call


class _Gate(DynamoDBEnvironmentGate):
    def __init__(self, shim: FenceShim) -> None:
        self._shim = shim
        self._client = shim.client("dynamodb")
        self._table_name = TABLE

    def _get_item(self, key: tuple[str, str]) -> Item | None:
        response = self._client.get_item(
            TableName=self._table_name, Key=item_key(*key), ConsistentRead=True
        )
        return response.get("Item")

    def _transact(self, actions: tuple[Action, ...],
                  client_request_token: str | None = None) -> None:
        execute_transaction(self._client, actions, client_request_token)

    def confirm_promotion_close(
        self, environment: str, promotion_id: str, observed_at: datetime
    ) -> EnvironmentGateState:
        state = super().confirm_promotion_close(environment, promotion_id, observed_at)
        self._shim.seal()
        return state


def _close_transaction(parameters: Mapping[str, Any]) -> bool:
    items = parameters.get("TransactItems") if set(parameters) == {"TransactItems"} else None
    if not isinstance(items, list) or len(items) != 2:
        return False
    if not all(isinstance(item, dict) and len(item) == 1 for item in items):
        return False
    update, check = items[0].get("Update"), items[1].get("ConditionCheck")
    return (isinstance(update, dict) and isinstance(check, dict)
            and (update.get("TableName"), update.get("Key")) == (TABLE, _FENCE)
            and (check.get("TableName"), check.get("Key")) == (TABLE, _SEMAPHORE))


def _activation(raw: bytes, request: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    if hashlib.sha256(raw).hexdigest() != request["activation_sha256"]:
        raise ValueError("activation=sha256")
    try:
        value = json.loads(raw)
    except ValueError as error:
        raise ValueError("activation=encoding") from error
    if (not isinstance(value, dict) or set(value) - {"inputs"} != FIELDS
            or not all(isinstance(value[key], str) for key in _COMMON)):
        raise ValueError("activation=fields")
    return value, _account(value["revisions"])


def _account(revisions: Any) -> str:
    arns = list(revisions.values()) if isinstance(revisions, dict) else []
    matches = [_TASK.fullmatch(arn) if isinstance(arn, str) else None for arn in arns]
    accounts = {match[1] for match in matches if match is not None}
    if not arns or None in matches or len(accounts) != 1:
        raise ValueError("activation=account")
    return accounts.pop()


def _expirations(raw: bytes) -> list[dict[str, Any]]:
    try:
        value = json.loads(raw)
    except ValueError as error:
        raise ValueError("expirations=encoding") from error
    if not isinstance(value, list) or not value:
        raise ValueError("expirations=list")
    if canonical(value) != raw:
        raise ValueError("expirations=canonical")
    for item in value:
        _expiry(item)
    keys = [(item["kind"], item["object_sha256"]) for item in value]
    if keys != sorted(set(keys)):
        raise ValueError("expirations=order")
    return value


def _expiry(item: Any) -> None:
    if not isinstance(item, dict) or set(item) != {"kind", "object_sha256", "expires_at"}:
        raise ValueError("expirations=item")
    if not isinstance(item["kind"], str) or item["kind"] not in EXPIRATION_KINDS:
        raise ValueError("expirations=kind")
    if not isinstance(item["object_sha256"], str) or not _DIGEST.fullmatch(item["object_sha256"]):
        raise ValueError("expirations=object_sha256")
    instant = item["expires_at"]
    if not isinstance(instant, str) or not _INSTANT.fullmatch(instant):
        raise ValueError("expirations=expires_at")
    try:
        datetime.strptime(instant, "%Y-%m-%dT%H:%M:%SZ")  # noqa: DTZ007
    except ValueError as error:
        raise ValueError("expirations=expires_at") from error


def _now() -> datetime:
    from scripts.promotion import promote_api  # promote_api registers this handler

    return promote_api.CLOCK()
