"""Revogação imediata de entitlement sobre a single table DynamoDB."""

import json
import logging
from dataclasses import replace
from datetime import datetime
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from cnes_domain.billing.errors import (
    BillingDependencyError,
    RetryableBillingError,
)
from cnes_domain.billing.execution import RunBillingState
from cnes_domain.billing.ports import ClockPort
from cnes_domain.billing.revocation import (
    PUBLICATION_DENIABLE_RUN_STATES,
    REVOCABLE_RUN_STATES,
    CancelRunUnitsCommand,
    CancelRunUnitsResult,
    FailDeniedPublicationCommand,
    RevocableRunPage,
    RevocationPhase,
    RevocationProgress,
    RevokeRunCommand,
)
from cnes_domain.control_plane.entities import OutboxEvent, Run, RunDispatch
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.transitions import transition_run
from cnes_infra.billing.dynamodb_items import (
    UNAVAILABLE_CODE,
    canonical_json,
    corrupt_item,
    get_item,
    outbox_item,
    put_new,
    transact,
)
from cnes_infra.billing.dynamodb_quota_items import (
    RUN_LOOKUP_ENTITY,
    encode_run_billing_state,
)
from cnes_infra.billing.dynamodb_revocation_publication import PublicationDenial
from cnes_infra.billing.dynamodb_revocation_units import (
    RunCancellation,
    RunContext,
    load_context,
)
from cnes_infra.billing.keys import (
    billing_partition,
    revocation_progress_key,
    run_lookup_key,
    run_lookup_partition,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import (
    Item,
    check_action,
    decode_model,
    payload,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import dispatch_key
from cnes_infra.control_plane.dynamodb_run_codec import run_item

logger = logging.getLogger(__name__)
PROGRESS_ENTITY = "BILLINGREVOCATIONPROGRESS"
_RUN_PREFIX = "RUN#"
_PROGRESS_PREFIX = "REVOCATION#"
_DECODE_ERRORS = (KeyError, TypeError, ValueError, AttributeError)
_STALE = "run_revocation_stale"


def _text(value: str) -> dict[str, str]:
    return {"S": value}


def _query(client: Any, request: dict[str, Any]) -> dict[str, Any]:
    try:
        return client.query(**request)
    except (ClientError, BotoCoreError) as error:
        raise BillingDependencyError(UNAVAILABLE_CODE) from error


def _partition_query(table: str, partition: str, prefix: str) -> dict[str, Any]:
    return {
        "TableName": table,
        "KeyConditionExpression": "pk = :partition AND begins_with(sk, :prefix)",
        "ExpressionAttributeValues": {":partition": _text(partition), ":prefix": _text(prefix)},
        "ConsistentRead": True,
    }


def _decode_lookup(item: Item, billing_account_id: str) -> tuple[str, str]:
    try:
        data = json.loads(item["payload"]["S"])
        tenant_id, run_id = data["tenant_id"], data["run_id"]
        identity = (
            item["entity"]["S"], data["billing_account_id"], item["pk"]["S"], item["sk"]["S"]
        )
        key = run_lookup_key(billing_account_id, tenant_id, run_id)
        if identity == (RUN_LOOKUP_ENTITY, billing_account_id, *key):
            return tenant_id, run_id
    except _DECODE_ERRORS as error:
        raise corrupt_item(RUN_LOOKUP_ENTITY) from error
    raise corrupt_item(RUN_LOOKUP_ENTITY)


def _progress_item(progress: RevocationProgress) -> Item:
    key = revocation_progress_key(progress.billing_account_id, progress.entitlement_version)
    return {
        "pk": _text(key[0]),
        "sk": _text(key[1]),
        "entity": _text(PROGRESS_ENTITY),
        "payload": _text(canonical_json(progress)),
    }


def _decode_progress(item: Item) -> RevocationProgress:
    try:
        data = json.loads(item["payload"]["S"])
        progress = RevocationProgress(
            **{
                **data,
                "phase": RevocationPhase(data["phase"]),
                "updated_at": datetime.fromisoformat(data["updated_at"]),
            }
        )
        stored = (item["entity"], item["pk"], item["sk"])
        expected = _progress_item(progress)
        if stored == tuple(expected[name] for name in ("entity", "pk", "sk")):
            return progress
    except _DECODE_ERRORS as error:
        raise corrupt_item(PROGRESS_ENTITY) from error
    raise corrupt_item(PROGRESS_ENTITY)


def _require_event(command: RevokeRunCommand, event: OutboxEvent) -> None:
    matches = (
        event.tenant_id == command.tenant_id
        and event.aggregate_id == command.run_id
        and event.delivered_at is None
    )
    if not matches:
        raise ValueError("reason=revocation_event_mismatch")


def _cursor_request(request: dict[str, Any], partition: str, cursor: str | None) -> dict[str, Any]:
    if cursor is None:
        return request
    if not cursor.startswith(_RUN_PREFIX):
        raise ValueError("reason=invalid_revocation_cursor")
    return {**request, "ExclusiveStartKey": {"pk": _text(partition), "sk": _text(cursor)}}


class DynamoRevocationStore:
    """Implementa RevocationStorePort sobre a single table DynamoDB."""

    def __init__(self, client: Any, table_name: str, clock: ClockPort) -> None:
        self._client = client
        self._table = table_name
        self._clock = clock
        self._plane = DynamoDBControlPlane(client, table_name, clock)
        self._cancellation = RunCancellation(client, table_name, self._plane, clock)
        self._denial = PublicationDenial(client, table_name, clock)

    def get_run(self, tenant_id: str, run_id: str) -> Run | None:
        """Lê o Run canônico."""
        return self._plane.get_run(tenant_id, run_id)

    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None:
        """Lê o companion de billing do Run."""
        return self._plane.get_run_billing_state(tenant_id, run_id)

    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        """Lê o dispatch ativo do Run."""
        return self._plane.get_active_run_dispatch(tenant_id, run_id)

    def get_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        """Lê com consistência forte o dispatch canônico, em qualquer estado ou lease."""
        item = get_item(self._client, self._table, dispatch_key(tenant_id, run_id), True)
        return None if item is None else decode_model(item, RunDispatch)

    def list_revocable_runs(
        self, billing_account_id: str, limit: int, cursor: str | None,
    ) -> RevocableRunPage:
        """Lista, com leitura forte, os Runs revogáveis, em publicação ou cancelados com fence.

        Runs CANCELED com cancel_requested seguem listados para liquidação idempotente.
        Runs PUBLISHING são listados para que a publicação negada seja falhada.
        Lookups órfãos são ignorados com aviso.

        Args: Conta, tamanho da página e cursor opaco da página anterior.
        Returns: Companions revogáveis e o cursor da próxima página.
        Raises: ValueError, PermanentBillingError, BillingDependencyError.
        """
        partition = run_lookup_partition(billing_account_id)
        request = {**_partition_query(self._table, partition, _RUN_PREFIX), "Limit": limit}
        response = _query(self._client, _cursor_request(request, partition, cursor))
        states = (self._revocable(billing_account_id, item) for item in response["Items"])
        last_key = response.get("LastEvaluatedKey")
        next_cursor = None if last_key is None else last_key["sk"]["S"]
        return RevocableRunPage(tuple(state for state in states if state), next_cursor)

    def _revocable(self, billing_account_id: str, lookup: Item) -> RunBillingState | None:
        tenant_id, run_id = _decode_lookup(lookup, billing_account_id)
        state = self.get_run_billing_state(tenant_id, run_id)
        run = self.get_run(tenant_id, run_id)
        if state is None or run is None:
            logger.warning("revocation_lookup_orphan tenant_id=%s run_id=%s", tenant_id, run_id)
            return None
        fenced_cancel = run.state is RunState.CANCELED and state.cancel_requested
        listed = REVOCABLE_RUN_STATES | PUBLICATION_DENIABLE_RUN_STATES
        return state if run.state in listed or fenced_cancel else None

    def request_run_revocation(
        self, command: RevokeRunCommand, event: OutboxEvent,
    ) -> RunBillingState:
        """Cerca o Run: cancel_requested, fence+1 e evento em uma transação.

        Args: Comando com as expectativas de estado e fence, e o evento de outbox.
        Returns: Companion após o fence (o mesmo em retentativas).
        Raises: ValueError, PermanentBillingError, RetryableBillingError.
        """
        _require_event(command, event)
        context = load_context(self._client, self._table, command.tenant_id, command.run_id)
        if context.state.cancel_requested:
            return context.state
        stale = (
            context.state.fencing_token != command.expected_fencing_token
            or context.run.state is not command.expected_state
        )
        if stale:
            raise RetryableBillingError(_STALE)
        updated = replace(
            context.state,
            cancel_requested=True,
            fencing_token=context.state.fencing_token + 1,
            updated_at=self._clock(),
        )
        actions = (
            self._run_fence_action(context),
            put_action(
                self._table, encode_run_billing_state(updated), payload(context.companion_item)
            ),
            put_new(self._table, outbox_item(event)),
        )
        if not transact(self._client, actions):
            raise RetryableBillingError(_STALE)
        return updated

    def _run_fence_action(self, context: RunContext) -> dict[str, Any]:
        if context.run.state is RunState.CANCEL_REQUESTED:
            return check_action(self._table, context.run_item)
        requested = transition_run(context.run, RunState.CANCEL_REQUESTED)
        return put_action(self._table, run_item(requested), payload(context.run_item))

    def fail_denied_publication(
        self, command: FailDeniedPublicationCommand, event: OutboxEvent,
    ) -> bool:
        """Move o Run PUBLISHING para FAILED e libera a reserva em uma transação.

        Args: Comando com fence esperado e o evento de outbox.
        Returns: True se gravou; False se o Run não era elegível ou outro atuou antes.
        Raises: ValueError, PermanentBillingError, RetryableBillingError.
        """
        return self._denial.fail(command, event)

    def cancel_run_units(self, command: CancelRunUnitsCommand) -> CancelRunUnitsResult:
        """Cancela um lote de unidades; finaliza e liquida o Run no último lote.

        Args: Comando com fence esperado, limite, cursor e instante.
        Returns: Unidades canceladas, próximo cursor e se o Run foi cancelado.
        Raises: PermanentBillingError, RetryableBillingError.
        """
        return self._cancellation.cancel(command)

    def get_revocation_progress(self, billing_account_id: str) -> RevocationProgress | None:
        """Lê a versão mais recente do progresso de revogação da conta."""
        partition = billing_partition(billing_account_id)
        request = {
            **_partition_query(self._table, partition, _PROGRESS_PREFIX),
            "ScanIndexForward": False,
            "Limit": 1,
        }
        items = _query(self._client, request)["Items"]
        return _decode_progress(items[0]) if items else None

    def save_revocation_progress(
        self, expected: RevocationProgress | None, replacement: RevocationProgress,
    ) -> bool:
        """Grava o progresso por compare-and-set sobre o payload esperado.

        Args: Progresso esperado (None para criar) e substituto da mesma versão.
        Returns: True se gravou; False se o armazenado divergiu.
        Raises: ValueError: Conta ou versão diferentes.
        """
        if expected is not None and (expected.billing_account_id, expected.entitlement_version) != (
            replacement.billing_account_id,
            replacement.entitlement_version,
        ):
            raise ValueError("reason=revocation_progress_mismatch")
        previous = None if expected is None else payload(_progress_item(expected))
        action = put_action(self._table, _progress_item(replacement), previous)
        return transact(self._client, (action,))
