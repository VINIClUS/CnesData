"""DynamoDB billing catalog: accounts, tenant links, customers and owners."""

import dataclasses
from datetime import timedelta
from typing import Any

from botocore.exceptions import ClientError

from cnes_domain.billing.commands import (
    AttachStripeCustomerCommand,
    CreateBillingAccountCommand,
    LinkBillingTenantCommand,
    TransferOwnerCommand,
)
from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingTenantConflict,
    IdempotencyConflict,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountPage,
    BillingAccountStatus,
    BillingAccountTenantLink,
    BillingAuditEvent,
    ReadConsistency,
)
from cnes_domain.billing.ports import ClockPort
from cnes_domain.control_plane.entities import IdempotencyRecord
from cnes_infra.billing.dynamodb_catalog_checkout import DynamoPendingCheckoutMixin
from cnes_infra.billing.dynamodb_catalog_plans import DynamoPlanCatalogMixin
from cnes_infra.billing.dynamodb_catalog_replays import DynamoLateReplayMixin
from cnes_infra.billing.dynamodb_catalog_tenants import DynamoTenantAccountMixin
from cnes_infra.billing.dynamodb_items import (
    CUSTOMER_MAP_ENTITY,
    audit_outbox_event,
    corrupt_item,
    decode_account,
    decode_account_list_row,
    decode_customer_map,
    decode_idempotency_record,
    decode_link,
    deterministic_id,
    encode_account,
    encode_account_list_row,
    encode_customer_map,
    encode_link,
    encode_tenant_account,
    get_item,
    idempotency_digest,
    idempotency_item,
    outbox_item,
    put_new,
    transact,
    utc_attribute,
)
from cnes_infra.billing.keys import (
    BILLING_ACCOUNT_LIST_PARTITION,
    BILLING_ACCOUNT_LIST_PREFIX,
    account_tenant_key,
    billing_account_key,
    billing_account_list_key,
    stripe_customer_key,
    tenant_account_key,
    tenant_entity_key,
)
from cnes_infra.control_plane.dynamodb_codec import Action, Item, payload, put_action
from cnes_infra.control_plane.dynamodb_keys import idempotency_key, item_key

CREATE_SCOPE = "billing_account.create"
LINK_SCOPE = "billing_account.link_tenant"
IDEMPOTENCY_TTL = timedelta(days=1)
_MIN_ADVANCE = timedelta(microseconds=1)
MAX_PAGE_LIMIT = 100
type _Prior = tuple[Item | None, IdempotencyRecord | None]


def _tenant_check(table: str, tenant_id: str) -> Action:
    return {
        "ConditionCheck": {
            "TableName": table,
            "Key": item_key(*tenant_entity_key(tenant_id)),
            "ConditionExpression": "attribute_exists(pk)",
        }
    }


def _account_check(table: str, command: LinkBillingTenantCommand) -> Action:
    key = billing_account_key(command.link.billing_account_id)
    return {
        "ConditionCheck": {
            "TableName": table,
            "Key": item_key(*key),
            "ConditionExpression": "attribute_exists(pk) AND #status = :active"
            " AND updated_at = :expected",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {
                ":active": {"S": BillingAccountStatus.ACTIVE.value},
                ":expected": {"S": utc_attribute(command.expected_account_updated_at)},
            },
        }
    }


def _list_row_put(table: str, account: BillingAccount) -> Action:
    return {
        "Put": {
            "TableName": table,
            "Item": encode_account_list_row(account),
            "ConditionExpression": "attribute_exists(pk)"
            " AND attribute_not_exists(stripe_customer_id)",
        }
    }


def _cursor_of(last_key: dict[str, Any] | None) -> str | None:
    if last_key is None:
        return None
    sort_key = last_key["sk"]["S"]
    return bytes.fromhex(sort_key.removeprefix(BILLING_ACCOUNT_LIST_PREFIX)).decode()


class DynamoBillingCatalog(
    DynamoLateReplayMixin,
    DynamoPlanCatalogMixin,
    DynamoPendingCheckoutMixin,
    DynamoTenantAccountMixin,
):
    """Catálogo de billing em DynamoDB sem GSI, com escritas em transação única."""

    def __init__(self, client: Any, table_name: str, clock: ClockPort) -> None:
        self._client = client
        self._table = table_name
        self._clock = clock

    def create_account(self, command: CreateBillingAccountCommand) -> BillingAccount:
        """Cria conta, lista, links, idempotência e outbox em uma transação.

        Args: Comando de criação com link inicial e chave de idempotência.
        Returns: A conta criada ou a conta de um replay idêntico.
        Raises: BillingTenantConflict, IdempotencyConflict, erros de billing.
        """
        if command.account.stripe_customer_id is not None:
            raise PermanentBillingError("stripe_customer_requires_attach")
        tenant_id = command.initial_tenant_link.tenant_id
        prior, live = self._prior(
            tenant_id, CREATE_SCOPE, command.idempotency_key, idempotency_digest(command)
        )
        if live is not None:
            return self._replay_account(live)
        if transact(self._client, self._create_actions(command, prior)):
            return command.account
        return self._classify_create(command)

    def get_account(self, billing_account_id: str) -> BillingAccount | None:
        """Lê a conta por chave base com leitura forte."""
        item = self._account_item(billing_account_id)
        return None if item is None else decode_account(item, billing_account_id)

    def get_account_by_customer(self, stripe_customer_id: str) -> BillingAccount | None:
        """Resolve a conta de um Customer; mapa inconsistente é corrupção."""
        item = get_item(self._client, self._table, stripe_customer_key(stripe_customer_id), True)
        if item is None:
            return None
        account = self.get_account(decode_customer_map(item, stripe_customer_id))
        if account is None or account.stripe_customer_id != stripe_customer_id:
            raise corrupt_item(CUSTOMER_MAP_ENTITY)
        return account

    def list_stripe_accounts(self, limit: int, cursor: str | None) -> BillingAccountPage:
        """Lista uma página de contas com Customer, revalidando a chave base.

        Args: Limite de 1 a 100 e cursor opcional.
        Returns: Página com contas e próximo cursor.
        Raises: ValueError, BillingDependencyError.
        """
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_PAGE_LIMIT
        ):
            raise ValueError("limit=invalid")
        response = self._query_page(limit, cursor)
        accounts = (self.get_account(decode_account_list_row(row)) for row in response["Items"])
        attached = tuple(a for a in accounts if a is not None and a.stripe_customer_id is not None)
        return BillingAccountPage(attached, _cursor_of(response.get("LastEvaluatedKey")))

    def get_tenant_link(
        self, billing_account_id: str, tenant_id: str, consistency: ReadConsistency
    ) -> BillingAccountTenantLink | None:
        """Lê o link por chave base; só consistência forte é leitura forte."""
        key = account_tenant_key(billing_account_id, tenant_id)
        item = get_item(self._client, self._table, key, consistency is ReadConsistency.STRONG)
        return None if item is None else decode_link(item, billing_account_id, tenant_id)

    def link_tenant(self, command: LinkBillingTenantCommand) -> BillingAccountTenantLink:
        """Associa um tenant a uma conta ativa e atual em uma transação.

        Args: Comando com link, versão esperada da conta e idempotência.
        Returns: O link criado ou o link de um replay idêntico.
        Raises: BillingTenantConflict, IdempotencyConflict, erros de billing.
        """
        tenant_id = command.link.tenant_id
        prior, live = self._prior(
            tenant_id, LINK_SCOPE, command.idempotency_key, idempotency_digest(command)
        )
        if live is not None:
            return self._replay_link(live, tenant_id)
        if transact(self._client, self._link_actions(command, prior)):
            return command.link
        return self._classify_link(command)

    def attach_customer(self, command: AttachStripeCustomerCommand) -> BillingAccount:
        """Anexa um Customer Stripe à conta; um Customer serve a uma só conta.

        Args: Comando com conta, Customer e updated_at esperado.
        Returns: A conta atualizada, ou a atual se já anexada ao mesmo Customer.
        Raises: PermanentBillingError, RetryableBillingError.
        """
        item = self._account_item(command.billing_account_id)
        if item is None:
            raise PermanentBillingError("billing_account_missing")
        account = decode_account(item, command.billing_account_id)
        customer = command.stripe_customer_id
        if account.stripe_customer_id == customer:
            return account
        if account.stripe_customer_id is not None:
            raise PermanentBillingError("stripe_customer_already_attached")
        if utc_attribute(account.updated_at) != utc_attribute(command.expected_updated_at):
            raise PermanentBillingError("billing_account_stale")
        advanced = max(self._clock(), account.updated_at + _MIN_ADVANCE)
        updated = dataclasses.replace(account, stripe_customer_id=customer, updated_at=advanced)
        if transact(self._client, self._attach_actions(updated, item)):
            return updated
        return self._classify_attach(updated, item)

    def transfer_owner(self, command: TransferOwnerCommand) -> BillingAccount:
        """Transfere o owner com compare-and-set do item da conta.

        Args: Comando com owner esperado e novo owner.
        Returns: A conta atualizada, ou a atual se o owner já é o novo.
        Raises: PermanentBillingError, RetryableBillingError.
        """
        item = self._account_item(command.billing_account_id)
        if item is None:
            raise PermanentBillingError("billing_account_missing")
        account = decode_account(item, command.billing_account_id)
        if account.owner_user_id == command.new_owner_user_id:
            return account
        if account.owner_user_id != command.expected_owner_user_id:
            raise PermanentBillingError("billing_account_owner_mismatch")
        if command.transferred_at < account.updated_at:
            raise PermanentBillingError("billing_account_stale")
        updated = dataclasses.replace(
            account, owner_user_id=command.new_owner_user_id, updated_at=command.transferred_at
        )
        actions = (
            put_action(self._table, encode_account(updated), payload(item)),
            self._outbox(self._transferred_event(command, account.owner_user_id)),
        )
        if transact(self._client, actions):
            return updated
        fresh = self.get_account(command.billing_account_id)
        if fresh is not None and fresh.owner_user_id == command.new_owner_user_id:
            return fresh
        if fresh is None or fresh.owner_user_id != account.owner_user_id:
            raise PermanentBillingError("billing_account_owner_mismatch")
        raise RetryableBillingError("billing_transaction_conflict")

    def _account_item(self, billing_account_id: str) -> Item | None:
        key = billing_account_key(billing_account_id)
        return get_item(self._client, self._table, key, True)

    def _exists(self, key: tuple[str, str]) -> bool:
        return get_item(self._client, self._table, key, True) is not None

    def _outbox(self, event: BillingAuditEvent) -> Action:
        return put_new(self._table, outbox_item(audit_outbox_event(event)))

    def _prior(self, tenant_id: str, scope: str, key: str, digest: str) -> _Prior:
        item = get_item(self._client, self._table, idempotency_key(tenant_id, scope, key), True)
        if item is None:
            return None, None
        record = decode_idempotency_record(item, (tenant_id, scope, key))
        if record.expires_at <= self._clock():
            return item, None
        if record.request_hash != digest:
            raise IdempotencyConflict(f"key={key}")
        return item, record

    def _idempotency_action(self, prior: Item | None, record: IdempotencyRecord) -> Action:
        expected = None if prior is None else payload(prior)
        return put_action(self._table, idempotency_item(record), expected)

    def _record(
        self, tenant_id: str, scope: str, command: Any, account_id: str
    ) -> IdempotencyRecord:
        now = self._clock()
        return IdempotencyRecord(
            tenant_id=tenant_id,
            scope=scope,
            key=command.idempotency_key,
            request_hash=idempotency_digest(command),
            status="COMPLETED",
            resource_id=account_id,
            created_at=now,
            expires_at=now + IDEMPOTENCY_TTL,
        )

    def _replay_account(self, record: IdempotencyRecord) -> BillingAccount:
        account = self.get_account(record.resource_id)
        if account is None:
            raise RetryableBillingError("billing_idempotency_incomplete")
        return account

    def _replay_link(self, record: IdempotencyRecord, tenant_id: str) -> BillingAccountTenantLink:
        link = self.get_tenant_link(record.resource_id, tenant_id, ReadConsistency.STRONG)
        if link is None:
            raise RetryableBillingError("billing_idempotency_incomplete")
        return link

    def _create_actions(
        self, command: CreateBillingAccountCommand, prior: Item | None
    ) -> tuple[Action, ...]:
        account, link = command.account, command.initial_tenant_link
        event = BillingAuditEvent(
            event_id=deterministic_id("billing_account.created", account.billing_account_id),
            event_type="billing_account.created",
            aggregate_id=account.billing_account_id,
            actor_id=account.owner_user_id,
            reason_code=link.reason_code,
            occurred_at=account.created_at,
            attributes={"tenant_id": link.tenant_id},
        )
        record = self._record(link.tenant_id, CREATE_SCOPE, command, account.billing_account_id)
        return (
            _tenant_check(self._table, link.tenant_id),
            put_new(self._table, encode_account(account)),
            put_new(self._table, encode_account_list_row(account)),
            put_new(self._table, encode_link(link)),
            put_new(self._table, encode_tenant_account(link)),
            self._idempotency_action(prior, record),
            self._outbox(event),
        )

    def _classify_create(self, command: CreateBillingAccountCommand) -> BillingAccount:
        tenant_id = command.initial_tenant_link.tenant_id
        digest = idempotency_digest(command)
        _, live = self._prior(tenant_id, CREATE_SCOPE, command.idempotency_key, digest)
        if live is not None:
            return self._replay_account(live)
        existing = self._created_account(command)
        if existing is not None:
            return existing
        self._raise_tenant_failure(tenant_id)
        if self._exists(billing_account_key(command.account.billing_account_id)):
            raise PermanentBillingError("billing_account_exists")
        raise RetryableBillingError("billing_transaction_conflict")

    def _raise_tenant_failure(self, tenant_id: str) -> None:
        if self._exists(tenant_account_key(tenant_id)):
            raise BillingTenantConflict(f"tenant_id={tenant_id}")
        if not self._exists(tenant_entity_key(tenant_id)):
            raise PermanentBillingError("tenant_missing")

    def _link_actions(
        self, command: LinkBillingTenantCommand, prior: Item | None
    ) -> tuple[Action, ...]:
        link = command.link
        event = BillingAuditEvent(
            event_id=deterministic_id(
                "billing_account.tenant_linked", link.billing_account_id, link.tenant_id
            ),
            event_type="billing_account.tenant_linked",
            aggregate_id=link.billing_account_id,
            actor_id=link.linked_by_user_id,
            reason_code=link.reason_code,
            occurred_at=link.linked_at,
            attributes={"tenant_id": link.tenant_id},
        )
        record = self._record(link.tenant_id, LINK_SCOPE, command, link.billing_account_id)
        return (
            _account_check(self._table, command),
            _tenant_check(self._table, link.tenant_id),
            put_new(self._table, encode_link(link)),
            put_new(self._table, encode_tenant_account(link)),
            self._idempotency_action(prior, record),
            self._outbox(event),
        )

    def _classify_link(self, command: LinkBillingTenantCommand) -> BillingAccountTenantLink:
        tenant_id = command.link.tenant_id
        digest = idempotency_digest(command)
        _, live = self._prior(tenant_id, LINK_SCOPE, command.idempotency_key, digest)
        if live is not None:
            return self._replay_link(live, tenant_id)
        existing = self._linked_replay(command)
        if existing is not None:
            return existing
        self._raise_tenant_failure(tenant_id)
        self._raise_account_failure(command)
        raise RetryableBillingError("billing_transaction_conflict")

    def _raise_account_failure(self, command: LinkBillingTenantCommand) -> None:
        account = self.get_account(command.link.billing_account_id)
        if account is None:
            raise PermanentBillingError("billing_account_missing")
        if account.status is not BillingAccountStatus.ACTIVE:
            raise PermanentBillingError("billing_account_inactive")
        if utc_attribute(account.updated_at) != utc_attribute(command.expected_account_updated_at):
            raise PermanentBillingError("billing_account_stale")

    def _attach_actions(self, updated: BillingAccount, current: Item) -> tuple[Action, ...]:
        account_id = updated.billing_account_id
        customer = updated.stripe_customer_id or ""
        event = BillingAuditEvent(
            event_id=deterministic_id("billing_account.customer_attached", account_id, customer),
            event_type="billing_account.customer_attached",
            aggregate_id=account_id,
            actor_id="billing_system",
            reason_code="stripe_customer_attached",
            occurred_at=self._clock(),
            attributes={"stripe_customer_id": customer},
        )
        return (
            put_action(self._table, encode_account(updated), payload(current)),
            put_new(self._table, encode_customer_map(account_id, customer)),
            _list_row_put(self._table, updated),
            self._outbox(event),
        )

    def _classify_attach(self, updated: BillingAccount, current: Item) -> BillingAccount:
        customer = updated.stripe_customer_id or ""
        mapped = get_item(self._client, self._table, stripe_customer_key(customer), True)
        if (
            mapped is not None
            and decode_customer_map(mapped, customer) != updated.billing_account_id
        ):
            raise PermanentBillingError("stripe_customer_conflict")
        fresh = self._account_item(updated.billing_account_id)
        if fresh is not None and mapped is not None:
            attached = decode_account(fresh, updated.billing_account_id)
            if attached.stripe_customer_id == customer:
                return attached
        if fresh is None or payload(fresh) != payload(current):
            raise PermanentBillingError("billing_account_stale")
        raise RetryableBillingError("billing_transaction_conflict")

    def _transferred_event(
        self, command: TransferOwnerCommand, previous_owner: str
    ) -> BillingAuditEvent:
        return BillingAuditEvent(
            event_id=deterministic_id(
                "billing_account.transferred",
                command.billing_account_id,
                command.new_owner_user_id,
                utc_attribute(command.transferred_at),
            ),
            event_type="billing_account.transferred",
            aggregate_id=command.billing_account_id,
            actor_id=command.actor_id,
            reason_code=command.reason_code,
            occurred_at=command.transferred_at,
            attributes={
                "previous_owner_user_id": previous_owner,
                "new_owner_user_id": command.new_owner_user_id,
            },
        )

    def _query_page(self, limit: int, cursor: str | None) -> dict[str, Any]:
        request: dict[str, Any] = {
            "TableName": self._table,
            "KeyConditionExpression": "pk = :partition AND begins_with(sk, :prefix)",
            "FilterExpression": "attribute_exists(stripe_customer_id)",
            "ExpressionAttributeValues": {
                ":partition": {"S": BILLING_ACCOUNT_LIST_PARTITION},
                ":prefix": {"S": BILLING_ACCOUNT_LIST_PREFIX},
            },
            "ConsistentRead": True,
            "Limit": limit,
        }
        if cursor is not None:
            request["ExclusiveStartKey"] = item_key(*billing_account_list_key(cursor))
        try:
            return dict(self._client.query(**request))
        except ClientError as error:
            raise BillingDependencyError("dynamodb_unavailable") from error
