"""Composicao real do fluxo de billing Stripe sobre DynamoDB Local, exceto a Stripe."""

import hashlib
import hmac
import json
import os
import sys
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from itertools import count
from types import ModuleType, SimpleNamespace
from typing import Any

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI
from fastapi.testclient import TestClient

from central_api.routes.stripe_webhook import (
    get_stripe_webhook_verifier,
    get_webhook_inbox,
    router,
)
from cnes_domain.billing.commands import (
    CreateRunRequest,
    ReserveRunCommand,
    StripeBillingState,
    StripeStateRequest,
)
from cnes_domain.billing.gate import (
    EntitlementGate,
    EntitlementGateDependencies,
    RunReservationSettings,
)
from cnes_domain.billing.inbox import (
    RecoveryRequest,
    StripeEvent,
    StripeEventListRequest,
    StripeEventPage,
)
from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountStatus,
    PlanVersion,
    QuotaLimits,
    ReadConsistency,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.control_plane.entities import RunDependency
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import (
    encode_account,
    encode_customer_map,
    encode_plan,
    encode_price_map,
)
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.projector import ProjectorDependencies, StripeEventProjector
from cnes_infra.billing.recovery import RecoveryDependencies, WebhookRecovery
from cnes_infra.billing.recovery_cursor import DynamoRecoveryCursor
from cnes_infra.billing.webhook_inbox import WebhookInbox
from cnes_infra.billing.webhook_verifier import StripeWebhookVerifier
from packages.cnes_infra.tests.billing.test_webhook_verifier import (
    SIGNING_KEY,
    SignatureVerificationError,
    StripeError,
    construct_event,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

ENDPOINT = os.getenv("DYNAMODB_ENDPOINT_URL", "http://127.0.0.1:18000")
REGION = "us-east-1"
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
WEBHOOK_URL = "/api/v1/billing/webhooks/stripe"
ACCOUNT_ID = "ba_01"
CUSTOMER = "cus_01"
SUBSCRIPTION = "sub_01"
PRICE_V1 = "price_monthly"
PRICE_V2 = "price_v2"
REQUEST = RecoveryRequest(72, 100)
SHA = "a" * 64
STRONG = ReadConsistency.STRONG
_INDEXES = tuple(f"gsi{number}" for number in range(1, 7))


def dynamodb_client() -> Any:
    """Cria o cliente DynamoDB Local ou pula o teste se o endpoint estiver inacessivel."""
    client = boto3.client(
        "dynamodb",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(retries={"max_attempts": 1}, connect_timeout=2, read_timeout=10),
    )
    try:
        client.list_tables(Limit=1)
    except (BotoCoreError, ClientError, OSError):
        pytest.skip("reason=dynamodb_local_unreachable")
    return client


def create_billing_table(client: Any, table_name: str) -> None:
    """Cria a tabela single-table com pk/sk e gsi1..gsi6 (projecao ALL)."""
    index_attributes = tuple(f"{index}{suffix}" for index in _INDEXES for suffix in ("pk", "sk"))
    names = ("pk", "sk", *index_attributes)
    throughput = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    indexes = [
        {
            "IndexName": index,
            "KeySchema": [
                {"AttributeName": f"{index}pk", "KeyType": "HASH"},
                {"AttributeName": f"{index}sk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput": throughput,
        }
        for index in _INDEXES
    ]
    client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"} for n in names],
        GlobalSecondaryIndexes=indexes,
        ProvisionedThroughput=throughput,
    )


def install_fake_stripe(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Injeta o SDK Stripe simulado em sys.modules."""
    module = ModuleType("stripe")
    module.StripeError = StripeError
    module.SignatureVerificationError = SignatureVerificationError
    module.Webhook = SimpleNamespace(construct_event=construct_event)
    monkeypatch.setitem(sys.modules, "stripe", module)
    return module


def sign(payload: bytes, secret: str = SIGNING_KEY) -> str:
    timestamp = 1_780_000_000
    signed = f"{timestamp}.{payload.decode()}".encode()
    return f"t={timestamp},v1={hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()}"


def make_state(**changes: Any) -> StripeBillingState:
    state = StripeBillingState(
        stripe_customer_id=CUSTOMER,
        stripe_subscription_id=SUBSCRIPTION,
        subscription_status=SubscriptionStatus.ACTIVE,
        cancel_at_period_end=False,
        stripe_price_id=PRICE_V1,
        active_features=frozenset({"create_run"}),
        period_start=NOW,
        period_end=NOW + timedelta(days=30),
        latest_invoice_id=None,
    )
    return replace(state, **changes)


def make_quotas(max_agents: int = 5) -> QuotaLimits:
    return QuotaLimits(
        max_tenants=3,
        max_agents=max_agents,
        max_runs_per_period=100,
        max_concurrency=2,
        retention_days=365,
        athena_scan_budget_bytes=10**9,
    )


def make_plan(plan_version_id: str, max_agents: int, prices: tuple[str, ...]) -> PlanVersion:
    return PlanVersion(
        plan_version_id=plan_version_id,
        plan_key="basico",
        stripe_product_id="prod_01",
        stripe_price_ids=prices,
        features=frozenset({"create_run"}),
        quotas=make_quotas(max_agents),
        grace_period_days=7,
        effective_from=NOW,
    )


def make_run_request() -> CreateRunRequest:
    return CreateRunRequest(
        billing_account_id=ACCOUNT_ID,
        tenant_id="354130",
        run_id="run-1",
        competencia="2026-08",
        dataset_name="cnes",
        dependencies=(RunDependency(source_type="CNES", file_subtype="LOCAL", required=True),),
        idempotency_key="req-1",
        request_hash="a" * 64,
        requested_concurrency=2,
        estimated_scan_bytes=0,
    )


def webhook_body(
    event_id: str, event_type: str = "checkout.session.completed", created: int = 1_780_000_123,
) -> bytes:
    obj = {"object": "checkout.session", "customer": CUSTOMER, "subscription": SUBSCRIPTION}
    event = {"id": event_id, "type": event_type, "created": created, "data": {"object": obj}}
    return json.dumps(event).encode()


def stripe_event(event_id: str) -> StripeEvent:
    return StripeEvent(event_id, "customer.subscription.updated", NOW, CUSTOMER, SUBSCRIPTION, SHA)


def serve_pages(ids: list[str]) -> Callable[[StripeEventListRequest], StripeEventPage]:
    def serve(request: StripeEventListRequest) -> StripeEventPage:
        start = 0 if request.starting_after is None else ids.index(request.starting_after) + 1
        chunk = ids[start : start + request.limit]
        events = tuple(stripe_event(event_id) for event_id in chunk)
        return StripeEventPage(events, start + request.limit < len(ids))

    return serve


class FakeStripeGateway:
    def __init__(self) -> None:
        self.state = make_state()
        self.failures: list[Exception] = []
        self.state_calls = 0
        self.on_state: Callable[[], None] | None = None
        self.list_requests: list[StripeEventListRequest] = []
        self.pager: Callable[[StripeEventListRequest], StripeEventPage] = _empty_page

    def get_current_state(self, request: StripeStateRequest) -> StripeBillingState:
        self.state_calls += 1
        if self.on_state is not None:
            self.on_state()
        if self.failures:
            raise self.failures.pop(0)
        return self.state

    def list_events(self, request: StripeEventListRequest) -> StripeEventPage:
        self.list_requests.append(request)
        return self.pager(request)


def _empty_page(request: StripeEventListRequest) -> StripeEventPage:
    return StripeEventPage((), False)


class FakeCheckout:
    def __init__(self) -> None:
        self.completed: list[str] = []

    def complete_redirect(self, session_id: str) -> None:
        self.completed.append(session_id)


class FakeQuotas:
    def __init__(self) -> None:
        self.commands: list[ReserveRunCommand] = []

    def reserve_and_create_run(self, command: ReserveRunCommand) -> RunAuthorization:
        self.commands.append(command)
        return RunAuthorization(
            billing_account_id=command.request.billing_account_id,
            plan_version_id=command.snapshot.plan_version_id,
            entitlement_version=command.snapshot.entitlement_version,
            max_concurrency=command.request.requested_concurrency,
            budget_reservation_id=command.reservation_id,
            authorized_at=command.expires_at,
        )


class FaultyClient:
    """Envolve o cliente DynamoDB e falha operacoes selecionadas com o erro configurado."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._faults: dict[str, Exception] = {}

    def fail(self, operation: str, error: Exception) -> None:
        self._faults[operation] = error

    def heal(self) -> None:
        self._faults.clear()

    def __getattr__(self, name: str) -> Any:
        if name in self._faults:
            return self._raiser(self._faults[name])
        return getattr(self._inner, name)

    @staticmethod
    def _raiser(error: Exception) -> Callable[..., Any]:
        def raise_error(**_: Any) -> Any:
            raise error

        return raise_error


def unavailable_error(operation: str) -> ClientError:
    response = {"Error": {"Code": "ServiceUnavailable", "Message": "down"}}
    return ClientError(response, operation)


class BillingStack:
    """Verificador, rota, inbox, projecao, recovery e gate reais sobre um unico DynamoDB."""

    def __init__(self, client: Any, table_name: str) -> None:
        self.client = client
        self.table_name = table_name
        self.clock = MutableClock(NOW)
        self.stripe = FakeStripeGateway()
        self.checkout = FakeCheckout()
        self.quotas = FakeQuotas()
        now = self.clock.now
        self.inbox = WebhookInbox(client, table_name, now)
        self.projection = DynamoEntitlementProjection(client, table_name, now)
        self.catalog = DynamoBillingCatalog(client, table_name, now)
        self.cursor = DynamoRecoveryCursor(client, table_name, now)
        self.projector = self._projector()
        self.recovery = self.recovery_with(self.cursor)
        self.gate = self._gate()
        self.http = self._http()
        self._seed()

    def recovery_with(self, cursor: Any) -> WebhookRecovery:
        return WebhookRecovery(
            RecoveryDependencies(
                inbox=self.inbox,
                projector=self.projector,
                stripe=self.stripe,
                cursor=cursor,
                clock=self.clock.now,
            )
        )

    def post_webhook(
        self,
        event_id: str,
        event_type: str = "checkout.session.completed",
        signing_key: str = SIGNING_KEY,
    ) -> Any:
        payload = webhook_body(event_id, event_type)
        return self.http.post(WEBHOOK_URL, content=payload, headers={
            "Stripe-Signature": sign(payload, signing_key),
        })

    def drain(self) -> Any:
        return self.recovery.drain_inbox(100)

    def inbox_items(self) -> list[dict[str, Any]]:
        items = self.client.scan(TableName=self.table_name, ConsistentRead=True)["Items"]
        return [item for item in items if item["entity"]["S"] == "STRIPEEVENTINBOX"]

    def outbox_count(self) -> int:
        items = self.client.scan(TableName=self.table_name, ConsistentRead=True)["Items"]
        return sum(1 for item in items if item["entity"]["S"] == "OUTBOXEVENT")

    def snapshot(self) -> Any:
        return self.projection.get_snapshot(ACCOUNT_ID, STRONG)

    def inbox_state(self, event_id: str) -> Any:
        return self.inbox.get_state(event_id, STRONG)

    def _projector(self) -> StripeEventProjector:
        return StripeEventProjector(
            ProjectorDependencies(
                inbox=self.inbox,
                catalog=self.catalog,
                stripe=self.stripe,
                projection=self.projection,
                clock=self.clock.now,
            )
        )

    def _gate(self) -> EntitlementGate:
        ids = count(1)
        settings = RunReservationSettings(
            deployment_max_concurrency=4,
            reservation_id_factory=lambda: f"res-{next(ids):03d}",
            reservation_ttl=timedelta(minutes=5),
        )
        return EntitlementGate(
            EntitlementGateDependencies(
                projection=self.projection,
                quotas=self.quotas,
                clock=self.clock.now,
                run_settings=settings,
            )
        )

    def _http(self) -> TestClient:
        app = FastAPI()
        app.include_router(router)
        verifier = StripeWebhookVerifier(SIGNING_KEY)
        app.dependency_overrides[get_stripe_webhook_verifier] = lambda: verifier
        app.dependency_overrides[get_webhook_inbox] = lambda: self.inbox
        return TestClient(app, raise_server_exceptions=False)

    def _seed(self) -> None:
        account = BillingAccount(
            billing_account_id=ACCOUNT_ID,
            stripe_customer_id=CUSTOMER,
            owner_user_id="user-owner",
            status=BillingAccountStatus.ACTIVE,
            created_at=NOW,
            updated_at=NOW,
        )
        plans = (
            make_plan("plan_v1", 5, (PRICE_V1, "price_yearly")),
            make_plan("plan_v2", 9, (PRICE_V2,)),
        )
        items = [encode_account(account), encode_customer_map(ACCOUNT_ID, CUSTOMER)]
        for plan in plans:
            items.append(encode_plan(plan))
            prices = plan.stripe_price_ids
            items.extend(encode_price_map(price, plan.plan_version_id) for price in prices)
        for item in items:
            self.client.put_item(TableName=self.table_name, Item=item)
