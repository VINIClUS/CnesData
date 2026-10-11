"""Fixtures E2E do ciclo de vida de billing com Stripe Test Clock e DynamoDB Local."""

import math
import os
import shutil
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest

from cnes_domain.billing import models
from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.inbox import InboxProcessingState, RecoveryRequest
from cnes_domain.billing.policy import EntitlementPolicy
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.profiles import BillingMode
from cnes_infra.billing import dynamodb_items as items
from cnes_infra.billing.audit_outbox import DynamoBillingAudit
from cnes_infra.billing.composition import BillingStorage, build_webhook_recovery
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.dynamodb_revocation import DynamoRevocationStore
from cnes_infra.billing.keys import usage_key
from cnes_infra.billing.projector import ProjectorDependencies
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.billing.stripe_gateway import StripeGateway, StripeGatewayConfig
from cnes_infra.billing.webhook_inbox import WebhookInbox
from cnes_infra.billing.webhook_inbox_items import INBOX_ENTITY, STRIPE_WEBHOOK_EVENT_TYPES
from cnes_infra.billing.wiring import BillingGateResources, build_entitlement_gate
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from packages.cnes_infra.tests.billing.billing_factories import make_account, make_plan
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, make_limits, make_run_request
from packages.cnes_infra.tests.billing.revocation_support import (
    RevEnv,
    create_named_table,
    get_raw,
    lookup_period,
)
from packages.cnes_infra.tests.billing.test_control_plane_extensions import SpyClient
from packages.cnes_infra.tests.contracts.clock import MutableClock

CLOCK_NAME = "cnesdata-bil-024"
WEBHOOK_PATH = "/api/v1/billing/webhooks/stripe"
GRACE_DAYS = 7
RECOVERY_LOOKBACK_HOURS = 2160
RECOVERY_PAGE = 100
RECOVERY_MAX_PAGES = 50
PLAN_VERSION_ID = "plan_e2e"
DEPLOYMENT_LIMIT = 4
ENDPOINT = "http://127.0.0.1:18000"
STARTUP_SECONDS, STOP_SECONDS = 45.0, 10
INBOX_DONE_STATES = (InboxProcessingState.PROCESSED, InboxProcessingState.IGNORED)


class StripeE2EConfigError(ValueError):
    """Configuracao E2E invalida; a mensagem e apenas o codigo key=value."""


@dataclass(frozen=True, slots=True)
class StripeE2EConfig:
    secret_key: str = field(repr=False)
    price_id: str
    webhook_secret: str | None = field(repr=False)
    delivery: str
    timeout_seconds: float
    dynamodb_endpoint: str


@dataclass(slots=True)
class InboxTarget:
    inbox: Any | None = None


@dataclass(frozen=True, slots=True)
class E2EInfra:
    config: StripeE2EConfig
    stripe: Any
    dynamodb: Any
    target: InboxTarget


def _parse_timeout(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as error:
        raise StripeE2EConfigError("stripe_e2e_timeout_invalid") from error
    if not math.isfinite(value) or value <= 0:
        raise StripeE2EConfigError("stripe_e2e_timeout_invalid")
    return value


def load_config(environ: Mapping[str, str]) -> StripeE2EConfig:
    """Returns: Configuracao validada. Raises: StripeE2EConfigError com codigo key=value."""
    delivery = environ.get("STRIPE_E2E_DELIVERY", "").strip() or "cli"
    if delivery not in ("cli", "recover"):
        raise StripeE2EConfigError("stripe_e2e_delivery_invalid")
    required = ["STRIPE_TEST_SECRET_KEY", "STRIPE_TEST_PRICE_ID"]
    if delivery == "cli":
        required.append("STRIPE_TEST_WEBHOOK_SECRET")
    missing = sorted(name for name in required if not environ.get(name, "").strip())
    if missing:
        raise StripeE2EConfigError(f"stripe_test_config_missing missing={','.join(missing)}")
    secret_key = environ["STRIPE_TEST_SECRET_KEY"].strip()
    if not secret_key.startswith("sk_test_"):
        raise StripeE2EConfigError("stripe_test_key_invalid reason=not_sk_test")
    timeout = environ.get("STRIPE_E2E_TIMEOUT_SECONDS", "").strip()
    webhook_secret = environ.get("STRIPE_TEST_WEBHOOK_SECRET", "").strip() or None
    return StripeE2EConfig(
        secret_key=secret_key,
        price_id=environ["STRIPE_TEST_PRICE_ID"].strip(),
        webhook_secret=webhook_secret if delivery == "cli" else None,
        delivery=delivery,
        timeout_seconds=_parse_timeout(timeout) if timeout else 180.0,
        dynamodb_endpoint=environ.get("DYNAMODB_ENDPOINT_URL", "").strip() or ENDPOINT,
    )


def _poll_until(timeout_seconds: float, step: Callable[[], bool]) -> bool:
    deadline = time.monotonic() + timeout_seconds
    delay = 0.5
    while not step():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(delay, remaining))
        delay = min(delay * 1.5, 5.0)
    return True


class ClockRuntime:
    """Runtime E2E: Stripe Test Clock, DynamoDB Local e componentes reais de billing."""

    def __init__(self, infra: E2EInfra, table: str, test_clock: Any) -> None:
        self.config, self.stripe, self.client = infra.config, infra.stripe, infra.dynamodb
        self.table, self.price = table, infra.config.price_id
        self.test_clock_id = test_clock.id
        self.subscription_id: str | None = None
        self.clock = MutableClock(datetime.fromtimestamp(test_clock.frozen_time, tz=UTC))
        self._stripe_time = self.clock.now()
        self._wire()
        self._seed()

    def subscribe(self, trial_days: int | None = None) -> str:
        """Returns: Id da assinatura Stripe criada, com trial opcional."""
        params: dict[str, Any] = {"customer": self.customer_id, "items": [{"price": self.price}]}
        if trial_days:
            params["trial_period_days"] = trial_days
        self.subscription_id = self.stripe.v1.subscriptions.create(params=params).id
        return cast("str", self.subscription_id)

    def period(self) -> tuple[datetime, datetime]:
        subscription = self.stripe.v1.subscriptions.retrieve(self._subscription())
        item = subscription.items.data[0]
        start, end = item.current_period_start, item.current_period_end
        return datetime.fromtimestamp(start, tz=UTC), datetime.fromtimestamp(end, tz=UTC)

    def advance_to(self, instant: datetime) -> None:
        """Raises: ValueError se instant nao avanca; AssertionError se o clock falha ou expira."""
        if instant <= self._stripe_time:
            raise ValueError("reason=test_clock_not_forward")
        clocks = self.stripe.v1.test_helpers.test_clocks
        clocks.advance(self.test_clock_id, params={"frozen_time": int(instant.timestamp())})
        if not _poll_until(self.config.timeout_seconds, self._clock_ready):
            raise AssertionError("stripe_e2e_timeout reason=test_clock_advance")
        self._stripe_time = instant
        self.clock.instant = max(self.clock.instant, instant)

    def set_app_time(self, instant: datetime) -> None:
        self.clock.instant = instant

    def fail_future_payments(self) -> None:
        method = self.stripe.v1.payment_methods.attach(
            "pm_card_chargeCustomerFail", params={"customer": self.customer_id},
        )
        self.stripe.v1.customers.update(
            self.customer_id, params={"invoice_settings": {"default_payment_method": method.id}},
        )

    def cancel_at_period_end(self) -> None:
        subscriptions = self.stripe.v1.subscriptions
        subscriptions.update(self._subscription(), params={"cancel_at_period_end": True})

    def cancel_now(self) -> None:
        self.stripe.v1.subscriptions.cancel(self._subscription())

    def snapshot(self) -> models.EntitlementSnapshot | None:
        return self.projection.get_snapshot(ACCOUNT, models.ReadConsistency.STRONG)

    def await_snapshot(
        self, predicate: Callable[[models.EntitlementSnapshot], bool], reason: str,
    ) -> models.EntitlementSnapshot:
        """Returns: Primeiro snapshot que satisfaz predicate. Raises: AssertionError no prazo."""
        seen: list[models.EntitlementSnapshot | None] = [None]

        def step() -> bool:
            self.pump()
            seen[0] = self.snapshot()
            return seen[0] is not None and predicate(seen[0])

        if _poll_until(self.config.timeout_seconds, step) and seen[0] is not None:
            return seen[0]
        status = seen[0].subscription_status.value if seen[0] else None
        version = seen[0].entitlement_version if seen[0] else None
        raise AssertionError(f"stripe_e2e_timeout reason={reason} status={status} v={version}")

    def await_inbox_event(self, event_type: str) -> None:
        """Raises: AssertionError se o evento do cliente nao for liquidado no prazo."""

        def step() -> bool:
            self.pump()
            return self._inbox_has(event_type)

        if not _poll_until(self.config.timeout_seconds, step):
            raise AssertionError(f"stripe_e2e_timeout reason=inbox_event type={event_type}")

    def pump(self) -> None:
        """Processa o inbox (cli) ou executa a recuperacao paginada (recover)."""
        try:
            if self.config.delivery == "cli":
                self.recovery.drain_inbox(RECOVERY_PAGE)
                return
            request = RecoveryRequest(RECOVERY_LOOKBACK_HOURS, RECOVERY_PAGE)
            for _ in range(RECOVERY_MAX_PAGES):
                if self.recovery.run(request).next_cursor is None:
                    return
        except RetryableBillingError:
            return

    def decide(self, action: models.EntitlementAction) -> models.EntitlementDecision:
        snapshot = self.snapshot()
        if snapshot is None:
            raise AssertionError("reason=snapshot_missing")
        return EntitlementPolicy(BillingMode.STRIPE).evaluate(snapshot, action, self.clock.now())

    def create_run(self, run_id: str) -> models.RunAuthorization:
        request = make_run_request(run_id=run_id, idempotency_key=f"req-{run_id}")
        return self.gate.authorize_create_run(request)

    def run_period(self, run_id: str) -> datetime:
        return lookup_period(self.env, run_id)

    def usage(self, period_start: datetime) -> dict[str, int]:
        item = get_raw(self.env, usage_key(ACCOUNT, period_start))
        return {k: int(v["N"]) for k, v in (item or {}).items() if "N" in v}

    def outbox_events(self, event_type: str) -> list[OutboxEvent]:
        pending = self.env.plane.pending_outbox(1000)
        return [event for event in pending if event.event_type == event_type]

    def _wire(self) -> None:
        client, table, now = self.client, self.table, self.clock.now
        self.catalog = DynamoBillingCatalog(client, table, now)
        self.projection = DynamoEntitlementProjection(client, table, now)
        self.inbox = WebhookInbox(client, table, now)
        host = "https://e2e.invalid"
        pages = (f"{host}/success", f"{host}/cancel", f"{host}/portal")
        urls = StripeGatewayConfig(*pages, frozenset({host}))
        self.gateway = StripeGateway(self.stripe, urls, self.catalog)
        deps = ProjectorDependencies(self.inbox, self.catalog, self.gateway, self.projection, now)
        self.recovery = build_webhook_recovery(BillingStorage(client, table), deps)
        mode = BillingSettings(BillingMode.STRIPE, models.BillingEnforcementMode.ENFORCE, 0)
        resources = BillingGateResources(now, DEPLOYMENT_LIMIT, client, table)
        self.gate = build_entitlement_gate(mode, resources)
        self.audit = DynamoBillingAudit(client, table)
        self.env = RevEnv(
            client=client,
            spy=SpyClient(client),
            clock=self.clock,
            table=table,
            store=DynamoRevocationStore(client, table, now),
            plane=DynamoDBControlPlane(client, table, now),
            quota=DynamoQuotaReservations(client, table, now),
        )
        self.plan = make_plan(
            PLAN_VERSION_ID,
            stripe_price_ids=(self.config.price_id,),
            quotas=make_limits(),
            grace_period_days=GRACE_DAYS,
            effective_from=now(),
        )

    def _seed(self) -> None:
        customer = self.stripe.v1.customers.create(params={
            "test_clock": self.test_clock_id,
            "payment_method": "pm_card_visa",
            "invoice_settings": {"default_payment_method": "pm_card_visa"},
            "metadata": {"billing_account_id": ACCOUNT},
        })
        self.customer_id = customer.id
        account = make_account(ACCOUNT, stripe_customer_id=customer.id)
        for item in (
            items.encode_plan(self.plan),
            items.encode_price_map(self.config.price_id, PLAN_VERSION_ID),
            items.encode_account(account),
            items.encode_customer_map(ACCOUNT, customer.id),
        ):
            self.client.put_item(TableName=self.table, Item=item)

    def _subscription(self) -> str:
        if self.subscription_id is None:
            raise AssertionError("reason=subscription_missing")
        return self.subscription_id

    def _clock_ready(self) -> bool:
        clock = self.stripe.v1.test_helpers.test_clocks.retrieve(self.test_clock_id)
        if clock.status == "internal_failure":
            raise AssertionError("stripe_test_clock_failed")
        return clock.status == "ready"

    def _inbox_has(self, event_type: str) -> bool:
        pages = self.client.get_paginator("scan").paginate(
            TableName=self.table,
            ConsistentRead=True,
            FilterExpression="#entity = :entity",
            ExpressionAttributeNames={"#entity": "entity"},
            ExpressionAttributeValues={":entity": {"S": INBOX_ENTITY}},
        )
        return any(
            item["event_type"]["S"] == event_type
            and item.get("stripe_customer_id", {}).get("S") == self.customer_id
            and item["state"]["S"] in INBOX_DONE_STATES
            for page in pages
            for item in page["Items"]
        )


@pytest.fixture(scope="session", autouse=True)
def stripe_e2e_config() -> StripeE2EConfig:
    if os.environ.get("RUN_STRIPE_E2E") != "1":
        pytest.skip("reason=run_stripe_e2e_disabled")
    try:
        return load_config(os.environ)
    except StripeE2EConfigError as error:
        pytest.fail(str(error), pytrace=False)


@pytest.fixture(scope="session")
def stripe_client(stripe_e2e_config: StripeE2EConfig) -> Any:
    import stripe

    return stripe.StripeClient(stripe_e2e_config.secret_key, max_network_retries=2)


@pytest.fixture(scope="session")
def dynamodb(stripe_e2e_config: StripeE2EConfig) -> Any:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError

    client = boto3.client(
        "dynamodb",
        endpoint_url=stripe_e2e_config.dynamodb_endpoint,
        region_name="us-east-1",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(retries={"max_attempts": 2}, connect_timeout=2, read_timeout=10),
    )
    try:
        client.list_tables(Limit=1)
    except (BotoCoreError, ClientError, OSError):
        pytest.fail("dynamodb_local_unreachable", pytrace=False)
    return client


@pytest.fixture(scope="session")
def webhook_target() -> InboxTarget:
    return InboxTarget()


def _build_webhook_app(webhook_secret: str, target: InboxTarget) -> Any:
    from fastapi import FastAPI, HTTPException

    from central_api.routes import stripe_webhook as routes
    from cnes_infra.billing.webhook_verifier import StripeWebhookVerifier

    verifier = StripeWebhookVerifier(webhook_secret)

    def target_inbox() -> Any:
        if target.inbox is None:
            raise HTTPException(status_code=503, detail="billing_not_configured")
        return target.inbox

    app = FastAPI()
    app.include_router(routes.router)
    app.dependency_overrides[routes.get_stripe_webhook_verifier] = lambda: verifier
    app.dependency_overrides[routes.get_webhook_inbox] = target_inbox
    return app


@contextmanager
def _webhook_server(app: Any) -> Iterator[int]:
    import uvicorn

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        if not _poll_until(STARTUP_SECONDS, lambda: server.started):
            pytest.fail("webhook_server_not_started", pytrace=False)
        yield port
    finally:
        server.should_exit = True
        thread.join(timeout=STOP_SECONDS)


def _drain_stderr(process: subprocess.Popen[str], ready: threading.Event) -> None:
    for line in process.stderr or ():
        if "Ready!" in line:
            ready.set()


@contextmanager
def _stripe_listener(executable: str, secret_key: str, port: int) -> Iterator[None]:
    events = ",".join(sorted(STRIPE_WEBHOOK_EVENT_TYPES))
    target = f"http://127.0.0.1:{port}{WEBHOOK_PATH}"
    process = subprocess.Popen(  # noqa: S603
        [executable, "listen", "--forward-to", target, "--events", events],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "STRIPE_API_KEY": secret_key},
    )
    ready = threading.Event()
    threading.Thread(target=_drain_stderr, args=(process, ready), daemon=True).start()
    try:
        _poll_until(STARTUP_SECONDS, lambda: ready.is_set() or process.poll() is not None)
        if not ready.is_set() or process.poll() is not None:
            pytest.fail("stripe_cli_not_ready", pytrace=False)
        yield
    finally:
        process.terminate()
        try:
            process.wait(timeout=STOP_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()


@pytest.fixture(scope="session")
def webhook_forwarder(
    stripe_e2e_config: StripeE2EConfig, webhook_target: InboxTarget,
) -> Iterator[None]:
    if stripe_e2e_config.delivery != "cli":
        yield
        return
    executable = shutil.which("stripe")
    if executable is None:
        pytest.fail("stripe_cli_missing", pytrace=False)
    secret_key = stripe_e2e_config.secret_key
    app = _build_webhook_app(stripe_e2e_config.webhook_secret or "", webhook_target)
    with _webhook_server(app) as port, _stripe_listener(executable, secret_key, port):
        yield


@pytest.fixture(scope="session")
def e2e_infra(
    stripe_e2e_config: StripeE2EConfig,
    stripe_client: Any,
    dynamodb: Any,
    request: pytest.FixtureRequest,
) -> E2EInfra:
    request.getfixturevalue("webhook_forwarder")
    target = request.getfixturevalue("webhook_target")
    return E2EInfra(stripe_e2e_config, stripe_client, dynamodb, target)


@pytest.fixture
def stripe_test_clock(stripe_client: Any) -> Iterator[Any]:
    clocks = stripe_client.v1.test_helpers.test_clocks
    clock = clocks.create(params={"frozen_time": int(time.time()), "name": CLOCK_NAME})
    try:
        yield clock
    finally:
        clocks.delete(clock.id)


@pytest.fixture
def clock_runtime(e2e_infra: E2EInfra, stripe_test_clock: Any) -> Iterator[ClockRuntime]:
    table = f"bil024-{uuid4().hex[:12]}"
    create_named_table(e2e_infra.dynamodb, table)
    try:
        runtime = ClockRuntime(e2e_infra, table, stripe_test_clock)
        e2e_infra.target.inbox = runtime.inbox
        yield runtime
    finally:
        e2e_infra.target.inbox = None
        e2e_infra.dynamodb.delete_table(TableName=table)
