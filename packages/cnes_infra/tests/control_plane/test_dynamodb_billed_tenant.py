"""Testes da criação transacional de tenant faturado no plano de controle DynamoDB."""

from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from cnes_domain.billing.commands import ConsumeCapacityCommand
from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingTenantConflict,
    EntitlementDenied,
    IdempotencyConflict,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.models import (
    BillingAccountStatus,
    CapacityKind,
    ReservationStatus,
    SubscriptionStatus,
)
from cnes_domain.control_plane.entities import IdempotencyRecord, Tenant
from cnes_infra.billing.dynamodb_items import (
    decode_idempotency_record,
    decode_tenant_account,
    deterministic_id,
    encode_account,
    encode_tenant_account,
    idempotency_item,
)
from cnes_infra.billing.keys import (
    BILLING_AUDIT_TENANT_ID,
    account_tenant_key,
    billing_account_key,
    entitlement_snapshot_key,
    tenant_account_key,
)
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.billed_tenant import (
    TENANT_SCOPE,
    billed_tenant_digest,
    require_creatable_tenant_id,
    tenant_created_event,
)
from cnes_infra.control_plane.dynamodb_keys import idempotency_key, item_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    make_account,
    make_link,
    make_snapshot,
)
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    HASH_A,
    make_limits,
    make_quota_snapshot,
    seed_snapshot,
)
from packages.cnes_infra.tests.control_plane.billed_tenant_support import (
    ALL_MODES,
    DISABLED,
    ENFORCE,
    NEW,
    OTHER,
    STRIPE_OFF,
    Env,
    assert_nothing_written,
    open_env,
)


@pytest.fixture(params=[ENFORCE, DISABLED, STRIPE_OFF], ids=["enforce", "disabled", "stripe_off"])
def env(request: pytest.FixtureRequest) -> Iterator[Env]:
    with open_env(request.param) as opened:
        yield opened


@pytest.fixture
def enforce_env() -> Iterator[Env]:
    with open_env(ENFORCE) as opened:
        yield opened


@pytest.fixture
def disabled_env() -> Iterator[Env]:
    with open_env(DISABLED) as opened:
        yield opened


@pytest.fixture
def stripe_off_env() -> Iterator[Env]:
    with open_env(STRIPE_OFF) as opened:
        yield opened


def test_cria_tenant_links_e_consome_reserva_em_uma_transacao(enforce_env: Env) -> None:
    reservation_id = enforce_env.reserve()
    before = enforce_env.counter()
    command = enforce_env.command(reservation_id)

    created = enforce_env.plane.create_billed_tenant(command)

    assert created == command.tenant
    assert len(enforce_env.spy.transactions) == 1
    assert enforce_env.plane.get_tenant(NEW) == command.tenant
    forward = enforce_env.stored(account_tenant_key(ACCOUNT, NEW))
    assert forward is not None
    reverse = enforce_env.stored(tenant_account_key(NEW))
    assert decode_tenant_account(reverse, NEW) == ACCOUNT
    assert enforce_env.reservation(reservation_id).status is ReservationStatus.CONSUMED
    assert enforce_env.counter() == before == 1
    identity = (NEW, TENANT_SCOPE, "bt-01")
    record = decode_idempotency_record(enforce_env.stored(idempotency_key(*identity)), identity)
    assert (record.status, record.resource_id) == ("COMPLETED", NEW)
    assert record.request_hash == billed_tenant_digest(command)
    assert record.expires_at == NOW + timedelta(days=1)
    events = {event.event_type: event for event in enforce_env.plane.pending_outbox(100)}
    created_event = events["tenant.created"]
    assert created_event.event_id == deterministic_id("tenant.created", NEW)
    assert created_event.tenant_id == BILLING_AUDIT_TENANT_ID
    assert created_event.aggregate_id == ACCOUNT
    assert events["quota.consumed"].payload["resource_id"] == NEW


def test_criacao_de_tenant_e_link_rollbackam_juntos(enforce_env: Env) -> None:
    reservation_id = enforce_env.reserve()
    command = enforce_env.command(reservation_id)

    def fail(_: list[dict[str, Any]]) -> None:
        raise ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "boom"}},
            "TransactWriteItems",
        )

    enforce_env.spy.before_transaction = fail

    with pytest.raises(BillingDependencyError):
        enforce_env.plane.create_billed_tenant(command)

    assert_nothing_written(enforce_env)
    assert enforce_env.reservation(reservation_id).status is ReservationStatus.RESERVED


@ALL_MODES
def test_replay_com_mesmo_comando_devolve_tenant(settings: BillingSettings) -> None:
    with open_env(settings) as env:
        reservation_id = env.reserve()
        first = env.plane.create_billed_tenant(env.command(reservation_id))
        later = first.model_copy(update={"created_at": NOW + timedelta(hours=2)})
        retry = replace(env.command(reservation_id), tenant=later)
        retry = replace(retry, link=replace(retry.link, linked_at=NOW + timedelta(hours=2)))

        replayed = env.plane.create_billed_tenant(retry)

        assert replayed == first
        assert len(env.spy.transactions) == 1


def test_replay_sem_tenant_gravado_pede_nova_tentativa(disabled_env: Env) -> None:
    command = disabled_env.command("res-x")
    record = IdempotencyRecord(
        tenant_id=NEW,
        scope=TENANT_SCOPE,
        key="bt-01",
        request_hash=billed_tenant_digest(command),
        status="COMPLETED",
        resource_id=NEW,
        created_at=NOW,
        expires_at=NOW + timedelta(days=1),
    )
    disabled_env.client.put_item(TableName=TABLE_NAME, Item=idempotency_item(record))

    with pytest.raises(RetryableBillingError, match="billing_idempotency_incomplete"):
        disabled_env.plane.create_billed_tenant(command)


@ALL_MODES
def test_idempotencia_com_pedido_diferente_conflita(settings: BillingSettings) -> None:
    with open_env(settings) as env:
        reservation_id = env.reserve()
        env.plane.create_billed_tenant(env.command(reservation_id))

        with pytest.raises(IdempotencyConflict, match="key=bt-01"):
            env.plane.create_billed_tenant(
                env.command(reservation_id, municipality_name="Outro Municipio")
            )


def test_idempotencia_expirada_e_sobrescrita(disabled_env: Env) -> None:
    command = disabled_env.command("res-x")
    expired = IdempotencyRecord(
        tenant_id=NEW,
        scope=TENANT_SCOPE,
        key="bt-01",
        request_hash=HASH_A,
        status="COMPLETED",
        resource_id="antigo",
        created_at=NOW - timedelta(days=2),
        expires_at=NOW - timedelta(days=1),
    )
    disabled_env.client.put_item(TableName=TABLE_NAME, Item=idempotency_item(expired))

    disabled_env.plane.create_billed_tenant(command)

    stored = decode_idempotency_record(
        disabled_env.stored(idempotency_key(NEW, TENANT_SCOPE, "bt-01")),
        (NEW, TENANT_SCOPE, "bt-01"),
    )
    assert stored.resource_id == NEW
    assert stored.expires_at == NOW + timedelta(days=1)


def test_tenant_existente_conflita(env: Env) -> None:
    reservation_id = env.reserve()
    env.plane.put_tenant(Tenant(tenant_id=NEW, municipality_name="Antigo", created_at=NOW))

    with pytest.raises(BillingTenantConflict, match=f"tenant_id={NEW}"):
        env.plane.create_billed_tenant(env.command(reservation_id))

    assert env.reservation(reservation_id).status is ReservationStatus.RESERVED
    assert cast("Any", env.plane.get_tenant(NEW)).municipality_name == "Antigo"
    assert env.stored(idempotency_key(NEW, TENANT_SCOPE, "bt-01")) is None


@pytest.mark.parametrize("name", ["enforce_env", "stripe_off_env"])
def test_tenant_ja_vinculado_conflita(name: str, request: pytest.FixtureRequest) -> None:
    env: Env = request.getfixturevalue(name)
    reservation_id = env.reserve()
    link = make_link("ba_other", NEW)
    env.client.put_item(TableName=TABLE_NAME, Item=encode_tenant_account(link))

    with pytest.raises(BillingTenantConflict, match=f"tenant_id={NEW}"):
        env.plane.create_billed_tenant(env.command(reservation_id))

    assert env.plane.get_tenant(NEW) is None
    assert env.reservation(reservation_id).status is ReservationStatus.RESERVED


def test_conta_inativa_nega(stripe_off_env: Env) -> None:
    closed = make_account(ACCOUNT, status=BillingAccountStatus.CLOSED)
    stripe_off_env.client.put_item(TableName=TABLE_NAME, Item=encode_account(closed))

    with pytest.raises(PermanentBillingError, match="billing_account_inactive"):
        stripe_off_env.plane.create_billed_tenant(stripe_off_env.command("res-x"))

    assert_nothing_written(stripe_off_env)


def test_conta_ausente_nega(stripe_off_env: Env) -> None:
    stripe_off_env.client.delete_item(
        TableName=TABLE_NAME, Key=item_key(*billing_account_key(ACCOUNT))
    )

    with pytest.raises(PermanentBillingError, match="billing_account_missing"):
        stripe_off_env.plane.create_billed_tenant(stripe_off_env.command("res-x"))

    assert_nothing_written(stripe_off_env)


def test_snapshot_ausente_nega(enforce_env: Env) -> None:
    reservation_id = enforce_env.reserve()
    enforce_env.client.delete_item(
        TableName=TABLE_NAME, Key=item_key(*entitlement_snapshot_key(ACCOUNT))
    )

    with pytest.raises(EntitlementDenied, match="reason=snapshot_missing"):
        enforce_env.plane.create_billed_tenant(enforce_env.command(reservation_id))

    assert_nothing_written(enforce_env)


def test_snapshot_vencido_nega(enforce_env: Env) -> None:
    reservation_id = enforce_env.reserve()
    past_due = make_snapshot(ACCOUNT, subscription_status=SubscriptionStatus.PAST_DUE)
    seed_snapshot(enforce_env.client, past_due)

    with pytest.raises(EntitlementDenied, match="reason=grace_expired"):
        enforce_env.plane.create_billed_tenant(enforce_env.command(reservation_id))

    assert_nothing_written(enforce_env)


@pytest.mark.parametrize(("changes", "reason"), [
    ({"subscription_status": SubscriptionStatus.ADMIN_REVOKED}, "admin_revoked"),
    ({"subscription_status": SubscriptionStatus.CANCELED}, "status_canceled"),
    ({"quotas": make_limits(max_tenants=0)}, "quota_not_granted"),
])
def test_snapshot_que_perde_direito_apos_a_reserva_nega(
    enforce_env: Env, changes: dict[str, Any], reason: str,
) -> None:
    reservation_id = enforce_env.reserve()
    current = make_quota_snapshot()
    seed_snapshot(enforce_env.client, replace(current, entitlement_version=9, **changes))

    with pytest.raises(EntitlementDenied, match=f"reason={reason}"):
        enforce_env.plane.create_billed_tenant(enforce_env.command(reservation_id))

    assert_nothing_written(enforce_env)


def test_reserva_ausente_nega(enforce_env: Env) -> None:
    with pytest.raises(PermanentBillingError, match="capacity_reservation_missing"):
        enforce_env.plane.create_billed_tenant(enforce_env.command("res-ausente"))

    assert_nothing_written(enforce_env)


def test_reserva_expirada_ou_consumida_nega(enforce_env: Env) -> None:
    expired_id = enforce_env.reserve()
    enforce_env.clock.advance(timedelta(minutes=16))
    with pytest.raises(PermanentBillingError, match="capacity_reservation_invalid"):
        enforce_env.plane.create_billed_tenant(enforce_env.command(expired_id))

    enforce_env.clock.instant = NOW
    consumed_id = enforce_env.reserve(OTHER)
    enforce_env.repo.consume_capacity(ConsumeCapacityCommand(ACCOUNT, consumed_id, NOW))
    with pytest.raises(PermanentBillingError, match="capacity_reservation_invalid"):
        enforce_env.plane.create_billed_tenant(enforce_env.command(consumed_id, OTHER, "bt-02"))

    assert_nothing_written(enforce_env)


def test_reserva_de_outro_recurso_nega(enforce_env: Env) -> None:
    other_resource = enforce_env.reserve(OTHER)
    agent_reservation = enforce_env.reserve(NEW, CapacityKind.AGENT)

    for reservation_id in (other_resource, agent_reservation):
        with pytest.raises(PermanentBillingError, match="capacity_reservation_invalid"):
            enforce_env.plane.create_billed_tenant(enforce_env.command(reservation_id))

    assert_nothing_written(enforce_env)


@ALL_MODES
def test_tenant_reservado_e_rejeitado(settings: BillingSettings) -> None:
    with open_env(settings) as env:
        command = env.command("res-x", BILLING_AUDIT_TENANT_ID)

        with pytest.raises(PermanentBillingError, match="tenant_id_reserved"):
            env.plane.create_billed_tenant(command)

        assert env.spy.transactions == []
        assert env.plane.get_tenant(BILLING_AUDIT_TENANT_ID) is None


def test_disabled_nao_toca_links_nem_reserva(disabled_env: Env) -> None:
    reservation_id = disabled_env.reserve()
    before = disabled_env.counter()

    disabled_env.plane.create_billed_tenant(disabled_env.command(reservation_id))

    assert disabled_env.plane.get_tenant(NEW) is not None
    assert disabled_env.stored(account_tenant_key(ACCOUNT, NEW)) is None
    assert disabled_env.stored(tenant_account_key(NEW)) is None
    assert disabled_env.reservation(reservation_id).status is ReservationStatus.RESERVED
    assert disabled_env.counter() == before
    assert "tenant.created" in disabled_env.outbox_types()
    assert "quota.consumed" not in disabled_env.outbox_types()


def test_stripe_sem_enforce_grava_links_sem_reserva(stripe_off_env: Env) -> None:
    reservation_id = stripe_off_env.reserve()

    stripe_off_env.plane.create_billed_tenant(stripe_off_env.command(reservation_id))

    assert stripe_off_env.plane.get_tenant(NEW) is not None
    assert stripe_off_env.stored(account_tenant_key(ACCOUNT, NEW)) is not None
    assert stripe_off_env.stored(tenant_account_key(NEW)) is not None
    assert stripe_off_env.reservation(reservation_id).status is ReservationStatus.RESERVED
    assert "quota.consumed" not in stripe_off_env.outbox_types()


def test_tenant_reservado_validado_pela_funcao_compartilhada() -> None:
    with pytest.raises(PermanentBillingError, match="tenant_id_reserved"):
        require_creatable_tenant_id("_qualquer")
    require_creatable_tenant_id("354130")


def test_evento_de_tenant_criado_e_deterministico() -> None:
    with open_env(DISABLED) as env:
        command = env.command("res-x")
        event = tenant_created_event(command)

        assert event.event_id == deterministic_id("tenant.created", NEW)
        assert event.event_type == "tenant.created"
        assert event.aggregate_id == ACCOUNT
        assert event.actor_id == command.link.linked_by_user_id
        assert event.reason_code == command.link.reason_code
        assert event.occurred_at == command.tenant.created_at
        assert dict(event.attributes) == {"tenant_id": NEW}
