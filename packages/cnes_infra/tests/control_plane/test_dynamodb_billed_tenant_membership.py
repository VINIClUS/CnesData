"""Testes da membership do criador gravada com o tenant faturado no DynamoDB."""

from collections.abc import Iterator
from typing import Any

import pytest
from botocore.exceptions import ClientError

from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingTenantConflict,
    IdempotencyConflict,
)
from cnes_domain.billing.models import BILLING_ADMIN_ROLE, ReservationStatus
from cnes_domain.control_plane.entities import Membership
from cnes_infra.auth.dynamodb_memberships import DynamoDBMembershipCandidates
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.billed_tenant import creator_membership
from cnes_infra.control_plane.dynamodb_keys import key_component, membership_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.control_plane.billed_tenant_support import (
    ALL_MODES,
    CREATOR,
    ENFORCE,
    ISSUER,
    NEW,
    Env,
    assert_nothing_written,
    open_env,
)

EXPECTED = Membership(
    tenant_id=NEW, user_id=CREATOR, role="gestor", created_at=NOW, oidc_issuer=ISSUER,
)


@pytest.fixture
def enforce_env() -> Iterator[Env]:
    with open_env(ENFORCE) as opened:
        yield opened


def _membership_put(actions: list[dict[str, Any]]) -> dict[str, Any]:
    (put,) = [
        action["Put"] for action in actions
        if action.get("Put", {}).get("Item", {}).get("entity", {}).get("S") == "MEMBERSHIP"
    ]
    return put


def _boom(_: list[dict[str, Any]]) -> None:
    raise ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "boom"}}, "TransactWriteItems",
    )


def test_papel_do_criador_e_o_papel_administrativo_de_billing() -> None:
    assert BILLING_ADMIN_ROLE == "gestor"


@ALL_MODES
def test_grava_membership_gestor_do_criador_na_mesma_transacao(
    settings: BillingSettings,
) -> None:
    with open_env(settings) as env:
        command = env.command(env.reserve())

        env.plane.create_billed_tenant(command)

        assert env.membership() == EXPECTED
        assert creator_membership(command) == EXPECTED
        (actions,) = env.spy.transactions
        put = _membership_put(actions)
        assert put["ConditionExpression"] == "attribute_not_exists(pk)"
        assert (put["Item"]["pk"]["S"], put["Item"]["sk"]["S"]) == membership_key(NEW, CREATOR)
        assert put["Item"]["gsi1pk"]["S"] == f"USER#{key_component(CREATOR)}"
        assert put["Item"]["gsi1sk"]["S"] == f"TENANT#{key_component(NEW)}"
        candidates = DynamoDBMembershipCandidates(env.client, TABLE_NAME)
        assert candidates.list_candidates(CREATOR) == (NEW,)


@ALL_MODES
def test_falha_apos_a_transacao_e_replay_deixam_uma_membership(
    settings: BillingSettings,
) -> None:
    with open_env(settings) as env:
        command = env.command(env.reserve())
        env.spy.after_transaction = _boom

        with pytest.raises(BillingDependencyError):
            env.plane.create_billed_tenant(command)
        env.spy.after_transaction = None
        replayed = env.plane.create_billed_tenant(command)

        assert replayed == command.tenant
        assert len(env.memberships()) == 1
        assert env.membership() == EXPECTED


@ALL_MODES
def test_membership_orfa_do_criador_conflita_sem_sobrescrever(
    settings: BillingSettings,
) -> None:
    with open_env(settings) as env:
        reservation_id = env.reserve()
        orphan = EXPECTED.model_copy(update={"role": "leitor", "oidc_issuer": None})
        env.plane.put_membership(orphan)

        with pytest.raises(BillingTenantConflict, match=f"tenant_id={NEW}"):
            env.plane.create_billed_tenant(env.command(reservation_id))

        assert env.membership() == orphan
        assert env.plane.get_tenant(NEW) is None
        assert env.reservation(reservation_id).status is ReservationStatus.RESERVED


@ALL_MODES
def test_issuer_diferente_com_a_mesma_chave_conflita(settings: BillingSettings) -> None:
    with open_env(settings) as env:
        reservation_id = env.reserve()
        env.plane.create_billed_tenant(env.command(reservation_id))

        with pytest.raises(IdempotencyConflict, match="key=bt-01"):
            env.plane.create_billed_tenant(
                env.command(reservation_id, creator_issuer="https://outro-issuer")
            )

        assert env.membership() == EXPECTED


def test_rollback_injetado_nao_deixa_membership(enforce_env: Env) -> None:
    reservation_id = enforce_env.reserve()
    enforce_env.spy.before_transaction = _boom

    with pytest.raises(BillingDependencyError):
        enforce_env.plane.create_billed_tenant(enforce_env.command(reservation_id))

    assert_nothing_written(enforce_env)
    assert enforce_env.memberships() == []
