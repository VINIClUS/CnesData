"""Integração do gate de criação de tenant cobrado com catálogo e capacidade reais."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from central_api.auth.aws_oidc import MembershipAuthorizer
from cnes_domain.billing.models import ReadConsistency, ReservationStatus
from cnes_domain.control_plane.entities import Membership
from cnes_infra.auth.dynamodb_memberships import DynamoDBMembershipCandidates
from cnes_infra.auth.oidc import OidcPrincipal
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.keys import account_tenant_key, tenant_account_key, tenant_entity_key
from cnes_infra.control_plane.billed_tenant import TENANT_SCOPE
from cnes_infra.control_plane.dynamodb_keys import idempotency_key, membership_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT
from tests.integration.billing._api_gates_stack import (
    ISSUER,
    MANAGER,
    OWNER,
    ApiStack,
    build_client,
    capacity_counter,
    capacity_reservations,
    create_tenant,
    default_snapshot,
    open_api_stack,
    stored_keys,
    user_headers,
)
from tests.integration.billing._enforcement_stack import DYNAMO_STRIPE


@pytest.fixture
def stack(tmp_path: Path) -> Iterator[ApiStack]:
    with open_api_stack(DYNAMO_STRIPE, tmp_path, default_snapshot(max_tenants=2)) as opened:
        yield opened


@pytest.fixture
def single_slot(tmp_path: Path) -> Iterator[ApiStack]:
    with open_api_stack(DYNAMO_STRIPE, tmp_path, default_snapshot(max_tenants=1)) as opened:
        yield opened


def _tenant_keys(tenant_id: str) -> set[tuple[str, str]]:
    return {
        tenant_entity_key(tenant_id), tenant_account_key(tenant_id),
        account_tenant_key(ACCOUNT, tenant_id), membership_key(tenant_id, OWNER),
    }


def test_cria_tenant_com_links_e_reserva_consumida(stack: ApiStack) -> None:
    assert capacity_counter(stack, "tenant_count") == 0
    client = build_client(stack)

    response = create_tenant(client, "novo-tenant")

    assert response.status_code == 201
    assert response.json()["billing_account_id"] == ACCOUNT
    assert stack.plane.get_tenant("novo-tenant").municipality_name == "Municipio"
    catalog = DynamoBillingCatalog(stack.client, TABLE_NAME, stack.clock.now)
    forward = catalog.get_tenant_link(ACCOUNT, "novo-tenant", ReadConsistency.STRONG)
    reverse = catalog.get_tenant_account("novo-tenant", ReadConsistency.STRONG)
    assert forward is not None
    assert reverse is not None
    assert (forward.billing_account_id, reverse.billing_account_id) == (ACCOUNT, ACCOUNT)
    assert [r.status for r in capacity_reservations(stack)] == [ReservationStatus.CONSUMED]
    assert capacity_counter(stack, "tenant_count") == 1


def test_criacao_de_tenant_e_link_rollbackam_juntos(stack: ApiStack) -> None:
    assert stack.faulty is not None
    stack.faulty.fail_tenant_creation = True
    client = build_client(stack)

    response = create_tenant(client, "novo-tenant")

    assert response.status_code == 503
    assert stack.faulty.failures == 2
    assert stack.plane.get_tenant("novo-tenant") is None
    assert not _tenant_keys("novo-tenant") & stored_keys(stack)
    partition, prefix = idempotency_key("novo-tenant", TENANT_SCOPE, "")
    assert [k for k in stored_keys(stack) if k[0] == partition and k[1].startswith(prefix)] == []
    assert [r.status for r in capacity_reservations(stack)] == [ReservationStatus.RELEASED]
    assert capacity_counter(stack, "tenant_count") == 0


def test_tenant_reservado_billing_422(stack: ApiStack) -> None:
    client = build_client(stack)

    response = create_tenant(client, "_billing")

    assert response.status_code == 422
    assert "tenant_id_reserved" in str(response.json()["detail"])
    assert capacity_reservations(stack) == []
    assert stack.plane.get_tenant("_billing") is None


def test_ultima_vaga_de_tenant_cria_um_so(single_slot: ApiStack) -> None:
    client = build_client(single_slot)

    first = create_tenant(client, "tenant-um", "key-1")
    second = create_tenant(client, "tenant-dois", "key-2")

    assert first.status_code == 201
    assert (second.status_code, second.json()["detail"]) == (403, "tenant_quota_exceeded")
    assert single_slot.plane.get_tenant("tenant-um") is not None
    assert single_slot.plane.get_tenant("tenant-dois") is None
    assert capacity_counter(single_slot, "tenant_count") == 1


def test_link_inicial_da_conta_nao_consome_max_tenants(single_slot: ApiStack) -> None:
    catalog = DynamoBillingCatalog(single_slot.client, TABLE_NAME, single_slot.clock.now)

    initial = catalog.get_tenant_account("354130", ReadConsistency.STRONG)

    assert initial is not None
    assert initial.billing_account_id == ACCOUNT
    assert capacity_counter(single_slot, "tenant_count") == 0
    assert create_tenant(build_client(single_slot), "tenant-um").status_code == 201


def test_replay_da_criacao_devolve_o_mesmo_tenant(stack: ApiStack) -> None:
    client = build_client(stack)

    first = create_tenant(client, "novo-tenant", "key-1")
    replay = create_tenant(client, "novo-tenant", "key-1")

    assert (first.status_code, replay.status_code) == (201, 201)
    assert replay.json() == first.json()
    assert len(capacity_reservations(stack)) == 1
    assert capacity_counter(stack, "tenant_count") == 1
    partition, _ = membership_key("novo-tenant", OWNER)
    memberships = {k for k in stored_keys(stack) if k[0] == partition and "MEMBERSHIP#" in k[1]}
    assert memberships == {membership_key("novo-tenant", OWNER)}


def test_gestor_criador_acessa_o_tenant_novo(stack: ApiStack) -> None:
    stack.plane.put_membership(Membership(
        tenant_id=TENANT, user_id=MANAGER, role="gestor", created_at=NOW, oidc_issuer=ISSUER,
    ))
    client = build_client(stack)

    created = create_tenant(client, "novo-tenant", "key-1", user_headers(MANAGER, TENANT))
    via_new = create_tenant(client, "outro-tenant", "key-2", user_headers(MANAGER, "novo-tenant"))

    assert created.status_code == 201
    assert via_new.status_code == 201, via_new.json()
    membership = stack.plane.get_membership("novo-tenant", MANAGER)
    assert (membership.role, membership.oidc_issuer) == ("gestor", ISSUER)
    candidates = DynamoDBMembershipCandidates(stack.client, TABLE_NAME)
    authorizer = MembershipAuthorizer(stack.plane, candidates)
    principal = OidcPrincipal(ISSUER, MANAGER, None, None)
    granted = {grant.tenant_id for grant in authorizer.list_authorized(principal)}
    assert {TENANT, "novo-tenant"} <= granted


def test_retry_concorrente_commitado_na_sonda_nao_libera_a_vaga(single_slot: ApiStack) -> None:
    assert single_slot.faulty is not None
    client = build_client(single_slot)

    def commit_identical_request() -> None:
        assert create_tenant(client, "novo-tenant", "key-1").status_code == 201

    single_slot.faulty.before_query = commit_identical_request

    retry = create_tenant(client, "novo-tenant", "key-1")
    later = create_tenant(client, "outro-tenant", "key-2")

    assert retry.status_code == 201
    assert [r.status for r in capacity_reservations(single_slot)] == [ReservationStatus.CONSUMED]
    assert capacity_counter(single_slot, "tenant_count") == 1
    assert (later.status_code, later.json()["detail"]) == (403, "tenant_quota_exceeded")
