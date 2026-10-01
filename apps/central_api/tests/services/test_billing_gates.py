"""Testes do resolvedor tenant → conta de billing usado pelos gates da API."""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from central_api.services.billing_gates import (
    ApiBillingGates,
    BillingAccountMissing,
    TenantAccountResolver,
)
from cnes_domain.billing.errors import BillingDependencyError
from cnes_domain.billing.models import BillingAccountTenantLink, ReadConsistency
from cnes_domain.profiles import BillingMode

NOW = datetime(2026, 9, 1, tzinfo=UTC)


def _link(account: str = "ba_01", tenant: str = "tenant-a") -> BillingAccountTenantLink:
    return BillingAccountTenantLink(account, tenant, "user-1", "account_created", NOW)


class FakeCatalog:
    def __init__(self, link: BillingAccountTenantLink | None = None) -> None:
        self.link = link
        self.calls: list[tuple[str, ReadConsistency]] = []

    def get_tenant_account(
        self, tenant_id: str, consistency: ReadConsistency,
    ) -> BillingAccountTenantLink | None:
        self.calls.append((tenant_id, consistency))
        return self.link


def test_disabled_usa_conta_local_sem_ler_catalogo() -> None:
    catalog = FakeCatalog(_link())
    resolver = TenantAccountResolver(BillingMode.DISABLED, catalog)

    assert resolver.resolve("tenant-a") == "local-tenant-a"
    assert catalog.calls == []


def test_stripe_le_link_reverso_com_leitura_forte() -> None:
    catalog = FakeCatalog(_link())
    resolver = TenantAccountResolver(BillingMode.STRIPE, catalog)

    assert resolver.resolve("tenant-a") == "ba_01"
    assert catalog.calls == [("tenant-a", ReadConsistency.STRONG)]


def test_stripe_sem_link_falha_fechado() -> None:
    resolver = TenantAccountResolver(BillingMode.STRIPE, FakeCatalog(None))

    with pytest.raises(BillingAccountMissing, match="billing_account_missing"):
        resolver.resolve("tenant-a")


def test_stripe_com_link_de_outro_tenant_falha_fechado() -> None:
    resolver = TenantAccountResolver(BillingMode.STRIPE, FakeCatalog(_link(tenant="tenant-b")))

    with pytest.raises(BillingAccountMissing):
        resolver.resolve("tenant-a")


def test_stripe_sem_catalogo_falha_fechado() -> None:
    resolver = TenantAccountResolver(BillingMode.STRIPE)

    with pytest.raises(BillingAccountMissing):
        resolver.resolve("tenant-a")


def test_erro_de_storage_propaga() -> None:
    catalog = Mock()
    catalog.get_tenant_account.side_effect = BillingDependencyError("billing_storage_unavailable")
    resolver = TenantAccountResolver(BillingMode.STRIPE, catalog)

    with pytest.raises(BillingDependencyError):
        resolver.resolve("tenant-a")


def test_bundle_expoe_modo_gate_capacidade_e_contas() -> None:
    gate, capacity = Mock(), Mock()
    accounts = TenantAccountResolver(BillingMode.DISABLED)

    gates = ApiBillingGates(BillingMode.DISABLED, gate, capacity, accounts)

    assert (gates.mode, gates.gate, gates.capacity, gates.accounts) == (
        BillingMode.DISABLED, gate, capacity, accounts,
    )
    assert gates.enforced is False
    assert ApiBillingGates(BillingMode.STRIPE, gate, capacity, accounts).enforced is True
