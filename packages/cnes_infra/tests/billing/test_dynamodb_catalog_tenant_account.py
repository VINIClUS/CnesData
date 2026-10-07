"""Testes do leitor tenant para conta de billing do catálogo DynamoDB."""

from typing import Any

import pytest

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.models import BillingAccountTenantLink, ReadConsistency
from cnes_domain.billing.ports import BillingCatalogPort
from cnes_infra.billing.dynamodb_catalog_tenants import DynamoTenantAccountMixin
from cnes_infra.billing.dynamodb_items import encode_tenant_account
from cnes_infra.billing.keys import account_tenant_key
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import (
    TABLE_NAME,
    make_create_command,
    make_link,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import catalog_env, put


class _GetItemSpy:
    def __init__(self, client: Any) -> None:
        self._client = client
        self.reads: list[dict[str, Any]] = []

    def get_item(self, **kwargs: Any) -> Any:
        self.reads.append(kwargs)
        return self._client.get_item(**kwargs)


class _FixedLinkReader(DynamoTenantAccountMixin):
    def __init__(self, client: Any, link: BillingAccountTenantLink | None) -> None:
        self._client = client
        self._table = TABLE_NAME
        self.get_tenant_link = lambda *_: link  # type: ignore[method-assign, assignment]


@pytest.fixture
def env() -> Any:
    with catalog_env() as value:
        yield value


def test_catalogo_expoe_leitor_de_conta_do_tenant(env: Any) -> None:
    _, _, catalog = env

    assert isinstance(catalog, BillingCatalogPort)
    assert hasattr(catalog, "get_tenant_account")


def test_le_conta_do_tenant_vinculado_com_leitura_forte(env: Any) -> None:
    _, _, catalog = env
    command = make_create_command()
    catalog.create_account(command)

    link = catalog.get_tenant_account("tenant-a", ReadConsistency.STRONG)

    assert link == command.initial_tenant_link


def test_leitura_usa_consistencia_conforme_o_modo(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())
    spy = _GetItemSpy(client)
    reader = _FixedLinkReader(spy, make_link())

    reader.get_tenant_account("tenant-a", ReadConsistency.STRONG)
    reader.get_tenant_account("tenant-a", ReadConsistency.EVENTUAL)

    assert [read["ConsistentRead"] for read in spy.reads] == [True, False]


def test_tenant_sem_link_reverso_devolve_none(env: Any) -> None:
    _, _, catalog = env

    assert catalog.get_tenant_account("tenant-a", ReadConsistency.STRONG) is None


def test_reverso_sem_link_direto_devolve_none(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())
    client.delete_item(
        TableName=TABLE_NAME, Key=item_key(*account_tenant_key("ba_01", "tenant-a"))
    )

    assert catalog.get_tenant_account("tenant-a", ReadConsistency.STRONG) is None


def test_link_direto_divergente_devolve_none(env: Any) -> None:
    client, _, _ = env
    reader = _FixedLinkReader(client, make_link("ba_other", "tenant-a"))
    put(client, encode_tenant_account(make_link("ba_01", "tenant-a")))

    assert reader.get_tenant_account("tenant-a", ReadConsistency.STRONG) is None


def test_link_direto_de_outro_tenant_devolve_none(env: Any) -> None:
    client, _, _ = env
    reader = _FixedLinkReader(client, make_link("ba_01", "tenant-b"))
    put(client, encode_tenant_account(make_link("ba_01", "tenant-a")))

    assert reader.get_tenant_account("tenant-a", ReadConsistency.STRONG) is None


def test_item_reverso_corrompido_propaga_erro_permanente(env: Any) -> None:
    client, _, catalog = env
    corrupt = encode_tenant_account(make_link())
    corrupt["entity"] = {"S": "OUTRA"}
    put(client, corrupt)

    with pytest.raises(PermanentBillingError):
        catalog.get_tenant_account("tenant-a", ReadConsistency.STRONG)


def test_falha_de_storage_propaga_erro_de_dependencia() -> None:
    class _Broken:
        def get_item(self, **_: Any) -> Any:
            from botocore.exceptions import ClientError

            raise ClientError({"Error": {"Code": "Throttled", "Message": "x"}}, "GetItem")

    reader = _FixedLinkReader(_Broken(), None)

    with pytest.raises(BillingDependencyError):
        reader.get_tenant_account("tenant-a", ReadConsistency.STRONG)
