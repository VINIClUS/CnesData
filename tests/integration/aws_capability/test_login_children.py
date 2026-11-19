"""Filhos do login: leitura de domain ausente, remoção por tipo e ordem da limpeza."""

from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_types_testing import denied
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._login_lab_testing import (
    API,
    cleanup_context,
    create_root,
    make_login_lab,
    server_of,
)
from tests.integration.aws_capability._probe_testing import janitor_ports
from tests.integration.aws_capability._resource_ids import cognito_child
from tests.integration.aws_capability.contracts import Resource, ServiceFailure

DOMAIN = "cnesdata-cap-a1b2c3d4"
MISSING = "ResourceNotFoundException"


def seed_children(lab, root):
    context = lab.context
    lab.services.create_child(root, "cognito.server", context)
    pool = next(iter(lab.aws.pools.values()))
    pool.clients["c1"] = {"UserPoolId": pool.pool_id, "ClientId": "c1", "ClientName": "n"}
    pool.domain = DOMAIN
    pool.brandings["b1"] = "c1"
    pool.users["sub-1"] = {"email": "x@example.invalid", "password": None}
    spec = lab.spec
    return {
        "server": server_of(lab, root),
        "client": cognito_child(root, spec, "cognito.client", {"client_id": "c1"}),
        "domain": cognito_child(root, spec, "cognito.domain", {"domain": DOMAIN}),
        "user": cognito_child(root, spec, "cognito.user", {"username": "sub-1"}),
        "branding": cognito_child(
            root, spec, "cognito.branding", {"client_id": "c1", "branding_id": "b1"}
        ),
    }


def test_servidor_canonico_e_criado_no_perfil_de_login(tmp_path) -> None:
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    child = lab.services.create_child(root, "cognito.server", lab.context)
    assert child == server_of(lab, root)
    pool = lab.aws.pools[root.resource_id]
    assert list(pool.servers) == [API]


def test_domain_ausente_com_descricao_vazia_e_ausencia(tmp_path) -> None:
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    seeded = seed_children(lab, root)
    domain = seeded["domain"]
    assert lab.services.lookup(domain, lab.context) is not None
    lab.aws.pools[root.resource_id].domain = None
    assert lab.services.lookup(domain, lab.context) is None
    with pytest.raises(ServiceFailure) as error:
        lab.services.inspect(domain, lab.context)
    assert error.value.args == ("service=not_found",)


def test_domain_com_erro_remoto_continua_falha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    domain = seed_children(lab, root)["domain"]
    monkeypatch.setattr(
        lab.aws,
        "_cognito_idp_describe_user_pool_domain",
        lambda payload, who: denied(MISSING),
    )
    with pytest.raises(ServiceFailure):
        lab.services.lookup(domain, lab.context)


@pytest.mark.parametrize("kind", ["client", "domain", "user", "branding"])
def test_remove_cada_filho_e_prova_ausencia(tmp_path, kind: str) -> None:
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    child: Resource = seed_children(lab, root)[kind]
    lab.services.delete(child, True, cleanup_context(lab))
    assert lab.services.lookup(child, cleanup_context(lab)) is None


@pytest.mark.parametrize("kind", ["client", "domain", "user", "branding"])
def test_remocao_de_filho_so_na_limpeza(tmp_path, kind: str) -> None:
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    child = seed_children(lab, root)[kind]
    with pytest.raises(ServiceFailure):
        lab.services.delete(child, True, lab.context)


def test_limpeza_remove_filhos_antes_do_pool_na_ordem_fixada(tmp_path) -> None:
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    seed_children(lab, root)
    prepare_directory(lab.spec.journal_dir)
    journal = Journal(lab.spec)
    journal.created = [root]
    journal.phase = "cleanup_requested"
    journal.save()
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    removals = [
        r.operation
        for r in lab.aws.requests
        if r.operation.startswith(("delete-", "admin-delete", "update-user-pool"))
    ]
    assert removals == [
        "delete-user-pool-domain",
        "delete-managed-login-branding",
        "delete-user-pool-client",
        "delete-resource-server",
        "admin-delete-user",
        "update-user-pool",
        "delete-user-pool",
    ]
    assert lab.aws.pools == {}
