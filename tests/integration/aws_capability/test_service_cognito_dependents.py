"""Colecoes e folhas Cognito refazem ownership sem antecipar a proxima lista."""

import json
from copy import deepcopy

import pytest

from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_read_testing import CANARY, ReadLab
from tests.integration.aws_capability.contracts import Page, Resource, ServiceFailure
from tests.integration.aws_capability.test_service_cognito_children_lookup import (
    CURSORS,
    DATA,
    KINDS,
    assert_failure,
    enqueue,
    read_lab,
    remote_error,
    resource,
    trace,
)
from tests.integration.aws_capability.test_service_cognito_client_lookup import assert_owner

__all__ = ["read_lab"]


def test_domains_rele_pool_e_devolve_dois_filhos_sem_consumir_clients(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "domains")
    page = read_lab.services.dependents(resource("pool"), None, read_lab.context)
    assert page == Page(
        (resource("domain"), resource("custom")), CURSORS["cursors"]["clients_start"]
    )
    assert_process(read_lab, trace("root", "tags", "domains"))


def test_domains_vazios_ainda_avancam_sem_antecipar_clientes(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "root")
    page = read_lab.services.dependents(resource("pool"), None, read_lab.context)
    assert page == Page((), CURSORS["cursors"]["clients_start"])
    assert_process(read_lab, trace("root", "tags", "domains"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("Id", "us-east-2_other"),
        ("Name", "foreign"),
        ("Arn", "arn:aws:cognito-idp:us-east-2:222222222222:userpool/us-east-2_X9opaqueQ"),
    ],
)
def test_describe_da_colecao_domains_valida_novamente_identidade(
    read_lab: ReadLab, field: str, value: str
) -> None:
    enqueue(read_lab, "root", "tags")
    reply = deepcopy(DATA["replies"]["domains"])
    reply["UserPool"][field] = value
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("pool"), None, read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags", "domains"))


@pytest.mark.parametrize("value", ["cap-login", "", None, 3, []])
def test_domains_duplicados_ou_valor_presente_invalido_nao_viram_pagina_vazia(
    read_lab: ReadLab, value: object
) -> None:
    enqueue(read_lab, "root", "tags")
    reply = deepcopy(DATA["replies"]["domains"])
    reply["UserPool"]["CustomDomain"] = value
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("pool"), None, read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags", "domains"))


def test_client_consulta_branding_com_pai_comprovado_sem_clientid_inventado(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "root", "tags", "client", "branding")
    page = read_lab.services.dependents(resource("client"), None, read_lab.context)
    assert page == Page((resource("branding"),))
    assert_process(read_lab, trace("root", "tags", "client", "branding"))


def test_client_sem_branding_exige_ausencia_restrita_apos_pais_comprovados(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "root", "tags", "client")
    remote_error(read_lab, "ResourceNotFoundException")
    assert read_lab.services.dependents(resource("client"), None, read_lab.context) == Page()
    assert_process(read_lab, trace("root", "tags", "client", "branding"))


@pytest.mark.parametrize("reply", [{}, {"ManagedLoginBranding": {}}, {"ManagedLoginBranding": []}])
def test_branding_vazio_incompleto_nao_faz_client_virar_folha(
    read_lab: ReadLab, reply: object
) -> None:
    enqueue(read_lab, "root", "tags", "client")
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("client"), None, read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags", "client", "branding"))


@pytest.mark.parametrize("kind", KINDS)
def test_folha_refaz_inspecao_completa_antes_da_pagina_vazia(read_lab: ReadLab, kind: str) -> None:
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script)
    assert read_lab.services.dependents(resource(kind), None, read_lab.context) == Page()
    assert_process(read_lab, trace(*script))


@pytest.mark.parametrize("kind", [*KINDS, "client"])
def test_folha_ou_client_rejeita_cursor_antes_de_io(read_lab: ReadLab, kind: str) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource(kind), CURSORS["cursors"]["clients_start"], read_lab.context
        )
    assert error.value.args == ("service=cursor_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


@pytest.mark.parametrize("kind", [*KINDS, "client", "pool"])
def test_dependents_nao_reaproveita_tags_entre_chamadas(read_lab: ReadLab, kind: str) -> None:
    script = DATA["lookup_scripts"].get(kind, ["root", "tags", "client", "branding"])
    if kind == "pool":
        script = ["root", "tags", "domains"]
    enqueue(read_lab, *script, "root")
    read_lab.process.outcomes.append((0, '{"Tags":{}}', ""))
    read_lab.services.dependents(resource(kind), None, read_lab.context)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource(kind), None, read_lab.context)
    assert error.value.args == ("service=not_owned",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script, "root", "tags"))


@pytest.mark.parametrize(
    ("collection", "cursor", "wrapper"),
    [
        ("clients", "clients_start", "UserPoolClients"),
        ("servers", "empty_servers", "ResourceServers"),
    ],
)
def test_linha_com_pool_divergente_nao_vira_filho(
    read_lab: ReadLab, collection: str, cursor: str, wrapper: str
) -> None:
    enqueue(read_lab, "root", "tags")
    reply = deepcopy(DATA["replies"][collection])
    reply[wrapper][0]["UserPoolId"] = "us-east-2_other"
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("pool"), CURSORS["cursors"][cursor], read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags", collection))


@pytest.mark.parametrize(
    "collection_cursor",
    [("clients", "clients_start"), ("servers", "empty_servers"), ("users", "empty_users")],
)
@pytest.mark.parametrize("reply", [{}, {"Users": None}, []])
def test_shape_de_lista_invalida_nao_e_colecao_vazia(
    read_lab: ReadLab, collection_cursor: tuple[str, str], reply: object
) -> None:
    collection, cursor = collection_cursor
    enqueue(read_lab, "root", "tags")
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("pool"), CURSORS["cursors"][cursor], read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags", collection))


def test_lista_users_nao_exige_pool_na_resposta_e_inspecao_refaz_admin_get_user(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "root", "tags", "users", "root", "tags", "user")
    page = read_lab.services.dependents(
        resource("pool"), CURSORS["cursors"]["empty_users"], read_lab.context
    )
    assert page == Page((resource("user"),))
    assert read_lab.services.inspect(page.resources[0], read_lab.context).account == "111111111111"
    assert_process(read_lab, trace("root", "tags", "users", "root", "tags", "user"))


@pytest.mark.parametrize("kind", KINDS)
def test_material_privado_em_resposta_valida_nao_vaza(read_lab: ReadLab, kind: str) -> None:
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script[:-1])
    reply = deepcopy(DATA["replies"][kind])
    if kind == "domain":
        reply["DomainDescription"]["CloudFrontDistribution"] = CANARY
    elif kind == "server":
        reply["ResourceServer"]["Name"] = CANARY
    elif kind == "user":
        reply["UserAttributes"] = [{"Name": "email", "Value": CANARY}]
    else:
        reply["ManagedLoginBranding"]["Settings"] = {"private": CANARY}
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    read_lab.services.inspect(resource(kind), read_lab.context)
    assert_process(read_lab, trace(*script))


@pytest.mark.parametrize(
    ("collection", "cursor"),
    [
        ("domains", None),
        ("clients", "clients_start"),
        ("servers", "empty_servers"),
        ("users", "empty_users"),
    ],
)
def test_cada_colecao_exige_tags_frescas_antes_da_lista(
    read_lab: ReadLab, collection: str, cursor: str | None
) -> None:
    enqueue(read_lab, "root")
    read_lab.process.outcomes.append((0, '{"Tags":{}}', ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("pool"),
            None if cursor is None else CURSORS["cursors"][cursor],
            read_lab.context,
        )
    assert error.value.args == ("service=not_owned",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags"))


@pytest.mark.parametrize("kind", ["domain", "server", "branding"])
def test_identidade_privada_valida_em_request_e_readback_nao_e_publicada(
    read_lab: ReadLab, kind: str
) -> None:
    case = DATA["private_identities"][kind]
    parents = DATA["lookup_scripts"][kind][:-1]
    enqueue(read_lab, *parents)
    read_lab.process.outcomes.append((0, json.dumps(case["readback"]), ""))
    private = Resource(**case["resource"])
    assert_owner(read_lab.services.inspect(private, read_lab.context))
    assert_process(read_lab, [*trace(*parents), tuple(case["call"])])
